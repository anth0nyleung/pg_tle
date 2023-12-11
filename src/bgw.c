#include "postgres.h"

/* These are always necessary for a bgworker */
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "pgstat.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "funcapi.h"
#include "tleextension.h"
#include "utils/builtins.h"
#include "common/hashfn.h"

PGDLLEXPORT void tle_bgw_main(Datum main_arg);

/* GUC variables */
static int tle_bgw_naptime = 5;
static int tle_bgw_workers = 3;

static const char *bgw_shmem_name = "pgtle_bgw";

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void bgw_shmem_startup(void);
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#if (PG_VERSION_NUM >= 150000)
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void bgw_shmem_request(void);
#endif

static void bgw_sigterm(SIGNAL_ARGS);
static void bgw_sighup(SIGNAL_ARGS);

static Size ws_entry_memsize(void);

static uint32 hash_key(const void *key, Size keysize);
static int cmp_key(const void *key1, const void *key2, Size keysize);

void bgw_init(void);

typedef struct bgwHashEntry
{
    char name[NAMEDATALEN];
    Oid funcoid;
    char db[NAMEDATALEN];
} bgwHashEntry;

typedef struct worker_state
{
    LWLock *lock;
    LWLock *htab_lock;
    bool is_worker_registered[3];
    char identifiers[3][NAMEDATALEN];
    int nworkers;
} worker_state;

static worker_state * ws = NULL;
static HTAB *bgw_hash = NULL;

static void
bgw_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
#if PG_VERSION_NUM >= 100000
    SetLatch(MyLatch);
#else
    if (MyProc)
        SetLatch(&MyProc->procLatch);
#endif
    errno = save_errno;
}

static void
bgw_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
#if PG_VERSION_NUM >= 100000
    SetLatch(MyLatch);
#else
    if (MyProc)
        SetLatch(&MyProc->procLatch);
#endif
    errno = save_errno;
}


PG_FUNCTION_INFO_V1(register_background_worker);
Datum
register_background_worker(PG_FUNCTION_ARGS)
{
    char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Oid funcoid = PG_GETARG_OID(1);
    char *db = text_to_cstring(PG_GETARG_TEXT_PP(2));
    bool found;
    bgwHashEntry *entry;
    char *key;

    key = pstrdup(name);

    LWLockAcquire(ws->lock, LW_EXCLUSIVE);

    if (ws->nworkers > tle_bgw_workers)
    {
        LWLockRelease(ws->lock);
        ereport(ERROR, 
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("All %d tle background worker are registered", tle_bgw_workers)));
    }

    LWLockAcquire(ws->htab_lock, LW_EXCLUSIVE);
    entry = (bgwHashEntry *) hash_search(bgw_hash, key, HASH_ENTER, &found);
    if (!found)
    {
        /* New entry, initialize it */
        entry->funcoid = funcoid;
        strlcpy(entry->name, name, sizeof(entry->name));
        strlcpy(entry->db, db, sizeof(entry->db));
    }
    LWLockRelease(ws->htab_lock);
    
    strlcpy(ws->identifiers[ws->nworkers], key, sizeof(ws->identifiers[ws->nworkers]));    
    ws->is_worker_registered[ws->nworkers++] = true;
    LWLockRelease(ws->lock);

    PG_RETURN_TEXT_P(cstring_to_text(""));
}

static int
cmp_key(const void *key1, const void *key2, Size keysize)
{
    const char *k1;
    const char *k2;
    if (key1 == NULL || key2 == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Attempting to hash compare null value(s)"),
            errhidestmt(true)));

    k1 = (const char *) key1;
    k2 = (const char *) key2;

    if (key1 == NULL || key2 == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Cannot compare null value(s)"),
            errhidestmt(true)));
    
    return strcmp(k1, k2);
}

static uint32
hash_key(const void *key, Size keysize)
{
    const char *k;
    uint32 val;

    if (key == NULL)
        (errcode(ERRCODE_INTERNAL_ERROR),
            (errmsg("Attempting to hash null key"),
            errhidestmt(true)));
    
    k = (const char *) key;
    if (k == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("hash key is null"),
            errhidestmt(true)));

    val = hash_any((const unsigned char *) k, strlen(k));
    return val;
}

PG_FUNCTION_INFO_V1(describe_bgw_shmem);
Datum
describe_bgw_shmem(PG_FUNCTION_ARGS)
{
    int i;
    bool registered;
    char *key;
    bgwHashEntry  *entry;

    LWLockAcquire(ws->lock, LW_SHARED);

    for (i = 0; i < tle_bgw_workers; i++)
    {
        registered = ws->is_worker_registered[i]; 
        elog(NOTICE, "slot at %d %s registered", i, registered ? "is" : "is not");
        if (!registered)
            continue;

        key = ws->identifiers[i];
        LWLockAcquire(ws->htab_lock, LW_EXCLUSIVE);
        entry = (bgwHashEntry *) hash_search(bgw_hash, key, HASH_FIND, NULL);

        if (entry)
            elog(NOTICE, "slot %d - name: %s, fnoid: %d, db: %s", i, entry->name, entry->funcoid, entry->db);            
        else
            elog(NOTICE, "slot %d - failed to describe...", i);
        LWLockRelease(ws->htab_lock);
    }
    LWLockRelease(ws->lock);
    PG_RETURN_VOID();
}

void
bgw_init(void)
{
    BackgroundWorker worker;

#if (PG_VERSION_NUM < 150000)
    RequestNamedLWLockTranche(bgw_shmem_name, 1);
    RequestAddinShmemSpace(ws_entry_memsize());
#endif

    /* PG15 requires shared memory space to be requested in shmem_request_hook */
#if (PG_VERSION_NUM >= 150000)
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = bgw_shmem_request;
#endif

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = bgw_shmem_startup;

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    worker.bgw_notify_pid = 0;
    sprintf(worker.bgw_library_name, PG_TLE_EXTNAME);
    sprintf(worker.bgw_function_name, "tle_bgw_main");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_tle background worker");

    for (int i = 0; i < tle_bgw_workers; i++)
    {
        snprintf(worker.bgw_name, BGW_MAXLEN, "pg_tle background worker %d", i);
        worker.bgw_main_arg = Int32GetDatum(i);
        RegisterBackgroundWorker(&worker);
    }
}

void
tle_bgw_main(Datum main_arg)
{
    bgwHashEntry  *entry;
    char *key;
    int index = DatumGetInt32(main_arg);
    
    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, bgw_sighup);
    pqsignal(SIGTERM, bgw_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();
    
    /*
     * Main loop: do this until SIGTERM is received and processed by
     * ProcessInterrupts.
     */

    while (!got_sigterm)
	{	
        /*
         * Background workers mustn't call usleep() or any direct equivalent:
         * instead, they may wait on their process latch, which sleeps as
         * necessary, but is awakened if postmaster dies.  That way the
         * background process goes away immediately in an emergency.
         */
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         tle_bgw_naptime * 1000L,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();
        
        /*
         * In case of a SIGHUP, just reload the configuration.
         */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            elog(LOG, "worker_spi_main %d: Finished sighup", index);
        }

        if (got_sigterm)
        {
            elog(LOG, "worker_spi_main %d: Got sigterm", index);
            proc_exit(0);
        }

        LWLockAcquire(ws->lock, LW_EXCLUSIVE);
        if (ws->is_worker_registered[index])
        {
            elog(LOG, "worker_spi_main %d runs", index);
            key = pstrdup(ws->identifiers[index]);
            LWLockAcquire(ws->htab_lock, LW_EXCLUSIVE);
            entry = (bgwHashEntry *) hash_search(bgw_hash, key, HASH_FIND, NULL);

            if (entry)
                elog(LOG, "slot %d - name: %s, fnoid: %d, db: %s", index, entry->name, entry->funcoid, entry->db);            
            else
                elog(LOG, "slot %d - failed to describe...", index);

            LWLockRelease(ws->htab_lock);
        }
        LWLockRelease(ws->lock);
    }
    proc_exit(0);
}

static void
bgw_shmem_startup(void)
{
    bool found;
    HASHCTL info;
    int i;
    
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ws = ShmemInitStruct(bgw_shmem_name, ws_entry_memsize(), &found);

	if (!found)
    {
        /* Initialize clientauth_ss */
        ws->lock = &(GetNamedLWLockTranche(bgw_shmem_name)[0]).lock;
        ws->htab_lock = &(GetNamedLWLockTranche(bgw_shmem_name)[1]).lock;
        ws->nworkers = 0;
        
        for (i = 0; i < tle_bgw_workers; i++)
        {
            ws->is_worker_registered[i] = false;
        }
    }

    memset(&info, 0, sizeof(info));
    info.keysize = NAMEDATALEN;
    info.entrysize = sizeof(bgwHashEntry);
    info.hash = hash_key;
    info.match = cmp_key;
    bgw_hash = ShmemInitHash("tle bgw hash",
                            tle_bgw_workers, tle_bgw_workers,
                            &info,
                            HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

    LWLockRelease(AddinShmemInitLock);
}

#if (PG_VERSION_NUM >= 150000)
static void
bgw_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestNamedLWLockTranche(bgw_shmem_name, 2);
    RequestAddinShmemSpace(ws_entry_memsize());
}
#endif

static Size
ws_entry_memsize(void)
{
    Size size;
    size = MAXALIGN(sizeof(worker_state));
    size = add_size(size, hash_estimate_size(tle_bgw_workers, sizeof(bgwHashEntry)));
    return size;
}
