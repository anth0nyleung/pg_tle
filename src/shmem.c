#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(get_tle_shmem_val);
PG_FUNCTION_INFO_V1(put_tle_shmem_val);
PG_FUNCTION_INFO_V1(update_tle_shmem_val);

void shmem_init(void);

/* Saved hook values in case of unload */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void tle_shmem_request(void);
static void tle_shmem_startup(void);

typedef struct TleShmemData
{
    LWLock	   	*lock;
    int data;
} TleShmemData; 

/* Links to shared memory state */
static TleShmemData *tleShmemData = NULL;

void shmem_init(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = tle_shmem_request;
#else
	tle_shmem_request();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = tle_shmem_startup;
}

static Size
pgtle_shmemsize(void)
{
	Size		size;
	size = MAXALIGN(sizeof(tleShmemData));
	return size;
}

static void
tle_shmem_request(void)
{
#if PG_VERSION_NUM >= 150000 
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(pgtle_shmemsize());
    RequestNamedLWLockTranche("pg_tle", 1);
}

static void
tle_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
    
    tleShmemData = NULL;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    tleShmemData = ShmemInitStruct("pg_tle", sizeof(tleShmemData), &found);
    if (!found) {
		tleShmemData->lock = &(GetNamedLWLockTranche("pg_tle"))->lock;
        tleShmemData->data = 42;
    }

	LWLockRelease(AddinShmemInitLock);
}

Datum
get_tle_shmem_val(PG_FUNCTION_ARGS)
{
    int32 res;

    LWLockAcquire(tleShmemData->lock, LW_SHARED);
    res = tleShmemData->data;
    LWLockRelease(tleShmemData->lock);

    PG_RETURN_INT32(res);
}

Datum
put_tle_shmem_val(PG_FUNCTION_ARGS)
{
    int32 val;

    val = PG_GETARG_INT32(0);

    LWLockAcquire(tleShmemData->lock, LW_EXCLUSIVE);
    tleShmemData->data = tleShmemData->data + val;
    LWLockRelease(tleShmemData->lock);

    PG_RETURN_VOID();
}

Datum
update_tle_shmem_val(PG_FUNCTION_ARGS)
{
    int32 val;

    val = PG_GETARG_INT32(0);

    LWLockAcquire(tleShmemData->lock, LW_EXCLUSIVE);
    tleShmemData->data = tleShmemData->data + val;
    LWLockRelease(tleShmemData->lock);

    PG_RETURN_VOID();
}