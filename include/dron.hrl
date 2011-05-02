
-define(JOB_REGISTRY_TABLE, job_registry).

-record(dron_job, {id,
                   state,
                   started_on,
                   ended_on,
                   cmd}).
