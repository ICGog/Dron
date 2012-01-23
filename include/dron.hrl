-record(job, {name, cmd_line, start_time, frequency, timeout, max_retries,
             dependencies, deps_timeout}).

-record(job_instance, {jid, name, cmd_line, state, timeout, run_time, num_retry,
                       deps_timeout, dependencies, worker}).

-record(archive_job, {name, version, cmd_line, start_time, frequency, timeout,
                     max_retries, dependencies, deps_timeout}).

-record(resource_deps, {rid, state, dep}).

-record(worker, {name, scheduler, enabled, max_slots, used_slots}).

-define(NAME, {global, ?MODULE}).
