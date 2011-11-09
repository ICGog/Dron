-record(job, {name, cmd_line, start_time, frequency, timeout, max_retries}).

-record(job_instance, {jid, name, cmd_line, state, timeout, run_time, num_retry,
                       worker}).

-record(archive_job, {name, version, cmd_line, start_time, frequency, timeout,
                     max_retries}).

-record(worker, {name, enabled, max_slots, used_slots}).

-record(job_time, {name, expected_run_time, run_time}).

-define(NAME, {global, ?MODULE}).
