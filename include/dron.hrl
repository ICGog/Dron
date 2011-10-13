-record(job, {name, cmd_line, start_time, frequency, timeout}).

-record(job_instance, {jid, name, cmd_line, timeout, run_time}).

-record(archive_job, {name, version, cmd_line, start_time, frequency, timeout}).

-record(id, {counter, id}).

-define(NAME, {global, ?MODULE}).
