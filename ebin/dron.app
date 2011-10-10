{application, dron, [{description, "Job Scheduler"},
                    {vsn, "0.1"},
                    {modules, [dron_scheduler, dron_mnesia, dron_pool,
                    dron_worker]},
                    {env, []},
                    {applications, [kernel, stdlib]}]}.
                    
