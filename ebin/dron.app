{application, dron, [{description, "Dron"},
                    {vsn, "0.1"},
                    {modules, [dron_master, dron_worker]},
                    {registered, [dron_master]},
                    {env, []},
                    {mod, {dron, []}},
                    {applications, [kernel, stdlib]}]}.
                    