-module(dron_load).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([load/0]).

%-------------------------------------------------------------------------------

test_path() ->
  "/home/ubuntu/Dron/benchmark/".

load() ->
  create_grep(),
  create_grep_hive(),
  create_grep_table(),
  copy_uv(),
  create_uv(),
  copy_rankings(),
  create_rankings().

create_grep() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CreateGrep",
         cmd_line = "hadoop jar \"$HADOOP_HOME\"/hadoop-examples.jar teragen 500000000 /input/grep",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

create_grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CreateGrepHive",
         cmd_line = "hadoop jar \"$HADOOP_HOME\"/hadoop-examples.jar teragen 500000000 /input/hive/grep",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

create_grep_table() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateGrepTable",
                             cmd_line = "hive -f " ++ test_path() ++ "create_grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CreateGrepHive", {today}}],
                             deps_timeout = 10000}).

copy_uv() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CopyUV",
         cmd_line = "hadoop jar " ++ test_path() ++ "/jars/dataloader.jar uservisits \"$DATA_DIR/uservisits/UserVisits.dat\" /input/uservisits/UserVisits.dat",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

create_uv() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CreateUV",
         cmd_line = "hive -f " ++ test_path() ++ "create_uservisits.hive",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

copy_rankings() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CopyRankings",
         cmd_line = "hadoop jar " ++ test_path() ++ "/jars/dataloader.jar rankings \"$DATA_DIR/rankings/Rankings.dat\" /input/rankings/Rankings.dat",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

create_rankings() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateRankings",
                             cmd_line = "hive -f " ++ test_path() ++ "create_rankings.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).
