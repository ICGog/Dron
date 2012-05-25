-module(dron_unload).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([clean/0, delete/0]).

%-------------------------------------------------------------------------------

test_path() ->
  "/home/ubuntu/Dron/benchmark/".

clean() ->
  drop_grep_select(),
  drop_agg_uv(),
  drop_join_uv(),
  drop_ad_uv(),
  drop_rankings_select(),
  delete_grep_output(),
  delete_select_rank_output(),
  delete_agg_uv_output(),
  delete_join_uv_output().

delete() ->
  delete_grep(),
  delete_rankings(),
  delete_uservisits(),
  delete_grep_hive().

%-------------------------------------------------------------------------------

drop_grep_select() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropGrep",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_grep_select.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_rankings_select() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropRankings",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_rankings_select.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_agg_uv() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropAggUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_uservisits_aggre.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_ad_uv() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropAdUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_uservisits_ad.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).
  
drop_join_uv() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropJoinUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_uservisits_join.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

%-------------------------------------------------------------------------------

delete_grep_output() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "EraseGrepMR",
         cmd_line = "hadoop fs -rmr /output/grep",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

delete_select_rank_output() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "EraseSelRankMR",
         cmd_line = "hadoop fs -rmr /output/rankings",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

delete_agg_uv_output() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "EraseAggUVMR",
         cmd_line = "hadoop fs -rmr /output/uservisits_agg",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

delete_join_uv_output() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "EraseJoinUVMR",
         cmd_line = "hadoop fs -rmr /output/uservisits_join",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

delete_grep() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "DeleteGrep",
         cmd_line = "hadoop fs -rmr /input/grep/",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).  

delete_rankings() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "DeleteRankings",
         cmd_line = "hadoop fs -rmr /input/rankings/",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

delete_uservisits() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "DeleteUV",
         cmd_line = "hadoop fs -rmr /input/uservisits/",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).  
  
delete_grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "DeleteHiveGrep",
         cmd_line = "hadoop fs -rmr /input/hive/grep/",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10000}).
