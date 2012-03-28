-module(dron_benchmark).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/0, stop/0, register_hive/0, register_hadoop/0,
         unregister_hive/0, unregister_hadoop/0]).

%-------------------------------------------------------------------------------

start() ->
  register_hive(),
  register_mapred().

stop() ->
  unregister_hive(),
  unregister_mapred().

register_hive() ->
  grep_hive(),
  select_rank_hive(),
  agg_uv_hive(),
  join_uv_hive().

register_mapred() ->
  grep_mapred(),
  select_rank_mapred(),
  agg_uv_mapred(),
  join_uv_mapred().

unregister_hive() ->
  dron_api:unregister_job("GrepHive"),
  dron_api:unregister_job("SelRankHive"),
  dron_api:unregister_job("AggUVHive"),
  dron_api:unregister_job("JoinUVHive").

unregister_mapred() ->
  dron_api:unregister_job("GrepMR"),
  dron_api:unregister_job("SelRankMR"),
  dron_api:unregister_job("AggUVMR"),
  dron_api:unregister_job("JoinUVMR").

test_path() ->
  "/home/ubuntu/Dron/benchmark/".

grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "GrepHive"
                             cmd_line = "hive -f " ++ test_path() ++ "grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropGrep",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

select_rank_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "SelRankHive",
                             cmd_line = "hive -f " ++ test_path() ++ "selrank.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_select_rank_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropSelRank",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_selrank.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

agg_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "AggUVHive",
                             cmd_line = "hive -f " ++ test_path() ++ "agguv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_agg_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropAggUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_agguv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "JoinUVHive",
                             cmd_line = "hive -f " ++ test_path() ++ "joinuv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

drop_join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropJoinUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_joinuv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

grep_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "GrepMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Grep /input/grep/ /output/grep/ -m 380 -r 0 -Dmapreduce.grep.textfind=true -Dmapreduce.grep.pattern=XYZ -Dmapreduce.grep.match_group=-1;",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

select_rank_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "SelRankMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark1 /input/rankings/ /output/rankings/ -m 30 -r 30 -Dmapreduce.minpagerank=10",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

agg_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "AggUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark2 /input/uservisits/ /output/uservisits_agg/ -m 450 -r 60",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

join_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "JoinUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark3 /input/uservisits /input/rankings/ /output/uservisits_join/ -m 480 -r 60 -Dmapreduce.startdate=1999-01-01 -Dmapreduce.stopdate=2001-01-01",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10}).

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
