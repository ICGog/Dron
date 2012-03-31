-module(dron_benchmark).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/0, stop/0, clean/0]).

%-------------------------------------------------------------------------------

test_path() ->
  "/home/ubuntu/Dron/benchmark/".

start() ->
  create_data(),
  create_hive(),
  register_hive(),
  register_mapred().

stop() ->
  unregister_hive(),
  unregister_mapred().

clean() ->
  remove_hive(),
  remove_data().

create_data() ->
  create_grep(),
  copy_grep(),
  copy_rankings(),
  copy_uv().

remove_data() ->
  delete_grep_output(),
  delete_select_rank_output(),
  delete_agg_uv_output(),
  delete_join_uv_output(),
  delete_grep_hive(),
  delete_grep(),
  delete_rankings(),
  delete_uservisits().

create_hive() ->
  create_grep_hive(),
  create_select_rank_hive(),
  create_agg_uv_hive(),
  create_join_uv_hive().

remove_hive() ->
  drop_grep_hive(),
  drop_select_rank_hive(),
  drop_agg_uv_hive(),
  drop_join_uv_hive().

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

create_grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateGrepH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CopyGrep", {today}}],
                             deps_timeout = 10000}).

grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "GrepHive",
                             cmd_line = "hive -f " ++ test_path() ++ "grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CreateGrepH", {today}}],
                             deps_timeout = 10000}).

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
                             dependencies = [{"CreateSelRankH", {today}}],
                             deps_timeout = 10000}).

create_select_rank_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateSelRankH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_rankings.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CopyRankings", {today}}],
                             deps_timeout = 10000}).

drop_select_rank_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropSelRank",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_rankings_select.hive",
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
                             dependencies = [{"CreateAggUVH", {today}}],
                             deps_timeout = 10000}).

drop_agg_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropAggUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_uservisits_aggre.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

create_agg_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateAggUVH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_uservisits_agg.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CopyUV", {today}}],
                             deps_timeout = 10000}).

join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "JoinUVHive",
                             cmd_line = "hive -f " ++ test_path() ++ "joinuv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CreateJoinUVH", {today}}],
                             deps_timeout = 10000}).

drop_join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "DropJoinUV",
                             cmd_line = "hive -f " ++ test_path() ++ "drop_uservisits_join.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10}).

create_join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateJoinUVH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_uservisits_join.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CopyRankings", {today}}, {"CopyUV", {today}}],
                             deps_timeout = 10000}).

grep_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "GrepMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Grep /input/grep/ /output/grep/ -m 40 -r 0 -Dmapreduce.grep.textfind=true -Dmapreduce.grep.pattern=XYZ -Dmapreduce.grep.match_group=-1;",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [{"CopyGrep", {today}}],
         deps_timeout = 10000}).

select_rank_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "SelRankMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark1 /input/rankings/ /output/rankings/ -m 10 -r 10 -Dmapreduce.minpagerank=10",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [{"CopyRankings", {today}}],
         deps_timeout = 10000}).

agg_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "AggUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark2 /input/uservisits/ /output/uservisits_agg/ -m 60 -r 10",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [{"CopyUV", {today}}],
         deps_timeout = 10000}).

join_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "JoinUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark3 /input/uservisits /input/rankings/ /output/uservisits_join/ -m 60 -r 10 -Dmapreduce.startdate=1999-01-01 -Dmapreduce.stopdate=2001-01-01",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [{"CopyUV", {today}}, {"CopyRankings", {today}}],
         deps_timeout = 10000}).

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

copy_grep() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "CopyGrep",
         cmd_line = "hadoop fs -cp /input/grep/ /input/hive/grep/",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [{"CreateGrep", {today}}],
         deps_timeout = 10000}).
  
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
