-module(dron_benchmark).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/0, start2/0, start3/0, start4/0]).

%-------------------------------------------------------------------------------

test_path() ->
  "/home/ubuntu/Dron/benchmark/".

start() ->
  create_hive(),
  register_hive(),
  register_mapred().

start2() ->
  create_hive(),
  register_hive(),
  timer:sleep(25000),
  register_mapred().

start3() ->
  create_hive(),
  agg_uv_mapred(),
  register_hive(),
  timer:sleep(70000),
  grep_mapred(),
  select_rank_mapred(),
  join_uv_mapred().

start4() ->
  create_agg_uv_hive(),
  create_ad_uv_hive(),
  agg_uv_hive(),
  ad_uv_hive(),
  join_uv_mapred(),
  timer:sleep(90000),
  create_grep_hive(),
  create_select_rank_hive(),
  create_join_uv_hive(),
  grep_mapred(),
  select_rank_mapred(),
  agg_uv_mapred(),
  grep_hive(),
  select_rank_hive(),
  join_uv_hive().  

create_hive() ->
  create_grep_hive(),
  create_select_rank_hive(),
  create_agg_uv_hive(),
  create_ad_uv_hive(),
  create_join_uv_hive().

register_hive() ->
  grep_hive(),
  select_rank_hive(),
  agg_uv_hive(),
  ad_uv_hive(),
  join_uv_hive().

register_mapred() ->
  grep_mapred(),
  select_rank_mapred(),
  agg_uv_mapred(),
  join_uv_mapred().

%-------------------------------------------------------------------------------
% Create tables
%-------------------------------------------------------------------------------

create_grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateGrepSel",
                             cmd_line = "hive -f " ++ test_path() ++ "create_grep_select.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).

create_select_rank_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateSelRankH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_rankings_select.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).

create_agg_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateAggUVH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_uservisits_agg.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).

create_ad_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateAdUVH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_uservisits_ad.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).

create_join_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "CreateJoinUVH",
                             cmd_line = "hive -f " ++ test_path() ++ "create_uservisits_join.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10000}).

%-------------------------------------------------------------------------------
% Benchmark
%-------------------------------------------------------------------------------

grep_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "GrepHive",
                             cmd_line = "hive -f " ++ test_path() ++ "grep.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CreateGrepSel", {today}}],
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
         dependencies = [],
         deps_timeout = 10000}).

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

select_rank_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "SelRankMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark1 /input/rankings/ /output/rankings/ -m 10 -r 10 -Dmapreduce.minpagerank=10",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10000}).

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

agg_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "AggUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark2 /input/uservisits/ /output/uservisits_agg/ -m 60 -r 10",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10000}).

ad_uv_hive() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(#job{name = "AdUVHive",
                             cmd_line = "hive -f " ++ test_path() ++ "aduv.hive",
                             start_time = StartTime,
                             frequency = 0,
                             timeout = 10000,
                             max_retries = 1,
                             dependencies = [{"CreateAdUVH", {today}}, {"AggUVHive", {today}}],
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

join_uv_mapred() ->
  StartTime = calendar:local_time(),
  dron_api:register_job(
    #job{name = "JoinUVMR",
         cmd_line = "hadoop jar " ++ test_path() ++ "jars/benchmarks.jar Benchmark3 /input/uservisits /input/rankings/ /output/uservisits_join/ -m 60 -r 10 -Dmapreduce.startdate=1999-01-01 -Dmapreduce.stopdate=2001-01-01",
         start_time = StartTime,
         frequency = 0,
         timeout = 10000,
         max_retries = 1,
         dependencies = [],
         deps_timeout = 10000}).
