-module(dron_language).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([instanciate_dependencies/1]).

%%------------------------------------------------------------------------------
%% @doc
%% Instiates a list of dependencies.
%%
%% @spec instanciate_dependencies(Dependencies) -> InstanciatedDependencies
%% @end
%%------------------------------------------------------------------------------
instanciate_dependencies(Deps) ->
  instanciate_dependencies(Deps, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
instanciate_dependencies([], IDeps) ->
  IDeps;
instanciate_dependencies([Dep | Deps], IDeps) ->
  {JName, DepLan} = Dep, 
  {ok, #job{start_time = STime,
            frequency = Freq}} = dron_db:get_job_unsync(JName),
  IDep = case DepLan of
           {today} -> today_dep({today}, JName, STime, Freq);
           {today, {PD, ND}} ->
             today_dep({today, {PD, ND}}, JName, STime, Freq);
           {hour} -> hour_dep({hour}, JName, STime, Freq);
           {hour, {PH, NH}} -> hour_dep({hour, {PH, NH}}, JName, STime, Freq);
           {minute} -> minute_dep({minute}, JName, STime, Freq);
           {minute, {PM, NM}} -> minute_dep({minute, {PM, NM}}, JName,
                                            STime, Freq);
           {second} -> second_dep({second}, JName, STime, Freq);
           {second, {PS, NS}} -> second_dep({second, {PS, NS}}, JName,
                                            STime, Freq)
         end,
  instanciate_dependencies(Deps, lists:append(IDep, IDeps)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
today_dep({today}, JName, STime, Freq) ->
  today_dep({today, {0, 0}}, JName, STime, Freq);
today_dep({today, {DPrev, DNext}}, JName, STime, Freq) ->
  {Date, _} = calendar:local_time(),
  CDay = calendar:date_to_gregorian_days(Date),
  SSec = calendar:datetime_to_gregorian_seconds(STime),
  PSec = calendar:datetime_to_gregorian_seconds(
           {calendar:gregorian_days_to_date(CDay - DPrev), {0, 0, 0}}),
  NSec = calendar:datetime_to_gregorian_seconds(
           {calendar:gregorian_days_to_date(CDay + DNext), {23, 59, 59}}),
  compute_deps(JName, SSec + (PSec - SSec) div Freq * Freq,
               PSec, NSec, Freq, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
hour_dep({hour}, JName, STime, Freq) ->
  hour_dep({hour, {0, 0}}, JName, STime, Freq);
hour_dep({hour, {HPrev, HNext}}, JName, STime, Freq) ->
  CSec = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
  SSec = calendar:datetime_to_gregorian_seconds(STime),
  PSec = CSec - HPrev * 3600,
  NSec = CSec + HNext * 3600 + 3559,
  compute_deps(JName, SSec + (PSec - SSec) div Freq * Freq,
               PSec, NSec, Freq, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
minute_dep({minute}, JName, STime, Freq) ->
  minute_dep({minute, {0, 0}}, JName, STime, Freq);
minute_dep({minute, {MPrev, MNext}}, JName, STime, Freq) ->
  CSec = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
  SSec = calendar:datetime_to_gregorian_seconds(STime),
  PSec = CSec - MPrev * 60,
  NSec = CSec + MNext * 60 + 59,
  compute_deps(JName, SSec + (PSec - SSec) div Freq * Freq,
               PSec, NSec, Freq, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
second_dep({second}, JName, STime, Freq) ->
  second_dep({second, {0, 0}}, JName, STime, Freq);
second_dep({second, {SPrev, SNext}}, JName, STime, Freq) ->
  CSec = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
  SSec = calendar:datetime_to_gregorian_seconds(STime),
  PSec = CSec - SPrev,
  NSec = CSec + SNext,
  compute_deps(JName, SSec + (PSec - SSec) div Freq * Freq,
               PSec, NSec, Freq, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
compute_deps(_JName, CTime, _BTime, ETime, _Freq, Deps) when CTime > ETime ->
  Deps;
compute_deps(JName, CTime, BTime, ETime, Freq, Deps) when CTime < BTime ->
  compute_deps(JName, CTime + Freq, BTime, ETime, Freq, Deps);
compute_deps(JName, CTime, BTime, ETime, Freq, Deps) ->
  compute_deps(
    JName, CTime + Freq, BTime, ETime, Freq,
    [{JName, calendar:gregorian_seconds_to_datetime(CTime)} | Deps]).
