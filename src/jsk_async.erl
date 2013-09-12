%% vim: set ts=4 sts=4 sw=4 et:
%%
%% Our system tends to kill "stuck" processes but we absolutely must complete
%% some parts of code without interruptions.
%% 
%% The jsk_async module allows creation of non-linked processes and
%% following them through completion.
%%
%% When the wait completes, the :join or :wait calls can be used
%% to pickup any final process result or exception.
%%
%% API:
%% run/1           - Runs a specified function asynchronously.
%% join/1,2        - Waits until process completes and returns its value
%%                   or transparently propagates its errors and exceptions.
%% wait/1,2        - Same as join/1,2, but returns errors and exceptions
%%                   as values instead of raising them.
%% complete/1      - Start function asynchronously and join/1 it.
%% Mnemonics: a 'join' is a "tighter" version of 'wait'. Losely based on POSIX
%% thread joining idioms.
%% apply_after/2  - Call the second function after the first finishes.
%%
%%
-module(jsk_async).
-export([
        apply_after/2,
        complete/1,
        join/1,
        join/2,
        pmap/5,
        pmap/6,
        run/1,
        wait/1,
        wait/2
    ]).

%% Run a process asynchronously executing a specified function.
%% The process reference returned by this function can be used by other
%% processes for pickup: the first process which executes :join or :wait wins.
%% @spec run(fun()) -> Key
%% Key = term()
run(Fun) ->
    Parent = self(),
    APQRef = make_ref(), % Async/Pickuper Query Reference
    Pickuper = spawn(fun() -> pickuper(Parent) end),
    APid = spawn_opt(fun() ->
                receive {APQRef, run} ->
                        Pickuper ! {APQRef, monitor_pid, self()} end,
                unlink(Parent),
                Pickuper ! {APQRef, return, try {value, Fun()} catch
                        Class:Reason -> { exception,
                                {Class, Reason, erlang:get_stacktrace()} }
                    end
                }
        end, [link]),
    APid ! {APQRef, run},
    unlink(APid), % Make sure we can spawn and exit instantly!
    {Pickuper, APid}.

%% Wait for the process run by jsk_async:run/1 to finish
%% and return its output or transparently forwards its exception.
%%
%% @spec join(Key) -> Value
%% @spec join(Key, Timeout) -> Value
%% Value = term()
%% Timeout = int()
join(P) -> join(P, infinity).
join({Pickuper,_Pid}, Timeout) ->
    Monitor = erlang:monitor(process, Pickuper),
    RequestRef = make_ref(),
    Pickuper ! {attach_waiter, self(), RequestRef},
    receive
        {reply, RequestRef, {value, Value}} ->
            erlang:demonitor(Monitor),
            Value;
        {reply, RequestRef, {exception, {C,R,S}}} ->
            erlang:demonitor(Monitor),
            erlang:raise(C, R, S);
        {'DOWN', Monitor, process, Pickuper, _Info} ->
            erlang:raise(error, {jsk_async, noproc}, [])
    after Timeout ->
            erlang:demonitor(Monitor),
            Pickuper ! {forget_waiter, self(), RequestRef},
            erlang:raise(error, {jsk_async, timeout}, [])
    end.


%% A safe version of :join/1,2. It does not throw up if something is wrong,
%% just returns {Error, Reason} instead of default {ok, Value}:
%%
%% @spec wait(Key) -> {ok, Value} | {Error, Reason}
%% @spec wait(Key, Timeout) -> {ok, Value} | {Error, Reason}
%% Value = term()
%% Timeout = int()
%% Error = exit | error | throw
%% Reason = term()
wait(P) -> wait(P, infinity).
wait(P, T) ->
    try join(P, T) of
        Value -> {ok, Value}
        catch
            Class:Reason -> {Class, Reason}
        end.

complete(F) -> join(run(F)).

%% Run a process asynchronously executing a specified function.
%% After the specified function finishes, the after/2 gives the final value
%% or the error code the ValueHandler function.
%% @spec apply_after(Function, ValueHandler) -> Key
%% Function = fun() -> Value
%% Key = term()
%% ValueHandler = fun(Key, WrappedValue) -> ok
%% WrappedValue = {ok, Value} | {error, Reason}
apply_after(Fun, ValueHandler) ->
    RefKey = make_ref(),
    spawn_opt(fun() ->
        process_flag(trap_exit, true),
        Value = try Fun() of
            V -> {ok, V}
        catch Class:Reason -> {error,
                        {Class, Reason, erlang:get_stacktrace()}}
        end,
        receive
            {'EXIT', _, Info} ->
                ValueHandler(RefKey, {error, {exit, Info}})
        after 0 ->
            ValueHandler(RefKey, Value)
        end
    end, []),
    RefKey.

%% Perform a parallel mapping with a desired concurrency level
%% and a global timeout. If not all item were processed in the allotted time,
%% this function returns them in the Failed list.
%% @spec pmap(...) -> {Ok, Failed}
%% Types Ok = list()
%%       Failed = list()
pmap(ConcurrencyLevel, Timeout, Fun, List, Default) ->
    pmap(ConcurrencyLevel, Timeout, Fun, List, Default, false).

pmap(ConcurrencyLevel, Timeout, Fun, List, Default, NeedOrder) ->
    pmap(ConcurrencyLevel, Timeout, Fun, List, Default, NeedOrder, 0).

pmap(_ConcurrencyLevel,_Timeout,_Fun, [], _Default, _NeedOrder, _Processed) -> {[], []};
pmap(ConcurrencyLevel, Timeout, Fun, List, Default, NeedOrder, Processed)
        when ConcurrencyLevel > 0, Timeout > 0, is_list(List) ->
    {A, B, C} = now(),
    Until = {A, B, C + (Timeout * 1000)},
    Tab = ets:new(pmap, [private]),
    try
        pmap(0, ConcurrencyLevel, Until, Tab, [], [], Fun, List, Default, NeedOrder, Processed)
    after
        ets:delete(Tab)
    end.

pmap_result(NeedOrder, OK) ->
    {_, Res} = lists:unzip(case NeedOrder of
            true -> lists:sort(OK);
            false -> OK
        end),
    Res.

pmap(0,_CLevel,_Until,_Tab,OK,FAIL,_Fun,[],_Default, NeedOrder, _Processed) ->
    {pmap_result(NeedOrder, OK), FAIL};
pmap(InFlight, CLevel, Until, Tab, OK, FAIL, Fun, [H|List], Default, NeedOrder, Processed)
        when InFlight < CLevel ->
    {Ref, Pid} = pmap_spawn(Tab, Processed, fun() -> Fun(H) end),
    ets:insert_new(Tab, {Ref, Pid, H}),
    pmap(InFlight + 1, CLevel, Until, Tab, OK, FAIL, Fun, List, Default, NeedOrder, Processed + 1);
pmap(InFlight, CLevel, Until, Tab, OK, FAIL, Fun, List, Default, NeedOrder, Processed) ->
    TimeDiffMs = case timer:now_diff(Until, now()) of
        Us when Us > 0 -> Us div 1000;
        _ -> 0
    end,
    receive
        {Tab, Ref, Order, Return, V} ->
            {NewOK, NewFAIL} = case Return of
                return ->
                    true = ets:member(Tab, Ref),
                    {[{Order, V}|OK], FAIL};
                exception ->
                    Item = ets:lookup_element(Tab, Ref, 3),
                    {OK, [pmap_apply_default(Default, Item, V)|FAIL]}
            end,
            ets:delete(Tab, Ref),
            pmap(InFlight-1, CLevel, Until,
                Tab, NewOK, NewFAIL, Fun, List, Default, NeedOrder, Processed)
    after TimeDiffMs ->
        ExpiredList = ets:foldl(fun({Ref, Pid, Item}, ResultItems) ->
            Pid ! do_not_answer,
            [Item | ResultItems]
        end, [], Tab),
        {pmap_result(NeedOrder, OK), lists:foldl(fun(Item, A) ->
            [pmap_apply_default(Default, Item, timeout) | A]
        end, FAIL, ExpiredList ++ List)}
    end.

pmap_spawn(Key, Order, F) ->
    Self = self(),
    Ref = make_ref(),
    Pid = spawn_opt(fun() ->
        {Type, Value} = try
            V = F(),
            {return, V}
        catch
            Class:Reason ->
                {exception, {Class, Reason, erlang:get_stacktrace()}}
        end,
        receive
            do_not_answer ->
                nop
        after 0 ->
            Self ! {Key, Ref, Order, Type, Value}
        end
    end, [link]),
    {Ref, Pid}.

pmap_apply_default(Default, _Item, _) when is_tuple(Default) -> Default;
pmap_apply_default(Default, Item, _) when is_function(Default, 1) ->
    Default(Item);
pmap_apply_default(Default, Item, Reason) when is_function(Default, 2) ->
    Default(Item, Reason);
pmap_apply_default(Default, _Item, _) -> Default.

%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%

pickuper(Parent) ->
    ParentMon = erlang:monitor(process, Parent),
    receive
        {APQRef, monitor_pid, Pid} ->
            PRM = {Pid, APQRef, erlang:monitor(process, Pid)},
            pickuper(PRM, nostatus, {Parent, ParentMon, noref})
    end.

pickuper({AsyncPid, APQRef, AsyncProcMon} = PRM, ExitStatus, {OWPid, OWMon, OReqRef} = Waiter) ->
    receive
        {APQRef, return, Status} ->
            erlang:demonitor(AsyncProcMon, [flush]),
            case OReqRef of
                noref -> pickuper(PRM, Status, Waiter);
                _ -> notify(Waiter, Status)
            end;
        {attach_waiter, WaiterPid, ReqRef} ->
            NewWaiter = {WaiterPid,
                if WaiterPid /= OWPid ->
                        erlang:demonitor(OWMon),
                        erlang:monitor(process, WaiterPid);
                    true -> OWMon
                end,
                ReqRef},
            % Generate error exception to indicate
            % error to the previous waiters. Only one
            % waiter at a time can be present, so we kick
            % the old one out and install the new one.
            notify(Waiter, {exception,
                    {error, {jsk_async, stolen_wait}, []}}),
            case ExitStatus of
                nostatus -> pickuper(PRM, ExitStatus, NewWaiter);
                _ -> notify(NewWaiter, ExitStatus)
            end;
        {forget_waiter, OWPid, OReqRef} -> ok;
        {'DOWN', AsyncProcMon, process, AsyncPid, Info} ->
            % If we get this, it means something is very, very
            % wrong with the monitored process. We should have
            % received the {Ref, return, Status} message above.
            MyStatus = {exception, {exit, {jsk_async, Info}, []}},
            case OReqRef of
                noref -> pickuper(PRM, MyStatus, Waiter);
                _ -> notify(Waiter, MyStatus)
            end;
        {'DOWN', OWMon, process, OWPid, _Info} -> ok
    end.

notify({Pid, _Mon, ReqRef}, Status) when is_reference(ReqRef) ->
    Pid ! {reply, ReqRef, Status}, ok;
notify(_, _) -> ok.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->

    FCompareA = fun(Arg) -> a = Arg end,
    FTestA = fun(a) -> {ok} end,

    [{"join; join:noproc", fun() ->
        R = jsk_async:run(fun() -> test end),
        ?assertEqual(test, jsk_async:join(R)),
        ?assertEqual({jsk_async, noproc}, try jsk_async:join(R) catch error:E -> E end)
    end},
    {"join:throw", fun() ->
        R = jsk_async:run(fun() -> throw (foo) end),
        ?assertEqual(foo, try jsk_async:join(R) catch throw:T -> T end)
    end},
    {"join:error", fun() ->
        R = jsk_async:run(fun() -> FCompareA(b) end),
        ?assertEqual({badmatch, b}, try jsk_async:join(R) catch error:E -> E end)
    end},
    {"join:function_clause", fun() ->
        R = jsk_async:run(fun() -> FTestA(b) end),
        ?assertEqual(function_clause, try {jsk_async:join(R)} catch error:E -> E end)
    end},
    {"join:exit", fun() ->
        R = jsk_async:run(fun() -> exit(normal) end),
        ?assertEqual(normal, try jsk_async:join(R) catch exit:E -> E end)
    end},
    {"join:exit some_status", fun() ->
        R = jsk_async:run(fun() -> exit(some_status) end),
        ?assertEqual({exit, some_status}, try jsk_async:join(R) catch C:E -> {C, E} end)
    end},
    {"join:timeout; join:noproc", fun() ->
        R = jsk_async:run(fun() -> timer:sleep(1000), tmo end),
        ?assertEqual({error, {jsk_async, timeout}}, try jsk_async:join(R, 500) catch C1:E1 -> {C1, E1} end),
        % Double-join is prohibited, simulating total ignorance.
        ?assertEqual({error, {jsk_async, noproc}}, try jsk_async:join(R) catch C2:E2 -> {C2, E2} end)
    end},
    {"wait; wait:noproc", fun() ->
        R = jsk_async:run(fun() -> test end),
        ?assertEqual({ok, test}, jsk_async:wait(R)),
        ?assertEqual({error, {jsk_async, noproc}}, jsk_async:wait(R))
    end},
    {"wait:throw", fun() ->
        R = jsk_async:run(fun() -> throw (foo) end),
        ?assertEqual({throw, foo}, jsk_async:wait(R))
    end},
    {"wait:error", fun() ->
        R = jsk_async:run(fun() -> FCompareA(b) end),
        ?assertEqual({error, {badmatch,b}}, jsk_async:wait(R))
    end},
    {"wait:function_clause", fun() ->
        R = jsk_async:run(fun() -> FTestA(b) end),
        ?assertEqual({error, function_clause}, jsk_async:wait(R))
    end},
    {"wait:exit", fun() ->
        R = jsk_async:run(fun() -> exit(normal) end),
        ?assertEqual({exit, normal}, jsk_async:wait(R))
    end},
    {"wait:exit some_status", fun() ->
        R = jsk_async:run(fun() -> exit(some_status) end),
        ?assertEqual({exit, some_status}, jsk_async:wait(R))
    end}].

pmap_test_() ->
    Fun = fun
        (0) ->
            timer:sleep(200),
            0;
        (V) ->
            V
    end,
    DefaultFun = fun(Item, Reason) ->
        {Item, Reason}
    end,
    [{"normal", fun() ->
        Items = lists:seq(1, 1000),
        ?assertEqual({Items, []},
            jsk_async:pmap(10, 1000, Fun, Items, DefaultFun, true))
    end},
    {"timeout:working", fun() ->
        Items = [0, 1, 2],
        ?assertEqual({[1, 2], [
            {0, timeout}
        ]}, jsk_async:pmap(10, 100, Fun, Items, DefaultFun, true))
    end},
    {"timeout:qeued", fun() ->
        Items = [0, 0],
        ?assertEqual({[], [
            {0, timeout},
            {0, timeout}
        ]}, jsk_async:pmap(1, 100, Fun, Items, DefaultFun, true))
    end},
    {"cleanup:mailbox", fun() ->
        Items = [0],
        messages_receive(),
        ?assertEqual({[], [
            {0, timeout}
        ]}, jsk_async:pmap(10, 100, Fun, Items, DefaultFun)),
        timer:sleep(200),
        ?assertEqual(0, messages_receive())
    end},
    {"cleanup:processes", fun() ->
        Items = [0],
        Tab = ets:new(test, [set, public]),
        try
            ?assertEqual(true, ets:insert_new(Tab, {1, undefined})),
            ChangeFun = fun(0) ->
                timer:sleep(100),
                ets:insert(Tab, {1, changed})
            end,
            ?assertEqual({[], [
                {0, timeout}
            ]}, jsk_async:pmap(10, 100, ChangeFun, Items, DefaultFun)),
            timer:sleep(200),
            ?assertEqual([{1, changed}], ets:lookup(Tab, 1))
        after
            ets:delete(Tab)
        end
    end}].

% test private functions

messages_receive() ->
    messages_receive(0).

messages_receive(Count) ->
    receive
        Message ->
            messages_receive(Count + 1)
    after 0 ->
        Count
    end.


-endif.
