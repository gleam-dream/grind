%% A tiny, test-only TCP fault-injection proxy sitting between a Grind test
%% database connection and the real disposable PostgreSQL cluster. It never
%% closes a socket on its own initiative (a genuine half-open network fault,
%% unlike `pg_terminate_backend`, which closes the *server* side and lets the
%% client observe a fast, clean close). Loopback only; no TLS; a connect hang
%% (upstream never accepting) is not covered — see https://github.com/gleam-dream/grind/blob/510ca006d1af7ee35018676ee6aab026cc151b45/docs/RECOVERY-EVIDENCE.md.
%%
%% One controller process per `start/2` call owns the listen socket, the
%% current one-shot fault arming (`arm/3`), and the list of live per-connection
%% relay processes (so `stop/1` can kill them all, closing every socket the
%% relay owns and letting PostgreSQL notice the disconnect and roll back).
%%
%% Each accepted client connection gets its own relay process that owns both
%% the client-facing and upstream-facing sockets (`{active, once}` on both) and
%% asks the controller, for every chunk of bytes arriving from the client,
%% whether the currently armed fault (if not already used) matches this chunk.
%% Detection is a byte-pattern search for the extended-query-protocol `Parse`
%% message's null-terminated, unnamed-statement SQL text: `<<0, "commit", 0>>`
%% or `<<0, "begin", 0>>` (pog always sends lowercase SQL for its own
%% `begin`/`commit`/`rollback`), or an arbitrary caller-supplied pattern
%% (`{on_sql, Pattern}`) for a specific squirrel-generated statement. A small
%% trailing buffer (kept across chunks) lets the pattern match even when a
%% TCP segment boundary splits it — in practice these payloads are small
%% enough to arrive in one `recv` on loopback, but this makes no such
%% assumption a hard requirement.
%%
%% Fault actions, applied at most once per `arm/3` call (one-shot; call `arm/3`
%% again to re-arm):
%%   - `drop_reply`: forward the triggering chunk to the real server (so the
%%     statement actually executes and PostgreSQL genuinely processes it), but
%%     silently discard every subsequent server -> client byte for the rest of
%%     that connection. The client's blocking read never completes on its own.
%%   - `drop_request`: never forward the triggering chunk, or anything the
%%     client sends afterward, to the real server. The server never sees the
%%     statement at all and is left idle-in-transaction holding whatever locks
%%     the transaction already took; the client never receives a reply either.
%%
%% In both cases the relay keeps both sockets open and keeps relaying
%% traffic in the unaffected direction — this is a true half-open fault, not
%% a disguised close.
-module(grind_fault_proxy).

-export([start/2, stop/1, arm/3]).

%% ---------------------------------------------------------------------------
%% Public API
%% ---------------------------------------------------------------------------

%% start(UpstreamHost, UpstreamPort) -> {ok, Controller, ProxyPort}
%%   Controller: opaque handle for stop/1 and arm/3.
%%   ProxyPort: the ephemeral 127.0.0.1 port test databases should connect to
%%   instead of the real cluster port.
start(UpstreamHost, UpstreamPort) when is_binary(UpstreamHost) ->
    start(binary_to_list(UpstreamHost), UpstreamPort);
start(UpstreamHost, UpstreamPort) when is_list(UpstreamHost) ->
    Parent = self(),
    Ref = make_ref(),
    Controller = spawn(fun() -> controller_init(Parent, Ref, UpstreamHost, UpstreamPort) end),
    receive
        {Ref, {ok, Port}} -> {ok, {Controller, Port}};
        {Ref, {error, _Reason}} -> {error, nil}
    after 5000 ->
        {error, nil}
    end.

%% stop(Controller) -> nil
%%   Stops accepting new connections and kills every live relay, closing both
%%   of that relay's sockets. A relay's upstream socket closing lets
%%   PostgreSQL notice the disconnect and roll back any open transaction —
%%   the same recovery path a real dropped connection takes in production.
stop(Controller) ->
    Controller ! {stop, self()},
    receive
        stopped -> nil
    after 5000 ->
        nil
    end.

%% arm(Controller, Mode, NotifySubject) -> nil
%%   Mode: `pass` | `{armed, on_commit | on_begin | {on_sql, Pattern}, drop_reply | drop_request}`
%%     (the shape `grind/fault_proxy.gleam`'s `FaultMode` compiles to).
%%   NotifySubject: a `gleam/erlang/process.Subject` (or `undefined`) sent
%%     `{commit_seen, ConnId, UpstreamLocalPort}` the moment the fault fires.
%%     `UpstreamLocalPort` is this relay's own local port on its connection to
%%     the real PostgreSQL server — exactly the value that shows up as
%%     `pg_stat_activity.client_port` for that backend, letting an observer
%%     find and (if needed) terminate that exact backend.
%%   One-shot: fires for the first matching chunk seen after this call, then
%%   reverts to inert until armed again.
arm(Controller, Mode, NotifySubject) ->
    Controller ! {arm, Mode, NotifySubject},
    nil.

%% ---------------------------------------------------------------------------
%% Controller
%% ---------------------------------------------------------------------------

controller_init(Parent, Ref, UpstreamHost, UpstreamPort) ->
    case gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}, {ip, {127, 0, 0, 1}}]) of
        {ok, LSock} ->
            {ok, Port} = inet:port(LSock),
            Parent ! {Ref, {ok, Port}},
            Controller = self(),
            Acceptor = spawn_link(fun() -> accept_loop(LSock, Controller, UpstreamHost, UpstreamPort) end),
            controller_loop(#{
                lsock => LSock,
                acceptor => Acceptor,
                mode => pass,
                notify => undefined,
                used => false,
                relays => []
            });
        {error, Reason} ->
            Parent ! {Ref, {error, Reason}}
    end.

controller_loop(State) ->
    receive
        {arm, Mode, Notify} ->
            controller_loop(State#{mode => Mode, notify => Notify, used => false});
        {register_relay, Pid} ->
            #{relays := Relays} = State,
            controller_loop(State#{relays => [Pid | Relays]});
        {check, RelayPid, ConnId, LocalPort, Data} ->
            #{mode := Mode, notify := Notify, used := Used} = State,
            case (not Used) andalso trigger_matches(Mode, Data) of
                {true, Action} ->
                    RelayPid ! {verdict, Action},
                    notify(Notify, ConnId, LocalPort),
                    controller_loop(State#{used => true});
                false ->
                    RelayPid ! {verdict, pass},
                    controller_loop(State)
            end;
        {stop, From} ->
            #{lsock := LSock, acceptor := Acceptor, relays := Relays} = State,
            catch gen_tcp:close(LSock),
            catch exit(Acceptor, kill),
            lists:foreach(fun(Pid) -> catch exit(Pid, kill) end, Relays),
            From ! stopped,
            exit(normal)
    end.

notify(undefined, _ConnId, _LocalPort) ->
    ok;
notify({subject, Pid, Tag}, ConnId, LocalPort) ->
    Pid ! {Tag, {commit_seen, ConnId, LocalPort}},
    ok.

trigger_matches(pass, _Data) ->
    false;
trigger_matches({armed, on_commit, Action}, Data) ->
    match_pattern(Data, <<0, "commit", 0>>, Action);
trigger_matches({armed, on_begin, Action}, Data) ->
    match_pattern(Data, <<0, "begin", 0>>, Action);
trigger_matches({armed, {on_sql, Pattern}, Action}, Data) ->
    match_pattern(Data, Pattern, Action).

match_pattern(Data, Pattern, Action) ->
    case binary:match(Data, Pattern) of
        nomatch -> false;
        _ -> {true, Action}
    end.

%% ---------------------------------------------------------------------------
%% Acceptor + per-connection relay
%% ---------------------------------------------------------------------------

accept_loop(LSock, Controller, UpstreamHost, UpstreamPort) ->
    case gen_tcp:accept(LSock) of
        {ok, ClientSock} ->
            %% `gen_tcp:accept/1` makes the calling process (this acceptor)
            %% the socket's controlling process, so `{tcp, ClientSock, _}`
            %% messages would otherwise be delivered here, never to the
            %% relay process that actually calls `receive` on them. Transfer
            %% ownership to the relay before it touches the socket.
            Pid = spawn(fun() ->
                receive
                    go -> init_conn(ClientSock, Controller, UpstreamHost, UpstreamPort)
                end
            end),
            gen_tcp:controlling_process(ClientSock, Pid),
            Pid ! go,
            accept_loop(LSock, Controller, UpstreamHost, UpstreamPort);
        {error, closed} ->
            ok;
        {error, _Reason} ->
            accept_loop(LSock, Controller, UpstreamHost, UpstreamPort)
    end.

init_conn(ClientSock, Controller, UpstreamHost, UpstreamPort) ->
    case gen_tcp:connect(UpstreamHost, UpstreamPort, [binary, {packet, raw}, {active, false}]) of
        {ok, ServerSock} ->
            {ok, LocalPort} = inet:port(ServerSock),
            Controller ! {register_relay, self()},
            ConnId = erlang:unique_integer([positive]),
            inet:setopts(ClientSock, [{active, once}]),
            inet:setopts(ServerSock, [{active, once}]),
            relay_loop(#{
                client => ClientSock,
                server => ServerSock,
                controller => Controller,
                conn_id => ConnId,
                local_port => LocalPort,
                trailing => <<>>,
                discard_client => false,
                discard_server => false
            });
        {error, _Reason} ->
            catch gen_tcp:close(ClientSock)
    end.

relay_loop(S) ->
    #{client := C, server := Srv} = S,
    receive
        {tcp, C, Data} ->
            S1 = handle_client_data(S, Data),
            inet:setopts(C, [{active, once}]),
            relay_loop(S1);
        {tcp, Srv, Data} ->
            S1 = handle_server_data(S, Data),
            inet:setopts(Srv, [{active, once}]),
            relay_loop(S1);
        {tcp_closed, C} ->
            catch gen_tcp:close(Srv),
            ok;
        {tcp_closed, Srv} ->
            catch gen_tcp:close(C),
            ok;
        {tcp_error, _Sock, _Reason} ->
            catch gen_tcp:close(C),
            catch gen_tcp:close(Srv),
            ok
    end.

handle_client_data(S = #{discard_client := true}, _Data) ->
    %% True half-open: bytes are neither forwarded nor acknowledged, and the
    %% socket is never closed.
    S;
handle_client_data(S, Data) ->
    #{
        controller := Controller,
        conn_id := ConnId,
        local_port := LocalPort,
        trailing := Trailing,
        server := Srv
    } = S,
    Combined = <<Trailing/binary, Data/binary>>,
    Controller ! {check, self(), ConnId, LocalPort, Combined},
    Verdict =
        receive
            {verdict, V} -> V
        after 2000 -> pass
        end,
    NewTrailing = trailing_tail(Combined),
    case Verdict of
        pass ->
            gen_tcp:send(Srv, Data),
            S#{trailing => NewTrailing};
        drop_reply ->
            gen_tcp:send(Srv, Data),
            S#{trailing => NewTrailing, discard_server => true};
        drop_request ->
            S#{trailing => NewTrailing, discard_client => true}
    end.

handle_server_data(S = #{discard_server := true}, _Data) ->
    %% True half-open in the other direction: the request already reached
    %% PostgreSQL and was processed, but the client never sees the reply.
    S;
handle_server_data(S, Data) ->
    #{client := C} = S,
    gen_tcp:send(C, Data),
    S.

trailing_tail(Bin) ->
    Keep = 16,
    Len = byte_size(Bin),
    case Len > Keep of
        true -> binary:part(Bin, Len - Keep, Keep);
        false -> Bin
    end.
