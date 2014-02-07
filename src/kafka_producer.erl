%% @doc
-module(kafka_producer).

-behaviour(gen_server).

%% API
-export([start_link/2, produce/4, get_offsets/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {socket}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

produce(Topic, Partition, Payloads, Server) ->
    gen_server:call(Server, {produce, {Topic, Partition, Payloads}}).

get_offsets(Topic, Partition, Time, MaxNumber, Server) ->
    gen_server:call(Server, {get_offsets, Topic, Partition, Time, MaxNumber}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port]) ->
    {ok, Socket} =
        gen_tcp:connect(Host, Port, [binary, {active, true}, {sndbuf, 0}, {buffer, 0}, {packet, raw}]),
    {ok, #state{socket = Socket}}.

handle_call({produce, {Topic, Partition, Payloads}}, _From, #state{ socket = Socket } = State) ->
    Req      = kafka_protocol:produce_request(Topic, Partition, Payloads),
    TCPReply = gen_tcp:send(Socket, Req),
    Reply    = receive
        Msg = {tcp_closed, _} ->
            self() ! Msg,
            {error, closed}
    after 0 ->
        TCPReply
    end,
    {reply, Reply, State};

handle_call({get_offsets, Topic, Partition, Time, MaxNumber}, _From,
    State = #state{ socket = Socket }) ->

    Req = kafka_protocol:offset_request(Topic, Partition, Time, MaxNumber),
    inet:setopts(Socket,[{active, false}]),
    ok    = gen_tcp:send(Socket, Req),
    Reply = case gen_tcp:recv(Socket, 6) of
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            {ok, kafka_protocol:parse_offsets(Data)};
        {ok, B} ->
            {error, B}
    end,
    inet:setopts(Socket, [{active, true}]),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, _}, State) ->
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
