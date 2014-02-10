%% @doc
-module(kafka_producer).

-behaviour(gen_server).

%% API
-export([start_link/2, produce/4, produce_ack/4]).

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

produce_ack(Topic, Partition, Payloads, Server) ->
    gen_server:call(Server, {produce_ack, {Topic, Partition, Payloads}}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port]) ->
    {ok, Socket} =
        gen_tcp:connect(Host, Port, [binary, {active, true}, {sndbuf, 0}, {buffer, 0}, {packet, raw}]),
    {ok, #state{socket = Socket}}.

handle_call({produce, {Topic, Partition, Payloads}}, _From, #state{ socket = Socket } = State) ->
    Req      = kafka_protocol:produce_request(Topic, Partition, Payloads),
    ok = gen_tcp:send(Socket, Req),
    {reply, ok, State};

handle_call({produce_ack, {Topic, Partition, Payloads}}, _From, #state{ socket = Socket } = State) ->
    ReqProduce = kafka_protocol:produce_request(Topic, Partition, Payloads),
    ReqOffset  = kafka_protocol:offset_request(Topic, Partition, -1, 1),
    inet:setopts(Socket,[{active, false}]),
    ok = gen_tcp:send(Socket, << ReqProduce/binary, ReqOffset/binary >>),
    Reply = case gen_tcp:recv(Socket, 6) of
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            [Offset] = kafka_protocol:parse_offsets(Data),
            {ok ,Offset};
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
