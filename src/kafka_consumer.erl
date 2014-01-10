%% @doc
-module(kafka_consumer).
-author('Knut Nesheim <knutin@gmail.com>').

-behaviour(gen_server).

%% API
-export([start_link/5, start_link/6, get_current_offset/1, get_offsets/3, fetch/1, fetch_with_offset/1, set_offset/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {socket,
                start_offset,
                current_offset,
                offset_cb,
                max_size = 1024 * 1024,
                topic,
                partition
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, Port, Topic, Offset, OffsetCb) ->
    start_link(Host, Port, Topic, 0, Offset, OffsetCb).

start_link(Host, Port, Topic, Partition, Offset, OffsetCb) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic, Partition, Offset, OffsetCb], []).

get_current_offset(C) ->
    gen_server:call(C, get_current_offset).

get_offsets(C, Time, MaxNumber) ->
    gen_server:call(C, {get_offsets, Time, MaxNumber}).

set_offset(C, Offset) ->
    gen_server:call(C, {set_offset, Offset}).

fetch(C) ->
    gen_server:call(C, fetch).

fetch_with_offset(C) ->
    gen_server:call(C, fetch_with_offset).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port, Topic, Partition, Offset, OffsetCb]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, false}, {packet, raw}]),
    {ok, #state{socket = Socket,
                topic = Topic,
                partition = Partition,
                start_offset = Offset,
                current_offset = Offset,
                offset_cb = OffsetCb
               }}.

handle_call(fetch, _From, #state{current_offset = Offset, partition = Partition} = State) ->
    Req = kafka_protocol:fetch_request(State#state.topic, Partition, Offset, State#state.max_size),
    ok = gen_tcp:send(State#state.socket, Req),

    case gen_tcp:recv(State#state.socket, 6) of
        {ok, <<2:32/integer, 0:16/integer>>} ->
            {reply, {ok, []}, State};
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            {Messages, Size} = kafka_protocol:parse_messages(Data),
            update_offset_callback(State, Offset, Offset + Size),
            {reply, {ok, Messages}, State#state{current_offset = Offset + Size}};
        {ok, <<_:32/integer, ErrorCode:16/integer>>} ->
            {reply, {error, kafka_protocol:error(ErrorCode)}, State}
    end;

handle_call(fetch_with_offset, From, State) ->
    case handle_call(fetch, From, State) of
        {reply, {ok, Messages}, StateNew} ->
            {reply, {ok, {StateNew#state.current_offset, Messages}}, StateNew};
        Other -> Other
    end;

handle_call({get_offsets, Time, MaxNumber}, _From, State) ->
    Req = kafka_protocol:offset_request(State#state.topic, State#state.partition, Time, MaxNumber),
    ok = gen_tcp:send(State#state.socket, Req),

    case gen_tcp:recv(State#state.socket, 6) of
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(State#state.socket, L-2),
            {reply, {ok, kafka_protocol:parse_offsets(Data)}, State};
        {ok, B} ->
            {reply, {error, B}, State}
    end;

handle_call(get_current_offset, _From, State) ->
    {reply, {ok, State#state.current_offset}, State};
handle_call({set_offset, Offset}, _From, State) ->
    {reply, ok, State#state{current_offset = Offset}}.



handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(Info, State) ->
    io:format("info: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


update_offset_callback(#state{offset_cb = Callback, topic = Topic, partition = Partition}, OldOffset, NewOffset) ->
  case erlang:fun_info(Callback, arity) of
    {arity, 3} -> Callback(Topic, OldOffset, NewOffset);
    {arity, 4} -> Callback(Topic, Partition, OldOffset, NewOffset)
  end.


