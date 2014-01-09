%% @doc Parse and generate kafka protocol objects.
%% See https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka

-module(kafka_protocol).
-author('Knut Nesheim <knutin@gmail.com>').

-export([produce_request/3, fetch_request/3, fetch_request/4, offset_request/3, offset_request/4]).
-export([encode_messages/1, parse_messages/1, parse_offsets/1, error/1]).

-define(FETCH, 1).
-define(OFFSETS, 4).


fetch_request(Topic, Offset, MaxSize) ->
    fetch_request(Topic, 0, Offset, MaxSize).

fetch_request(Topic, Partition, Offset, MaxSize) ->
    TopicSize = size(Topic),

    RequestSize = 2 + 2 + TopicSize + 4 + 8 + 4,

    <<RequestSize:32/integer,
      ?FETCH:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      Offset:64/integer,
      MaxSize:32/integer>>.

offset_request(Topic, Time, MaxNumber) ->
    offset_request(Topic, 0, Time, MaxNumber).

offset_request(Topic, Partition, Time, MaxNumber) ->
    TopicSize = size(Topic),
    RequestSize = 2 + 2 + TopicSize + 4 + 8 + 4,
    <<RequestSize:32/integer,
      ?OFFSETS:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      Time:64/integer,
      MaxNumber:32/integer>>.

produce_request(Topic, Partition, Payloads) when is_list(Payloads) ->
    RequestType    = 0,
    Messages       = encode_messages(Payloads),
    MessagesLength = byte_size(Messages),
    TopicLength    = byte_size(Topic),

    Request = <<
        RequestType:16/integer,
        TopicLength:16/integer,
        Topic/binary,
        Partition:32/integer,
        MessagesLength:32/integer,
        Messages/binary >>,

    RequestLength  = byte_size(Request),
    << RequestLength:32/integer, Request/binary >>.

encode_messages(Payloads) ->
    Messages    = lists:map(fun encode_message/1, Payloads),
    << << Message/binary >> || Message <- Messages >>.

encode_message(Payload) ->
    Length   = 1 + 4 + byte_size(Payload),
    Magic    = 0,
    Checksum = erlang:crc32(Payload),
    << Length:32/integer, Magic:8/integer, Checksum:32/integer, Payload/binary >>.

parse_messages(Bs) ->
    parse_messages(Bs, [], 0).

parse_messages(<<>>, Acc, Size) ->
    {lists:reverse(Acc), Size};

parse_messages(<<L:32/integer, _/binary>> = B, Acc, Size) when size(B) >= L + 4->
    {Msg, Rest} = parse_message(L, B),
    parse_messages(Rest, [Msg | Acc], Size + L + 4);

parse_messages(_B, Acc, Size) ->
    {lists:reverse(Acc), Size}.

parse_message(MsgLength, <<_:32/integer, 0:8/integer, Rest0/bitstring>>) ->
    % Magic bit = 0
    PayloadLength = MsgLength - 4 - 1,
    << _Check:32/integer, Msg:PayloadLength/binary, Rest/bitstring>> = Rest0,
    {Msg, Rest};

parse_message(MsgLength, <<_:32/integer, 1:8/integer, Rest0/bitstring>>) ->
    % Magic bit = 0, codec = none
    PayloadLength = MsgLength - 4 - 1 - 1,
    <<0:8, _Check:32/integer, Msg:PayloadLength/binary, Rest/bitstring>> = Rest0,
    {Msg, Rest}.

parse_offsets(B) ->
    <<_:32/integer, Offsets/binary>> = B,
    parse_offsets(Offsets, []).

parse_offsets(<<>>, Acc) ->
    lists:reverse(Acc);

parse_offsets(B, Acc) ->
    <<Offset:64/integer, Rest/binary>> = B,
    parse_offsets(Rest, [Offset | Acc]).

error(-1) -> unknown_error;
error( 1) -> invalid_offset;
error( 2) -> corrupt_message;
error( 3) -> invalid_partion;
error( 4) -> message_larger_than_max_size.
