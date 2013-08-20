-module(kafka_protocol_tests).
-include_lib("eunit/include/eunit.hrl").

parse_messages_test() ->
    B = <<0,0,0,20,0,203,100,194,139,115,97,111,101,116,117,104,97,115,110,111,
        101,116,104,117,0,0,0,54,0,252,76,192,247,97,115,111,110,101,117,104,
        97,110,115,111,101,32,117,104,97,110,115,111,101,116,117,32,104,97,111,
        110,115,101,117,104,116,32,97,111,110,115,101,117,104,116,32,97,111,
        115,101,110,117,116>>,
    {M, Size} = kafka_protocol:parse_messages(B),
    ?assertEqual([<<"saoetuhasnoethu">>,
                  <<"asoneuhansoe uhansoetu haonseuht aonseuht aosenut">>], M),
    ?assertEqual(Size, size(B)).

p_test() ->
    B = <<0,0,0,161,0,133,213,112,36,55,55,46,49,
          57,54,46,50,53,49,46,50,51,50,32,49,51,
          50,55,54,54,53,52,53,49,32,47,119,47,
          103,111,108,100,95,112,117,114,99,104,
          97>>,
    {M, Size} = kafka_protocol:parse_messages(B),
    ?assertEqual([], M),
    ?assertEqual(0, Size).


offset_test() ->
    B = <<0,0,0,10,0,0,0,54,141,94,118,159,0,0,0,54,128,0,130,49,0,0,0,54,96,0,129,155,0,0,0,54,64,0,129,112,0,0,0,54,32,0,129,75,0,0,0,54,0,0,128,210,0,0,0,53,224,0,128,192,0,0,0,53,192,0,128,168,0,0,0,53,160,0,128,70,0,0,0,53,128,0,128,65>>,
    Offsets = kafka_protocol:parse_offsets(B),
    ?assertEqual(10, length(Offsets)),
    ?assertEqual(234300012191, hd(Offsets)).

offsets_default_to_partition_0_test() ->
    B = kafka_protocol:offset_request(<<"tpc">>, -1, 1),
    io:format("~p~n", [B]),
    ?assertEqual(
      <<0,0,0,23,0,4,0,3,116,112,99,0,0,0,0,255,255,255,255,255,255,255,255,0,0,0,1>>,
      B
    ).

offsets_for_partition_test() ->
    B = kafka_protocol:offset_request(<<"tpc">>, 123, -1, 1),
    io:format("~p~n", [B]),
    ?assertEqual(
      <<0,0,0,23,0,4,0,3,116,112,99,0,0,0,123,255,255,255,255,255,255,255,255,0,0,0,1>>,
      B
    ).


%% split_data_test() ->
%%     B1 = <<0,0,0,20,0,203,100,194,139,115,97,111,101,116,117,104,97,115,110,111,
%%            101,116,104,117,0,0,0,54,0,252,76,192,247,97,115,111,110,101,117,104>>,
%%     B2 = <<97,110,115,111,101,32,117,104,97,110,115,111,101,116,117,32,104,97,111,
%%            110,115,101,117,104,116,32,97,111,110,115,101,117,104,116,32,97,111,
%%            115,101,110,117,116>>,

%%     {M1, R1} = kafka_parser:parse_messages(B1),
%%     ?assertEqual([<<"saoetuhasnoethu">>], M1),
%%     ?assertEqual(R1,<<0,0,0,54,0,252,76,192,247,97,115,111,110,101,117,104>>),

%%     {M2, R2} = kafka_parser:parse_messages(<<R1/binary, B2/binary>>),
%%     ?assertEqual([<<"asoneuhansoe uhansoetu haonseuht aonseuht aosenut">>], M2),
%%     ?assertEqual(<<>>, R2).

compression_off_test() ->
  % magic value is 1 (compression enabled)
  % compression attribute is 0 (no compression codec, meaning payload is uncompressed)
  B = <<0,0,0,11,1,0,169,46,208,80,97,112,112,108,101,0,0,0,12,1,0,3,139,103,207,
          98,97,110,97,110,97,0,0,0,12,1,0,184,190,148,206,99,97,114,114,111,116>>,
    {M, Size} = kafka_protocol:parse_messages(B),
    ?assertEqual([<<"apple">>, <<"banana">>, <<"carrot">>], M),
    ?assertEqual(Size, size(B)).

fetch_without_partition_assumes_partition_0_test() ->
    Req = kafka_protocol:fetch_request(<<"tpc">>, 123, 1024),
    ?assertEqual(
      <<0,0,0,23,0,1,0,3,116,112,99,0,0,0,0,0,0,0,0,0,0,0,123,0,0,4,0>>,
      Req
    ).

fetch_with_partition_test() ->
    Req = kafka_protocol:fetch_request(<<"tpc">>, 5, 123, 1024),
    ?assertEqual(
      <<0,0,0,23,0,1,0,3,116,112,99,0,0,0,5,0,0,0,0,0,0,0,123,0,0,4,0>>,
      Req
    ).
