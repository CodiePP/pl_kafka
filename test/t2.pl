
% start a producer:
% ./bin/kafka-console-producer.sh --topic topic001 --bootstrap-server localhost:9092
% and write some data to the topic
% then, run this test:
%
% ?- [t2].
% ?- test.
%

:- use_module(sbcl(kafka)).

test :-
  run_test,
  halt.

run_test :-
  kafka_conf_new(Config),
  kafka_conf_set(Config, "client.id", "it-s-me"),
  kafka_conf_set(Config, "group.id", "1"),
  kafka_conf_set(Config, "bootstrap.servers", "localhost:9092"),

  kafka_consumer_new(Config, Consumer),

  kafka_subscribe(Consumer, 0, 0, ["topic001"]),

  % polls until first message
  repeat,
    kafka_consumer_poll(Consumer, 500, Msg, Meta),
    format("read payload: ~p~n     meta: ~p ~n",[Msg,Meta]),
  kafka_unsubscribe(Consumer),

  kafka_flush(Consumer, 500),
  format("destroying consumer ...~n",[]),
  kafka_destroy(Consumer),
  format("consumer destroyed~n",[]).


