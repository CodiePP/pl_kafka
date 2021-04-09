
% start a listener:
% ./bin/kafka-console-consumer.sh --topic topic001 --from-beginning --bootstrap-server localhost:9092
% then launch this test:
%
% ?- [t1].
% ?- test.
%

:- use_module(sbcl(kafka)).

test :-
  run_test,
  halt.

run_test :-
  kafka_conf_new(Config),
  kafka_conf_set(Config, "client.id", "it-s-me"),
  kafka_conf_set(Config, "bootstrap.servers", "localhost:9092,10.233.9.2:9092"),

  kafka_producer_new(Config, Producer),

  kafka_topic_conf_new(TopiConf),
  kafka_topic_conf_set(TopiConf, "acks", "all"),
  kafka_topic_new(Producer, "topic001", TopiConf, Topic001),

  kafka_produce(Topic001, "hello there."),
  kafka_produce(Topic001, -1, "keying..", 'key'),
  kafka_produce(Topic001, 0, 'sending in partition 0'),
  kafka_produce_batch(Topic001, -1, ['one', 'two', 'three']),

  format("written payload~n",[]),
  kafka_flush(Producer, 2000),
  format("flushed~n",[]),
  kafka_topic_destroy(Topic001),
  format("topic destroyed~n",[]).
  %kafka_topic_conf_destroy(TopiConf),
  %format("topic config destroyed~n",[]),
  %kafka_conf_destroy(Config),
  %format("config destroyed~n",[]).

