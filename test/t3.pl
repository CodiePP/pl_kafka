
% this test demonstrates the receiving of data in batches
%
% start a producer:
% ./bin/kafka-console-producer.sh --topic topic001 --bootstrap-server localhost:9092
% and write some data to the topic
% then, run this test:
% $ swipl -l test/t3.pl -g test
% or:
% ?- [t3].
% ?- test.
%

:- use_module(sbcl(kafka)).

test :-
  run_test,
  halt.

% calculate offset Cnt before tail
rd_kafka_offset_tail(Cnt, Offset) :-
  RD_KAFKA_OFFSET_TAIL_BASE = -2000,
  Offset is RD_KAFKA_OFFSET_TAIL_BASE - Cnt.

run_test :-
  kafka_conf_new(Config),
  kafka_conf_set(Config, 'client.id', 'it-s-me'),
  kafka_conf_set(Config, 'group.id', '1'),
  kafka_conf_set(Config, 'bootstrap.servers', 'localhost:9092,host.docker.internal:9092'),

  kafka_consumer_new(Config, Consumer),

  kafka_topic_conf_new(TopiConf),
  kafka_topic_conf_set(TopiConf, 'acks', 'all'),
  kafka_topic_new(Consumer, 'topic001', TopiConf, Topic001),

  % always reads from the beginning of the topic
  %RD_KAFKA_OFFSET_BEGINNING = -2,
  % a follower on the topic; only receives newly added data
  %RD_KAFKA_OFFSET_END = -1,
  % restarts from where we left before
  RD_KAFKA_OFFSET_STORED = -1000,

  Partition = 0,

  format("starting...~n",[]),
  kafka_consume_start(Topic001, Partition, RD_KAFKA_OFFSET_STORED),

  ( kafka_consume_batch(Topic001, Partition, 1500, Msgs),
    length(Msgs, LL),
    format("read payload: len = ~p~n~p~n",[LL,Msgs])
  ;
    format("timed out~n",[])
  ),

  kafka_consume_stop(Topic001, Partition),
  kafka_consumer_close(Consumer),
  kafka_destroy(Consumer).

