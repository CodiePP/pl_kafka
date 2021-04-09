/*-------------------------------------------------------------------------*/
/* Prolog Interface to Kafka                                               */
/*                                                                         */
/* File  : kafka.pl                                                        */
/* Author: Alexander Diemand                                               */
/*                                                                         */
/* Copyright (C) 2021 Alexander Diemand                                    */
/*                                                                         */
/*   This program is free software: you can redistribute it and/or modify  */
/*   it under the terms of the GNU General Public License as published by  */
/*   the Free Software Foundation, either version 3 of the License, or     */
/*   (at your option) any later version.                                   */
/*                                                                         */
/*   This program is distributed in the hope that it will be useful,       */
/*   but WITHOUT ANY WARRANTY; without even the implied warranty of        */
/*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         */
/*   GNU General Public License for more details.                          */
/*                                                                         */
/*   You should have received a copy of the GNU General Public License     */
/*   along with this program.  If not, see <http://www.gnu.org/licenses/>. */
/*-------------------------------------------------------------------------*/

:- module(kafka, [ kafka_version/1
                 , kafka_conf_new/1
                 , kafka_conf_set/3
                 , kafka_conf_destroy/1
                 , kafka_topic_conf_new/1
                 , kafka_topic_conf_set/3
                 , kafka_topic_conf_destroy/1
                 , kafka_topic_new/4
                 , kafka_topic_destroy/1
                 , kafka_consumer_new/2
                 , kafka_producer_new/2
                 , kafka_destroy/1
                 , kafka_conf_dump/2
                 , kafka_produce/2
                 , kafka_produce/3
                 , kafka_produce/4
                 , kafka_produce_batch/3
                 , kafka_flush/2
                 , kafka_consumer_poll/4
                 , kafka_subscribe/4
                 , kafka_unsubscribe/1
                 , kafka_consumer_close/1
                 , kafka_consume_batch/4
                 , kafka_consume_start/3
                 , kafka_consume_stop/2
                 ]).

:- use_foreign_library(sbcl('plkafka')).

% kafka_version(String)
kafka_version(Kv) :-
  pl_kafka_version(Kv).

% kafka_conf_new(-Cid)
kafka_conf_new(Cid) :-
  var(Cid),
  pl_kafka_conf_new(Cid).

% kafka_topic_conf_new(-Cid)
kafka_topic_conf_new(Cid) :-
  var(Cid),
  pl_kafka_topic_conf_new(Cid).

% kafka_conf_set(+Cid, +Key, +Value)
kafka_conf_set(Cid, K, V) :-
  nonvar(Cid), nonvar(K), nonvar(V),
  pl_kafka_conf_set(Cid, K, V).

% kafka_topic_conf_set(+Cid, +Key, +Value)
kafka_topic_conf_set(Cid, K, V) :-
  nonvar(Cid), nonvar(K), nonvar(V),
  pl_kafka_topic_conf_set(Cid, K, V).

% kafka_conf_destroy(+Cid)
kafka_conf_destroy(Cid) :-
  nonvar(Cid),
  pl_kafka_conf_destroy(Cid).

% kafka_topic_conf_destroy(+Cid)
kafka_topic_conf_destroy(Cid) :-
  nonvar(Cid),
  pl_kafka_topic_conf_destroy(Cid).

% kafka_consumer_new(+Cid, -Consumer)
kafka_consumer_new(Cid, Consumer) :-
  nonvar(Cid), var(Consumer),
  pl_kafka_consumer_new(Cid, Consumer).

% kafka_producer_new(+Cid, -Producer)
kafka_producer_new(Cid, Producer) :-
  nonvar(Cid), var(Producer),
  pl_kafka_producer_new(Cid, Producer).

% kafka_destroy(+Client)
kafka_destroy(Client) :-
  nonvar(Client),
  pl_kafka_destroy(Client).

% kafka_conf_dump(+Cid, -ConfPairs)
kafka_conf_dump(Cid, ConfPairs) :-
  var(ConfPairs),
  pl_kafka_conf_dump(Cid, ConfPairs).

% kafka_topic_new(+Cid, +Producer, +String, -Topic)
kafka_topic_new(Cid, Producer, TopicName, Topic) :-
  nonvar(Cid), nonvar(producer),
  nonvar(TopicName), var(Topic),
  pl_kafka_topic_new(Cid, Producer, TopicName, Topic).

% kafka_topic_destroy(+Topic)
kafka_topic_destroy(Topic) :-
  nonvar(Topic),
  pl_kafka_topic_destroy(Topic).

% kafka_produce(+Topic, +String)
kafka_produce(Topic, Payload) :-
  nonvar(Topic),
  ( string(Payload) ; atom(Payload) ),
  % partition unassigned (-1)
  pl_kafka_produce(Topic, -1, Payload, '').

% kafka_produce(+Topic, +Integer, +String)
kafka_produce(Topic, Partition, Payload) :-
  nonvar(Topic), integer(Partition),
  ( string(Payload) ; atom(Payload) ),
  pl_kafka_produce(Topic, Partition, Payload, '').

% kafka_produce(+Topic, +Integer, +String, +String)
kafka_produce(Topic, Partition, Payload, Key) :-
  nonvar(Topic), integer(Partition),
  ( string(Payload) ; atom(Payload) ),
  ( string(Key) ; atom(Key) ),
  pl_kafka_produce(Topic, Partition, Payload, Key).

% kafka_produce_batch(+Topic, +Integer, +List)
kafka_produce_batch(Topic, Partition, PayloadList) :-
  nonvar(Topic), integer(Partition),
  nonvar(PayloadList), length(PayloadList, LL), LL > 0,
  pl_kafka_produce_batch(Topic, Partition, LL, PayloadList).

% kafka_consume_batch(+Topic, +Integer, +Integer, +List)
kafka_consume_batch(Topic, Partition, Timeout, MessageList) :-
  nonvar(Topic), integer(Partition), integer(Timeout),
  var(MessageList),
  pl_kafka_consume_batch(Topic, Partition, Timeout, MessageList).

% kafka_consume_start(+Topic, +Integer, +Integer)
kafka_consume_start(Topic, Partition, Offset) :-
  nonvar(Topic), integer(Partition), integer(Offset),
  pl_kafka_consume_start(Topic, Partition, Offset).

% kafka_consume_stop(+Topic, +Integer)
kafka_consume_stop(Topic, Partition) :-
  nonvar(Topic), integer(Partition),
  pl_kafka_consume_stop(Topic, Partition).

% kafka_flush(+Client, +Integer)
kafka_flush(Client, Timeout) :-
  nonvar(Client), integer(Timeout),
  pl_kafka_flush(Client, Timeout).

% kafka_consumer_poll(+Client, +Integer, -String, -List)
kafka_consumer_poll(Client, Timeout, Msg, Meta) :-
  nonvar(Client), integer(Timeout),
  var(Msg), var(Meta),
  pl_kafka_consumer_poll(Client, Timeout, Msg, Meta).

% kafka_subscribe(+Client, +Integer, +Integer, +List)
kafka_subscribe(Client, Lo, Hi, Topics) :-
  nonvar(Client), nonvar(Topics),
  integer(Lo), integer(Hi), Lo =< Hi,
  length(Topics, Len), Len > 0,
  pl_kafka_subscribe(Client, Lo, Hi, Len, Topics).

% kafka_unsubscribe(+Client)
kafka_unsubscribe(Client) :-
  nonvar(Client),
  pl_kafka_unsubscribe(Client).

% kafka_consumer_close(+Client)
kafka_consumer_close(Client) :-
  nonvar(Client),
  pl_kafka_consumer_close(Client).
