/*-------------------------------------------------------------------------*/
/* Prolog Interface to Kafka                                               */
/*                                                                         */
/* File  : kafka.pl                                                        */
/* Descr.:                                                                 */
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
                 , kafka_conf_dump/2
                 , kafka_produce/2
                 , kafka_produce/3
                 , kafka_produce/4
                 , kafka_flush/2
                 %, pl_kafka_consumer_close/1
                 %, pl_kafka_destroy/1
                 %, pl_kafka_topic_new/2
                 %, pl_kafka_topic_destroy/1
                 %, pl_kafka_consume_start/4
                 %, pl_kafka_consume/2
                 %, pl_kafka_consume_batch/2
                 %, pl_kafka_consume_callback/2
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

% kafka_flush(+Client, +Integer)
kafka_flush(Client, Timeout) :-
  nonvar(Client), integer(Timeout),
  pl_kafka_flush(Client, Timeout).
