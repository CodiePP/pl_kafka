/*-------------------------------------------------------------------------*/
/* Prolog Interface to Kafka -- GNU Prolog bridge                          */
/*                                                                         */
/* File  : gp-kafka.pl                                                     */
/* Author: Alexander Diemand                                               */
/*                                                                         */
/* Copyright (C) 2021-2026 Alexander Diemand                               */
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

/* the C functions are named gp_kafka_* (see gp-kafka-c.c); fct_name/1 maps
 * each one onto the pl_kafka_* predicate name that the common kafka.pl
 * (shared with the SWI backend) actually calls. */

:- foreign(pl_kafka_version(term), [fct_name(gp_kafka_version)]).
:- foreign(pl_kafka_conf_new(term), [fct_name(gp_kafka_conf_new)]).
:- foreign(pl_kafka_topic_conf_new(term), [fct_name(gp_kafka_topic_conf_new)]).
:- foreign(pl_kafka_conf_destroy(term), [fct_name(gp_kafka_conf_destroy)]).
:- foreign(pl_kafka_topic_conf_destroy(term), [fct_name(gp_kafka_topic_conf_destroy)]).
:- foreign(pl_kafka_conf_set(+term, +term, +term), [fct_name(gp_kafka_conf_set)]).
:- foreign(pl_kafka_topic_conf_set(+term, +term, +term), [fct_name(gp_kafka_topic_conf_set)]).
:- foreign(pl_kafka_consumer_new(+term, term), [fct_name(gp_kafka_consumer_new)]).
:- foreign(pl_kafka_producer_new(+term, term), [fct_name(gp_kafka_producer_new)]).
:- foreign(pl_kafka_destroy(+term), [fct_name(gp_kafka_destroy)]).
:- foreign(pl_kafka_conf_dump(+term, term), [fct_name(gp_kafka_conf_dump)]).
:- foreign(pl_kafka_topic_new(+term, +term, +term, term), [fct_name(gp_kafka_topic_new)]).
:- foreign(pl_kafka_topic_destroy(+term), [fct_name(gp_kafka_topic_destroy)]).
:- foreign(pl_kafka_produce(+term, +term, +term, +term), [fct_name(gp_kafka_produce)]).
:- foreign(pl_kafka_produce_batch(+term, +term, +term, +term), [fct_name(gp_kafka_produce_batch)]).
:- foreign(pl_kafka_consume_batch(+term, +term, +term, term), [fct_name(gp_kafka_consume_batch)]).
:- foreign(pl_kafka_consume_start(+term, +term, +term), [fct_name(gp_kafka_consume_start)]).
:- foreign(pl_kafka_consume_stop(+term, +term), [fct_name(gp_kafka_consume_stop)]).
:- foreign(pl_kafka_flush(+term, +term), [fct_name(gp_kafka_flush)]).
:- foreign(pl_kafka_consumer_poll(+term, +term, term, term), [fct_name(gp_kafka_consumer_poll)]).
:- foreign(pl_kafka_subscribe(+term, +term, +term), [fct_name(gp_kafka_subscribe3)]).
:- foreign(pl_kafka_subscribe(+term, +term, +term, +term, +term), [fct_name(gp_kafka_subscribe5)]).
:- foreign(pl_kafka_unsubscribe(+term), [fct_name(gp_kafka_unsubscribe)]).
:- foreign(pl_kafka_consumer_close(+term), [fct_name(gp_kafka_consumer_close)]).

/* kafka.pl (shared with the SWI backend) is compiled and linked as a
 * sibling object -- see GP_SRCS/GP_OBJS in Makefile.in -- so its
 * kafka_* guard predicates resolve at link time, not via a runtime
 * consult here. */
