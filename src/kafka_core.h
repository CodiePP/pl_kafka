/*   Prolog Interface to Kafka
 *   Copyright (C) 2021-2026  Alexander Diemand
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef PL_KAFKA_CORE_H
#define PL_KAFKA_CORE_H

/* the actual librdkafka work, shared between the GNU Prolog and SWI-Prolog
 * bridges. this file has no dependency on either Prolog C interface. */

#include <stdint.h>
#include <stddef.h>
#include <librdkafka/rdkafka.h>

int pl_kafka_conf_set(rd_kafka_conf_t *conf, const char *key, const char *val,
                       char *errstr, size_t errstr_size);

int pl_kafka_topic_conf_set(rd_kafka_topic_conf_t *conf, const char *key, const char *val,
                             char *errstr, size_t errstr_size);

rd_kafka_t *pl_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf,
                          char *errstr, size_t errstr_size);

rd_kafka_topic_t *pl_kafka_topic_new(rd_kafka_t *rk, const char *name,
                                      rd_kafka_topic_conf_t *conf);

/* partition < 0 is normalized to RD_KAFKA_PARTITION_UA */
int pl_kafka_produce(rd_kafka_topic_t *rkt, int32_t partition,
                      const void *payload, size_t payload_len,
                      const void *key, size_t key_len);

/* returns the number of messages actually produced, as rd_kafka_produce_batch does */
int pl_kafka_produce_batch(rd_kafka_topic_t *rkt, int32_t partition,
                            rd_kafka_message_t *msgs, int msg_count);

rd_kafka_resp_err_t pl_kafka_flush(rd_kafka_t *rk, int timeout_ms);

/* takes ownership of topics: destroys the list after subscribing */
rd_kafka_resp_err_t pl_kafka_subscribe(rd_kafka_t *rk,
                                        rd_kafka_topic_partition_list_t *topics);

rd_kafka_resp_err_t pl_kafka_unsubscribe(rd_kafka_t *rk);

rd_kafka_message_t *pl_kafka_consumer_poll(rd_kafka_t *rk, int timeout_ms);

/* returns the number of messages consumed, as rd_kafka_consume_batch does */
int pl_kafka_consume_batch(rd_kafka_topic_t *rkt, int32_t partition, int timeout_ms,
                            rd_kafka_message_t **msgs, size_t max_msgs);

int pl_kafka_consume_start(rd_kafka_topic_t *rkt, int32_t partition, int64_t offset);

int pl_kafka_consume_stop(rd_kafka_topic_t *rkt, int32_t partition);

/* Prolog-neutral view of a consumed message, valid only until the caller
 * destroys the underlying rd_kafka_message_t. */
typedef struct {
  const void *payload;
  size_t payload_len;

  const void *key;
  size_t key_len;

  int32_t partition;
  int64_t offset;
} pl_kafka_message;

/* returns 0 on success; nonzero if msg is NULL or msg->err != 0 */
int pl_kafka_message_read(const rd_kafka_message_t *msg, pl_kafka_message *out);

#endif /* PL_KAFKA_CORE_H */
