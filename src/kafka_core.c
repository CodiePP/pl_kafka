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

#include "kafka_core.h"

static int32_t normalize_partition(int32_t partition)
{
  return (partition < 0) ? RD_KAFKA_PARTITION_UA : partition;
}

int pl_kafka_conf_set(rd_kafka_conf_t *conf, const char *key, const char *val,
                       char *errstr, size_t errstr_size)
{
  return (rd_kafka_conf_set(conf, key, val, errstr, errstr_size) == RD_KAFKA_CONF_OK) ? 0 : -1;
}

int pl_kafka_topic_conf_set(rd_kafka_topic_conf_t *conf, const char *key, const char *val,
                             char *errstr, size_t errstr_size)
{
  return (rd_kafka_topic_conf_set(conf, key, val, errstr, errstr_size) == RD_KAFKA_CONF_OK) ? 0 : -1;
}

rd_kafka_t *pl_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf,
                          char *errstr, size_t errstr_size)
{
  return rd_kafka_new(type, conf, errstr, errstr_size);
}

rd_kafka_topic_t *pl_kafka_topic_new(rd_kafka_t *rk, const char *name,
                                      rd_kafka_topic_conf_t *conf)
{
  return rd_kafka_topic_new(rk, name, conf);
}

int pl_kafka_produce(rd_kafka_topic_t *rkt, int32_t partition,
                      const void *payload, size_t payload_len,
                      const void *key, size_t key_len)
{
  return rd_kafka_produce(rkt, normalize_partition(partition),
                           RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
                           (void *)payload, payload_len,
                           key, key_len,
                           NULL);
}

int pl_kafka_produce_batch(rd_kafka_topic_t *rkt, int32_t partition,
                            rd_kafka_message_t *msgs, int msg_count)
{
  return rd_kafka_produce_batch(rkt, normalize_partition(partition),
                                 RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
                                 msgs, msg_count);
}

rd_kafka_resp_err_t pl_kafka_flush(rd_kafka_t *rk, int timeout_ms)
{
  return rd_kafka_flush(rk, timeout_ms);
}

rd_kafka_resp_err_t pl_kafka_subscribe(rd_kafka_t *rk,
                                        rd_kafka_topic_partition_list_t *topics)
{
  rd_kafka_resp_err_t res = rd_kafka_subscribe(rk, topics);
  rd_kafka_topic_partition_list_destroy(topics);
  return res;
}

rd_kafka_resp_err_t pl_kafka_unsubscribe(rd_kafka_t *rk)
{
  return rd_kafka_unsubscribe(rk);
}

rd_kafka_message_t *pl_kafka_consumer_poll(rd_kafka_t *rk, int timeout_ms)
{
  return rd_kafka_consumer_poll(rk, timeout_ms);
}

int pl_kafka_consume_batch(rd_kafka_topic_t *rkt, int32_t partition, int timeout_ms,
                            rd_kafka_message_t **msgs, size_t max_msgs)
{
  return rd_kafka_consume_batch(rkt, normalize_partition(partition), timeout_ms, msgs, max_msgs);
}

int pl_kafka_consume_start(rd_kafka_topic_t *rkt, int32_t partition, int64_t offset)
{
  return rd_kafka_consume_start(rkt, normalize_partition(partition), offset);
}

int pl_kafka_consume_stop(rd_kafka_topic_t *rkt, int32_t partition)
{
  return rd_kafka_consume_stop(rkt, normalize_partition(partition));
}

int pl_kafka_message_read(const rd_kafka_message_t *msg, pl_kafka_message *out)
{
  if (!msg || msg->err != 0) {
    return -1;
  }
  out->payload = msg->payload;
  out->payload_len = msg->len;
  out->key = msg->key;
  out->key_len = msg->key_len;
  out->partition = msg->partition;
  out->offset = msg->offset;
  return 0;
}
