/*   Prolog Interface to Kafka -- GNU Prolog bridge
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "gprolog.h"
#include "kafka_core.h"

/* opaque rd_kafka_* handles are passed between Prolog calls as plain
 * integers holding the pointer value, mirroring PL_unify_pointer/
 * PL_get_pointer on the SWI side. */
static PlTerm mk_pointer(const void *p)
{
  return Pl_Mk_Integer((PlLong)(intptr_t)p);
}

static void *rd_pointer_check(PlTerm t)
{
  return (void *)(intptr_t)Pl_Rd_Integer_Check(t);
}

/* payloads/keys are handed to us as (pointer,len) pairs that are not
 * necessarily NUL-terminated; GNU Prolog's Mk_String/Un_String only take
 * a C string, so copy through a NUL-terminated scratch buffer. */
static PlTerm mk_string_n(const void *data, size_t len)
{
  char *buf = (char *)malloc(len + 1);
  PlTerm t;
  if (!buf) { return Pl_Mk_String(""); }
  if (data && len > 0) { memcpy(buf, data, len); }
  buf[len] = '\0';
  t = Pl_Mk_String(buf);
  free(buf);
  return t;
}

static PlBool un_string_n(const void *data, size_t len, PlTerm term)
{
  char *buf = (char *)malloc(len + 1);
  PlBool ok;
  if (!buf) { return PL_FALSE; }
  if (data && len > 0) { memcpy(buf, data, len); }
  buf[len] = '\0';
  ok = Pl_Un_String_Check(buf, term);
  free(buf);
  return ok;
}

PlBool gp_kafka_version(PlTerm out_v)
{
  return Pl_Un_String_Check(rd_kafka_version_str(), out_v);
}

PlBool gp_kafka_conf_new(PlTerm out_cid)
{
  if (Pl_Builtin_Non_Var(out_cid)) { return PL_FALSE; }
  rd_kafka_conf_t *kc = rd_kafka_conf_new();
  return Pl_Un_Integer_Check((PlLong)(intptr_t)kc, out_cid);
}

PlBool gp_kafka_topic_conf_new(PlTerm out_cid)
{
  if (Pl_Builtin_Non_Var(out_cid)) { return PL_FALSE; }
  rd_kafka_topic_conf_t *kc = rd_kafka_topic_conf_new();
  return Pl_Un_Integer_Check((PlLong)(intptr_t)kc, out_cid);
}

PlBool gp_kafka_conf_destroy(PlTerm in_cid)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_conf_destroy((rd_kafka_conf_t *)rd_pointer_check(in_cid));
  return PL_TRUE;
}

PlBool gp_kafka_topic_conf_destroy(PlTerm in_cid)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_topic_conf_destroy((rd_kafka_topic_conf_t *)rd_pointer_check(in_cid));
  return PL_TRUE;
}

PlBool gp_kafka_conf_set(PlTerm in_cid, PlTerm in_k, PlTerm in_v)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_conf_t *kc = (rd_kafka_conf_t *)rd_pointer_check(in_cid);

  char *k_key = Pl_Rd_String_Check(in_k);
  char *k_val = Pl_Rd_String_Check(in_v);

  char errstr[128];
  if (pl_kafka_conf_set(kc, k_key, k_val, errstr, 127) != 0) { return PL_FALSE; }
  return PL_TRUE;
}

PlBool gp_kafka_topic_conf_set(PlTerm in_cid, PlTerm in_k, PlTerm in_v)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_topic_conf_t *kc = (rd_kafka_topic_conf_t *)rd_pointer_check(in_cid);

  char *k_key = Pl_Rd_String_Check(in_k);
  char *k_val = Pl_Rd_String_Check(in_v);

  char errstr[128];
  if (pl_kafka_topic_conf_set(kc, k_key, k_val, errstr, 127) != 0) { return PL_FALSE; }
  return PL_TRUE;
}

static PlBool gp_kafka_new_shared(const char *name, rd_kafka_type_t type, PlTerm in_cid, PlTerm out_client)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_conf_t *kc = (rd_kafka_conf_t *)rd_pointer_check(in_cid);

  if (Pl_Builtin_Non_Var(out_client)) { return PL_FALSE; }
  char errstr[128];
  rd_kafka_t *k = pl_kafka_new(type, kc, errstr, 127);
  if (!k) {
    printf("ERROR - kafka new %s: %s\n", name, errstr);
    return PL_FALSE;
  }
  return Pl_Un_Integer_Check((PlLong)(intptr_t)k, out_client);
}

PlBool gp_kafka_consumer_new(PlTerm in_cid, PlTerm out_client)
{
  return gp_kafka_new_shared("consumer", RD_KAFKA_CONSUMER, in_cid, out_client);
}

PlBool gp_kafka_producer_new(PlTerm in_cid, PlTerm out_client)
{
  return gp_kafka_new_shared("producer", RD_KAFKA_PRODUCER, in_cid, out_client);
}

PlBool gp_kafka_destroy(PlTerm in_client)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_destroy((rd_kafka_t *)rd_pointer_check(in_client));
  return PL_TRUE;
}

PlBool gp_kafka_conf_dump(PlTerm in_cid, PlTerm out_list)
{
  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_conf_t *kc = (rd_kafka_conf_t *)rd_pointer_check(in_cid);

  if (Pl_Builtin_Non_Var(out_list)) { return PL_FALSE; }

  size_t num_pairs = 0;
  const char **pairs = rd_kafka_conf_dump(kc, &num_pairs);
  size_t n = num_pairs / 2;

  PlTerm elems[n ? n : 1];
  size_t i;
  for (i = 0; i < n; i++) {
    PlTerm arg[1];
    arg[0] = Pl_Mk_String(pairs[i * 2 + 1]);
    elems[i] = Pl_Mk_Compound(Pl_Create_Atom(pairs[i * 2]), 1, arg);
  }

  rd_kafka_conf_dump_free(pairs, num_pairs);
  return Pl_Un_Proper_List_Check((int)n, elems, out_list);
}

PlBool gp_kafka_topic_new(PlTerm in_client, PlTerm in_name, PlTerm in_cid, PlTerm out_topic)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  if (Pl_Builtin_Var(in_cid)) { return PL_FALSE; }
  rd_kafka_topic_conf_t *kc = (rd_kafka_topic_conf_t *)rd_pointer_check(in_cid);

  if (Pl_Builtin_Var(in_name)) { return PL_FALSE; }
  char *k_name = Pl_Rd_String_Check(in_name);

  if (Pl_Builtin_Non_Var(out_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *t = pl_kafka_topic_new(rk, k_name, kc);
  if (!t) {
    printf("ERROR - kafka new topic failed with code: %d\n", errno);
    return PL_FALSE;
  }
  return Pl_Un_Integer_Check((PlLong)(intptr_t)t, out_topic);
}

PlBool gp_kafka_topic_destroy(PlTerm in_topic)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_destroy((rd_kafka_topic_t *)rd_pointer_check(in_topic));
  return PL_TRUE;
}

PlBool gp_kafka_produce(PlTerm in_topic, PlTerm in_partition, PlTerm in_payload, PlTerm in_key)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *rkt = (rd_kafka_topic_t *)rd_pointer_check(in_topic);

  int32_t partition = (int32_t)Pl_Rd_Integer_Check(in_partition);

  if (Pl_Builtin_Var(in_payload)) { return PL_FALSE; }
  char *k_payload = Pl_Rd_String_Check(in_payload);
  size_t n_payload = k_payload ? strlen(k_payload) : 0;

  if (Pl_Builtin_Var(in_key)) { return PL_FALSE; }
  char *k_key = Pl_Rd_String_Check(in_key);
  size_t n_key = 0;
  if (k_key && k_key[0] == '\0') { k_key = NULL; }
  if (k_key) { n_key = strlen(k_key); }

  int res = pl_kafka_produce(rkt, partition, k_payload, n_payload, k_key, n_key);
  return (res == 0) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_produce_batch(PlTerm in_topic, PlTerm in_partition, PlTerm in_len, PlTerm in_list)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *rkt = (rd_kafka_topic_t *)rd_pointer_check(in_topic);

  int32_t partition = (int32_t)Pl_Rd_Integer_Check(in_partition);

  int32_t llen = (int32_t)Pl_Rd_Integer_Check(in_len);
  if (llen <= 0) { return PL_FALSE; }

  if (Pl_Builtin_Var(in_list)) { return PL_FALSE; }

  rd_kafka_message_t msgs[llen];
  int cnt = 0;
  PlTerm ls = in_list;
  PlTerm *cons;
  while ((cons = Pl_Rd_List_Check(ls)) != NULL) {
    char *k_payload = Pl_Rd_String_Check(cons[0]);
    size_t n_payload = k_payload ? strlen(k_payload) : 0;
    msgs[cnt].payload = k_payload;
    msgs[cnt].len = n_payload;
    msgs[cnt].key_len = 0; msgs[cnt].key = NULL;
    msgs[cnt]._private = NULL;
    msgs[cnt].err = 0;
    cnt++;
    ls = cons[1];
  }

  int res = pl_kafka_produce_batch(rkt, partition, msgs, cnt);
  if (res != cnt) {
    printf("kafka_produce_batch produced: %d\n", res);
    return PL_FALSE;
  }
  return PL_TRUE;
}

PlBool gp_kafka_consume_batch(PlTerm in_topic, PlTerm in_partition, PlTerm in_timeout, PlTerm out_list)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *rkt = (rd_kafka_topic_t *)rd_pointer_check(in_topic);

  int32_t partition = (int32_t)Pl_Rd_Integer_Check(in_partition);

  int32_t timeout = (int32_t)Pl_Rd_Integer_Check(in_timeout);
  if (timeout <= 0) { return PL_FALSE; }

  if (Pl_Builtin_Non_Var(out_list)) { return PL_FALSE; }

  int sz = 100;
  rd_kafka_message_t *msgs[sz];

  int res = pl_kafka_consume_batch(rkt, partition, timeout, msgs, sz);
  if (res <= 0) { return PL_FALSE; }

  PlTerm elems[res];
  int idx = 0;
  int n = 0;
  while (idx < res) {
    if (msgs[idx]->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) { break; }
    if (msgs[idx]->err == 0) {
      pl_kafka_message nm;
      pl_kafka_message_read(msgs[idx], &nm);
      elems[n++] = mk_string_n(nm.payload, nm.payload_len);
    }
    rd_kafka_message_destroy(msgs[idx]);
    idx++;
  }

  return Pl_Un_Proper_List_Check(n, elems, out_list);
}

PlBool gp_kafka_consume_start(PlTerm in_topic, PlTerm in_partition, PlTerm in_offset)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *rkt = (rd_kafka_topic_t *)rd_pointer_check(in_topic);

  int32_t partition = (int32_t)Pl_Rd_Integer_Check(in_partition);
  int64_t offset = (int64_t)Pl_Rd_Integer_Check(in_offset);

  return (pl_kafka_consume_start(rkt, partition, offset) == 0) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_consume_stop(PlTerm in_topic, PlTerm in_partition)
{
  if (Pl_Builtin_Var(in_topic)) { return PL_FALSE; }
  rd_kafka_topic_t *rkt = (rd_kafka_topic_t *)rd_pointer_check(in_topic);

  int32_t partition = (int32_t)Pl_Rd_Integer_Check(in_partition);

  return (pl_kafka_consume_stop(rkt, partition) == 0) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_flush(PlTerm in_client, PlTerm in_timeout)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  if (Pl_Builtin_Var(in_timeout)) { return PL_FALSE; }
  int32_t timeout = (int32_t)Pl_Rd_Integer_Check(in_timeout);

  rd_kafka_resp_err_t res = pl_kafka_flush(rk, timeout);
  return (res == RD_KAFKA_RESP_ERR_NO_ERROR) ? PL_TRUE : PL_FALSE;
}

static PlBool gp_unify_kafka_message(const pl_kafka_message *msg, PlTerm out_msg, PlTerm out_meta)
{
  if (Pl_Builtin_Non_Var(out_msg)) { return PL_FALSE; }
  if (Pl_Builtin_Non_Var(out_meta)) { return PL_FALSE; }

  if (!un_string_n(msg->payload, msg->payload_len, out_msg)) { return PL_FALSE; }

  PlTerm meta[3];
  int n = 0;
  PlTerm arg[1];

  arg[0] = Pl_Mk_Integer((PlLong)msg->partition);
  meta[n++] = Pl_Mk_Compound(Pl_Create_Atom("partition"), 1, arg);

  arg[0] = Pl_Mk_Integer((PlLong)msg->offset);
  meta[n++] = Pl_Mk_Compound(Pl_Create_Atom("offset"), 1, arg);

  if (msg->key_len > 0 && msg->key) {
    arg[0] = mk_string_n(msg->key, msg->key_len);
    meta[n++] = Pl_Mk_Compound(Pl_Create_Atom("key"), 1, arg);
  }

  return Pl_Un_Proper_List_Check(n, meta, out_meta);
}

PlBool gp_kafka_consumer_poll(PlTerm in_client, PlTerm in_timeout, PlTerm out_message, PlTerm out_meta)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  if (Pl_Builtin_Var(in_timeout)) { return PL_FALSE; }
  int32_t timeout = (int32_t)Pl_Rd_Integer_Check(in_timeout);

  if (Pl_Builtin_Non_Var(out_message)) { return PL_FALSE; }
  if (Pl_Builtin_Non_Var(out_meta)) { return PL_FALSE; }

  rd_kafka_message_t *msg = pl_kafka_consumer_poll(rk, timeout);
  if (!msg) { return PL_FALSE; }

  if (msg->err != 0) {
    printf("ERROR: polling returned: %s\n", (char *)msg->payload);
    rd_kafka_message_destroy(msg);
    return PL_FALSE;
  }

  pl_kafka_message nm;
  pl_kafka_message_read(msg, &nm);

  PlBool ok = gp_unify_kafka_message(&nm, out_message, out_meta);
  rd_kafka_message_destroy(msg);

  if (!ok) {
    printf("ERROR: poll message unification failed\n");
    return PL_FALSE;
  }
  return PL_TRUE;
}

PlBool gp_kafka_subscribe3(PlTerm in_client, PlTerm in_len, PlTerm in_topics)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  if (Pl_Builtin_Var(in_len)) { return PL_FALSE; }
  int32_t len = (int32_t)Pl_Rd_Integer_Check(in_len);

  if (Pl_Builtin_Var(in_topics)) { return PL_FALSE; }

  rd_kafka_topic_partition_list_t *ktl = rd_kafka_topic_partition_list_new(len);
  int cnt = 0;
  PlTerm ls = in_topics;
  PlTerm *cons;
  while ((cons = Pl_Rd_List_Check(ls)) != NULL) {
    cnt++;
    if (cnt > len) { break; }
    char *k_topic = Pl_Rd_String_Check(cons[0]);
    rd_kafka_topic_partition_list_add(ktl, k_topic, -1);
    ls = cons[1];
  }
  rd_kafka_resp_err_t res = pl_kafka_subscribe(rk, ktl);
  return (res == RD_KAFKA_RESP_ERR_NO_ERROR) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_subscribe5(PlTerm in_client, PlTerm in_lo, PlTerm in_hi, PlTerm in_len, PlTerm in_topics)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  if (Pl_Builtin_Var(in_lo)) { return PL_FALSE; }
  int32_t lo = (int32_t)Pl_Rd_Integer_Check(in_lo);

  if (Pl_Builtin_Var(in_hi)) { return PL_FALSE; }
  int32_t hi = (int32_t)Pl_Rd_Integer_Check(in_hi);

  if (Pl_Builtin_Var(in_len)) { return PL_FALSE; }
  int32_t len = (int32_t)Pl_Rd_Integer_Check(in_len);

  if (Pl_Builtin_Var(in_topics)) { return PL_FALSE; }

  rd_kafka_topic_partition_list_t *ktl = rd_kafka_topic_partition_list_new(len);
  int cnt = 0;
  PlTerm ls = in_topics;
  PlTerm *cons;
  while ((cons = Pl_Rd_List_Check(ls)) != NULL) {
    cnt++;
    if (cnt > len) { break; }
    char *k_topic = Pl_Rd_String_Check(cons[0]);
    rd_kafka_topic_partition_list_add_range(ktl, k_topic, lo, hi);
    ls = cons[1];
  }
  rd_kafka_resp_err_t res = pl_kafka_subscribe(rk, ktl);
  return (res == RD_KAFKA_RESP_ERR_NO_ERROR) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_unsubscribe(PlTerm in_client)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  rd_kafka_resp_err_t res = pl_kafka_unsubscribe(rk);
  return (res == RD_KAFKA_RESP_ERR_NO_ERROR) ? PL_TRUE : PL_FALSE;
}

PlBool gp_kafka_consumer_close(PlTerm in_client)
{
  if (Pl_Builtin_Var(in_client)) { return PL_FALSE; }
  rd_kafka_t *rk = (rd_kafka_t *)rd_pointer_check(in_client);

  rd_kafka_resp_err_t res = rd_kafka_consumer_close(rk);
  return (res == RD_KAFKA_RESP_ERR_NO_ERROR) ? PL_TRUE : PL_FALSE;
}
