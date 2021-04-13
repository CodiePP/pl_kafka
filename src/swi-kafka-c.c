/*   SWI-Prolog Interface to Kafka
 *   Copyright (C) 2021  Alexander Diemand
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
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include <SWI-Prolog.h>
#include <librdkafka/rdkafka.h>

/* declarations */
foreign_t swi_kafka_version(atom_t v);
foreign_t swi_kafka_conf_new(term_t cid);
foreign_t swi_kafka_topic_conf_new(term_t cid);
foreign_t swi_kafka_conf_destroy(term_t cid);
foreign_t swi_kafka_topic_conf_destroy(term_t cid);
foreign_t swi_kafka_conf_set(term_t cid, term_t k, term_t v);
foreign_t swi_kafka_topic_conf_set(term_t cid, term_t k, term_t v);
foreign_t swi_kafka_consumer_new(term_t cid, term_t consumer);
foreign_t swi_kafka_producer_new(term_t cid, term_t producer);
foreign_t swi_kafka_destroy(term_t client);
foreign_t swi_kafka_conf_dump(term_t cid, term_t list);
foreign_t swi_kafka_topic_new(term_t cid, term_t producer, atom_t name, term_t topic);
foreign_t swi_kafka_topic_destroy(term_t topic);
foreign_t swi_kafka_produce(term_t topic, term_t partition, term_t payload, atom_t key);
foreign_t swi_kafka_produce_batch(term_t topic, term_t partition, term_t len, term_t list);
foreign_t swi_kafka_consume_batch(term_t topic, term_t partition, term_t timeout, term_t list);
foreign_t swi_kafka_consume_start(term_t topic, term_t partition, term_t offset);
foreign_t swi_kafka_consume_stop(term_t topic, term_t partition);
foreign_t swi_kafka_flush(term_t client, term_t timeout);
foreign_t swi_kafka_consumer_poll(term_t client, term_t timeout, term_t message, term_t meta);
foreign_t swi_kafka_subscribe3(term_t client, term_t len, term_t topics);
foreign_t swi_kafka_subscribe5(term_t client, term_t lo, term_t hi, term_t len, term_t topics);
foreign_t swi_kafka_unsubscribe(term_t client);
foreign_t swi_kafka_consumer_close(term_t client);


/* install predicates */
install_t install()
{
  PL_register_foreign("pl_kafka_version", 1, swi_kafka_version, 0);
  PL_register_foreign("pl_kafka_conf_new", 1, swi_kafka_conf_new, 0);
  PL_register_foreign("pl_kafka_topic_conf_new", 1, swi_kafka_topic_conf_new, 0);
  PL_register_foreign("pl_kafka_conf_destroy", 1, swi_kafka_conf_destroy, 0);
  PL_register_foreign("pl_kafka_topic_conf_destroy", 1, swi_kafka_topic_conf_destroy, 0);
  PL_register_foreign("pl_kafka_conf_set", 3, swi_kafka_conf_set, 0);
  PL_register_foreign("pl_kafka_topic_conf_set", 3, swi_kafka_topic_conf_set, 0);
  PL_register_foreign("pl_kafka_consumer_new", 2, swi_kafka_consumer_new, 0);
  PL_register_foreign("pl_kafka_producer_new", 2, swi_kafka_producer_new, 0);
  PL_register_foreign("pl_kafka_destroy", 1, swi_kafka_destroy, 0);
  PL_register_foreign("pl_kafka_conf_dump", 2, swi_kafka_conf_dump, 0);
  PL_register_foreign("pl_kafka_topic_new", 4, swi_kafka_topic_new, 0);
  PL_register_foreign("pl_kafka_topic_destroy", 1, swi_kafka_topic_destroy, 0);
  PL_register_foreign("pl_kafka_produce", 4, swi_kafka_produce, 0);
  PL_register_foreign("pl_kafka_produce_batch", 4, swi_kafka_produce_batch, 0);
  PL_register_foreign("pl_kafka_consume_batch", 4, swi_kafka_consume_batch, 0);
  PL_register_foreign("pl_kafka_flush", 2, swi_kafka_flush, 0);
  PL_register_foreign("pl_kafka_consumer_poll", 4, swi_kafka_consumer_poll, 0);
  PL_register_foreign("pl_kafka_subscribe", 3, swi_kafka_subscribe3, 0);
  PL_register_foreign("pl_kafka_subscribe", 5, swi_kafka_subscribe5, 0);
  PL_register_foreign("pl_kafka_unsubscribe", 1, swi_kafka_unsubscribe, 0);
  PL_register_foreign("pl_kafka_consumer_close", 1, swi_kafka_consumer_close, 0);
  PL_register_foreign("pl_kafka_consume_start", 3, swi_kafka_consume_start, 0);
  PL_register_foreign("pl_kafka_consume_stop", 2, swi_kafka_consume_stop, 0);
}


/* definitions */

foreign_t swi_kafka_version(atom_t kafka_version)
{
  const char *kv = rd_kafka_version_str();
  atom_t a_kv = PL_new_atom(kv);
  return PL_unify_atom(kafka_version, a_kv);
}

foreign_t swi_kafka_conf_dump(term_t in_cid, term_t out_list)
{
  if (PL_is_variable(in_cid)) { PL_fail; }
  rd_kafka_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }

  if (!PL_is_variable(out_list)) { PL_fail; }
  term_t ele = PL_new_term_ref();
  term_t lst = PL_copy_term_ref(out_list);

  size_t num_pairs = 0;
  const char **pairs = rd_kafka_conf_dump(kc, &num_pairs);

  functor_t fct;
  for (int i = 0; i < num_pairs; i+=2)
  {
    fct = PL_new_functor(PL_new_atom(pairs[i]), 1);
    if (!PL_unify_list(lst, ele, lst)) { PL_fail; }
    if (!PL_unify_term(ele, PL_FUNCTOR, fct,
                            PL_STRING,  pairs[i+1])) { PL_fail; }
  }

  rd_kafka_conf_dump_free(pairs, num_pairs);
  return PL_unify_nil(lst);
}

foreign_t swi_kafka_conf_new(term_t out_cid)
{
  if (!PL_is_variable(out_cid)) { PL_fail; }

  rd_kafka_conf_t *kc = rd_kafka_conf_new();
  return PL_unify_pointer(out_cid, kc);
}

foreign_t swi_kafka_topic_conf_new(term_t out_cid)
{
  if (!PL_is_variable(out_cid)) { PL_fail; }

  rd_kafka_topic_conf_t *kc = rd_kafka_topic_conf_new();
  return PL_unify_pointer(out_cid, kc);
}

foreign_t swi_kafka_conf_destroy(term_t in_cid)
{
  if (PL_is_variable(in_cid)) { PL_fail; }

  rd_kafka_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }
  rd_kafka_conf_destroy(kc);
  PL_succeed;
}

foreign_t swi_kafka_topic_conf_destroy(term_t in_cid)
{
  if (PL_is_variable(in_cid)) { PL_fail; }

  rd_kafka_topic_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }
  rd_kafka_topic_conf_destroy(kc);
  PL_succeed;
}

foreign_t swi_kafka_conf_set(term_t in_cid, term_t in_k, term_t in_v)
{
  if (PL_is_variable(in_cid)) { PL_fail; }
  rd_kafka_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }

  char *k_key, *k_val;
  if (!PL_get_chars(in_k, &k_key, CVT_ATOM|CVT_STRING)) { PL_fail; }
  if (!PL_get_chars(in_v, &k_val, CVT_ATOM|CVT_STRING)) { PL_fail; }

  char errstr[128];
  rd_kafka_conf_res_t res = rd_kafka_conf_set(kc, k_key, k_val, errstr, 127);
  if (res != RD_KAFKA_CONF_OK) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_topic_conf_set(term_t in_cid, term_t in_k, term_t in_v)
{
  if (PL_is_variable(in_cid)) { PL_fail; }
  rd_kafka_topic_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }

  char *k_key, *k_val;
  if (!PL_get_chars(in_k, &k_key, CVT_ATOM|CVT_STRING)) { PL_fail; }
  if (!PL_get_chars(in_v, &k_val, CVT_ATOM|CVT_STRING)) { PL_fail; }

  char errstr[128];
  rd_kafka_conf_res_t res = rd_kafka_topic_conf_set(kc, k_key, k_val, errstr, 127);
  if (res != RD_KAFKA_CONF_OK) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_new(const char *name, rd_kafka_type_t type, term_t in_cid, term_t out_client)
{
  if (PL_is_variable(in_cid)) { PL_fail; }
  rd_kafka_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }

  if (!PL_is_variable(out_client)) { PL_fail; }
  char errstr[128];
  rd_kafka_t *k = rd_kafka_new(type, kc, errstr, 127);
  if (!k)
  {
    printf("ERROR - kafka new %s: %s\n", name, errstr);
    PL_fail;
  }
  return PL_unify_pointer(out_client, k);
}

foreign_t swi_kafka_consumer_new(term_t in_cid, term_t out_client)
{
  return swi_kafka_new("consumer", RD_KAFKA_CONSUMER, in_cid, out_client);
}
foreign_t swi_kafka_producer_new(term_t in_cid, term_t out_client)
{
  return swi_kafka_new("producer", RD_KAFKA_PRODUCER, in_cid, out_client);
}

foreign_t swi_kafka_destroy(term_t in_client)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  rd_kafka_destroy(rk);
  PL_succeed;
}

foreign_t swi_kafka_topic_new(term_t in_client, atom_t in_name, term_t in_cid, term_t out_topic)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  if (PL_is_variable(in_cid)) { PL_fail; }
  rd_kafka_topic_conf_t *kc;
  if (!PL_get_pointer(in_cid, (void**)&kc)) { PL_fail; }

  if (PL_is_variable(in_name)) { PL_fail; }
  char *k_name;
  if (!PL_get_chars(in_name, &k_name, CVT_ATOM|CVT_STRING)) { PL_fail; }

  if (!PL_is_variable(out_topic)) { PL_fail; }
  rd_kafka_topic_t *t = rd_kafka_topic_new(rk, k_name, kc);
  if (!t)
  {
    printf("ERROR - kafka new topic failed with code: %d\n", errno);
    PL_fail;
  }
  return PL_unify_pointer(out_topic, t);
}

foreign_t swi_kafka_topic_destroy(term_t in_topic)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }
  rd_kafka_topic_destroy(rkt);
  PL_succeed;
}

foreign_t swi_kafka_produce(term_t in_topic, term_t in_partition, term_t in_payload, atom_t in_key)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }

  int32_t partition;
  PL_get_integer(in_partition, &partition);
  if (partition < 0) { partition = RD_KAFKA_PARTITION_UA; }

  if (PL_is_variable(in_payload)) { PL_fail; }
  char *k_payload; int n_payload = 0;
  if (!PL_get_chars(in_payload, &k_payload, CVT_ATOM|CVT_STRING)) { PL_fail; }
  if (k_payload) { n_payload = strlen(k_payload); }

  if (PL_is_variable(in_key)) { PL_fail; }
  char *k_key; int n_key = 0;
  if (!PL_get_chars(in_key, &k_key, CVT_ATOM|CVT_STRING)) { PL_fail; }
  if (k_key && k_key[0] == '\0') { k_key = NULL; }
  if (k_key) { n_key = strlen(k_key); }

  int res = rd_kafka_produce(rkt, partition,
              RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
              k_payload, n_payload,
              k_key, n_key,
              NULL);
  if (res != 0) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_produce_batch(term_t in_topic, term_t in_partition, term_t in_len, term_t in_list)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }

  int32_t partition;
  PL_get_integer(in_partition, &partition);
  if (partition < 0) { partition = RD_KAFKA_PARTITION_UA; }

  int32_t llen;
  PL_get_integer(in_len, &llen);
  if (llen <= 0) { PL_fail; }

  if (PL_is_variable(in_list)) { PL_fail; }

  term_t hd = PL_new_term_ref();
  term_t ls = PL_copy_term_ref(in_list);
  rd_kafka_message_t msgs[llen];
  int cnt = 0;
  while (PL_get_list(ls, hd, ls))
  {
    char *k_payload; int n_payload = 0;

    if (!PL_get_chars(hd, &k_payload, CVT_ATOM|CVT_STRING)) { PL_fail; }
    n_payload = strlen(k_payload);
    msgs[cnt].payload = k_payload;
    msgs[cnt].len = n_payload;
    msgs[cnt].key_len = 0; msgs[cnt].key = NULL;
    msgs[cnt]._private = NULL;
    msgs[cnt].err = 0;

    cnt++;
  }

  int res = rd_kafka_produce_batch(rkt, partition,
              RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK,
              msgs, cnt);
  if (res != cnt) {
    printf("kafka_produce_batch produced: %d\n", res);
    PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_consume_batch(term_t in_topic, term_t in_partition, term_t in_timeout, term_t out_list)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }

  int32_t partition;
  PL_get_integer(in_partition, &partition);
  if (partition < 0) { partition = RD_KAFKA_PARTITION_UA; }

  int32_t timeout;
  PL_get_integer(in_timeout, &timeout);
  if (timeout <= 0) { PL_fail; }

  if (!PL_is_variable(out_list)) { PL_fail; }

  int sz = 100;
  rd_kafka_message_t* msgs[sz];

  int res = rd_kafka_consume_batch(rkt, partition, timeout,
                                   msgs, sz);
  if (res <= 0) {
    PL_fail;
  }

  term_t ele = PL_new_term_ref();
  term_t lst = PL_copy_term_ref(out_list);
  int idx = 0;
  int isOK = 1;
  while (idx < res)
  {
    if (msgs[idx]->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) { break; }
    if (msgs[idx]->err == 0) {
      if (!PL_unify_list(lst, ele, lst)) { isOK = 0; break; }
      if (!PL_unify_term(ele, PL_NCHARS, msgs[idx]->len, (char*)msgs[idx]->payload)) { isOK = 0; break; }
    }
    rd_kafka_message_destroy(msgs[idx]);
    idx++;
  }

  if (isOK) {
    return PL_unify_nil(lst);
  }
  PL_fail;
}

foreign_t swi_kafka_consume_start(term_t in_topic, term_t in_partition, term_t in_offset)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }

  int32_t partition;
  PL_get_integer(in_partition, &partition);
  if (partition < 0) { partition = RD_KAFKA_PARTITION_UA; }

  int64_t offset;
  PL_get_int64(in_offset, &offset);

  if (rd_kafka_consume_start(rkt, partition, offset) != 0)
  {
    PL_fail;
  }
  PL_succeed;
}

foreign_t swi_kafka_consume_stop(term_t in_topic, term_t in_partition)
{
  if (PL_is_variable(in_topic)) { PL_fail; }
  rd_kafka_topic_t *rkt;
  if (!PL_get_pointer(in_topic, (void**)&rkt)) { PL_fail; }

  int32_t partition;
  PL_get_integer(in_partition, &partition);
  if (partition < 0) { partition = RD_KAFKA_PARTITION_UA; }

  if (rd_kafka_consume_stop(rkt, partition) != 0)
  {
    PL_fail;
  }
  PL_succeed;
}

foreign_t swi_kafka_flush(term_t in_client, term_t in_timeout)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  if (PL_is_variable(in_timeout)) { PL_fail; }
  int32_t timeout;
  PL_get_integer(in_timeout, &timeout);

  rd_kafka_resp_err_t res = rd_kafka_flush(rk, timeout);
  if (res != RD_KAFKA_RESP_ERR_NO_ERROR) { PL_fail; }
  PL_succeed;
}

int unify_message(rd_kafka_message_t *msg, term_t out_msg, term_t out_meta)
{
  if (!msg || msg->err != 0) { return -1; }
  if (!PL_is_variable(out_msg)) { return -2; }
  if (!PL_is_variable(out_meta)) { return -3; }

  if (!PL_unify_term(out_msg, PL_NCHARS, msg->len, (char*)msg->payload)) { return 1; }

  term_t ele = PL_new_term_ref();
  term_t lst = PL_copy_term_ref(out_meta);

  functor_t fct = PL_new_functor(PL_new_atom("partition"), 1);
  if (!PL_unify_list(lst, ele, lst)) { return 2; }
  if (!PL_unify_term(ele, PL_FUNCTOR, fct,
                          PL_LONG, msg->partition)) { return 3; }
  fct = PL_new_functor(PL_new_atom("offset"), 1);
  if (!PL_unify_list(lst, ele, lst)) { return 4; }
  if (!PL_unify_term(ele, PL_FUNCTOR, fct,
                          PL_INT64, msg->offset)) { return 5; }
  if (msg->key_len > 0 && msg->key)
  {
    fct = PL_new_functor(PL_new_atom("key"), 1);
    if (!PL_unify_list(lst, ele, lst)) { return 6; }
    if (!PL_unify_term(ele, PL_FUNCTOR, fct,
                            PL_NCHARS, msg->key_len, msg->key)) { return 7; }
  }

  if (PL_unify_nil(lst) != TRUE) { return 99; }
  return 0;
}

foreign_t swi_kafka_consumer_poll(term_t in_client, term_t in_timeout, term_t out_message, term_t out_meta)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  if (PL_is_variable(in_timeout)) { PL_fail; }
  int32_t timeout;
  PL_get_integer(in_timeout, &timeout);

  if (!PL_is_variable(out_message)) { PL_fail; }
  if (!PL_is_variable(out_meta)) { PL_fail; }

  rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, timeout);
  if (!msg)
  {
    //printf("ERROR: nothing returned from poll\n");
    PL_fail;
  }
  if (msg->err != 0)
  {
    printf("ERROR: polling returned: %s\n", (char*)msg->payload);
    PL_fail;
  }
  int res;
  if ((res = unify_message(msg, out_message, out_meta)) != 0)
  {
    printf("ERROR: poll message unification returned: %d\n", res);
    PL_fail;
  }
  PL_succeed;
}

foreign_t swi_kafka_subscribe3(term_t in_client, term_t in_len, term_t in_topics)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  if (PL_is_variable(in_len)) { PL_fail; }
  int32_t len;
  PL_get_integer(in_len, &len);

  if (PL_is_variable(in_topics)) { PL_fail; }

  term_t hd = PL_new_term_ref();
  term_t ls = PL_copy_term_ref(in_topics);
  rd_kafka_topic_partition_list_t *ktl = rd_kafka_topic_partition_list_new(len);
  int cnt = 0;
  while (PL_get_list(ls, hd, ls))
  {
    cnt++;
    if (cnt > len) { break; }
    char *k_topic;
    if (!PL_get_chars(hd, &k_topic, CVT_ATOM|CVT_STRING)) { PL_fail; }
    rd_kafka_topic_partition_list_add(ktl, k_topic, -1);
  }
  rd_kafka_resp_err_t res = rd_kafka_subscribe(rk, ktl);
  rd_kafka_topic_partition_list_destroy(ktl);
  if (res != RD_KAFKA_RESP_ERR_NO_ERROR) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_subscribe5(term_t in_client, term_t in_lo, term_t in_hi, term_t in_len, term_t in_topics)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  if (PL_is_variable(in_lo)) { PL_fail; }
  int32_t lo;
  PL_get_integer(in_lo, &lo);

  if (PL_is_variable(in_hi)) { PL_fail; }
  int32_t hi;
  PL_get_integer(in_hi, &hi);

  if (PL_is_variable(in_len)) { PL_fail; }
  int32_t len;
  PL_get_integer(in_len, &len);

  if (PL_is_variable(in_topics)) { PL_fail; }

  term_t hd = PL_new_term_ref();
  term_t ls = PL_copy_term_ref(in_topics);
  rd_kafka_topic_partition_list_t *ktl = rd_kafka_topic_partition_list_new(len);
  int cnt = 0;
  while (PL_get_list(ls, hd, ls))
  {
    cnt++;
    if (cnt > len) { break; }
    char *k_topic;
    if (!PL_get_chars(hd, &k_topic, CVT_ATOM|CVT_STRING)) { PL_fail; }
    rd_kafka_topic_partition_list_add_range(ktl, k_topic, lo, hi);
  }
  rd_kafka_resp_err_t res = rd_kafka_subscribe(rk, ktl);
  rd_kafka_topic_partition_list_destroy(ktl);
  if (res != RD_KAFKA_RESP_ERR_NO_ERROR) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_unsubscribe(term_t in_client)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  rd_kafka_resp_err_t res = rd_kafka_unsubscribe(rk);
  if (res != RD_KAFKA_RESP_ERR_NO_ERROR) { PL_fail; }
  PL_succeed;
}

foreign_t swi_kafka_consumer_close(term_t in_client)
{
  if (PL_is_variable(in_client)) { PL_fail; }
  rd_kafka_t *rk;
  if (!PL_get_pointer(in_client, (void**)&rk)) { PL_fail; }

  rd_kafka_resp_err_t res = rd_kafka_consumer_close(rk);
  if (res != RD_KAFKA_RESP_ERR_NO_ERROR) { PL_fail; }
  PL_succeed;
}
