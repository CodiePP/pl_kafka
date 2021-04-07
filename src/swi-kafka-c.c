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
foreign_t swi_kafka_conf_dump(term_t cid, term_t list);
foreign_t swi_kafka_topic_new(term_t cid, term_t producer, atom_t name, term_t topic);
foreign_t swi_kafka_topic_destroy(term_t topic);
foreign_t swi_kafka_produce(term_t topic, term_t partition, term_t payload, atom_t key);
foreign_t swi_kafka_flush(term_t client, term_t timeout);


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
  PL_register_foreign("pl_kafka_conf_dump", 2, swi_kafka_conf_dump, 0);
  PL_register_foreign("pl_kafka_topic_new", 4, swi_kafka_topic_new, 0);
  PL_register_foreign("pl_kafka_topic_destroy", 1, swi_kafka_topic_destroy, 0);
  PL_register_foreign("pl_kafka_produce", 4, swi_kafka_produce, 0);
  PL_register_foreign("pl_kafka_flush", 2, swi_kafka_flush, 0);
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

