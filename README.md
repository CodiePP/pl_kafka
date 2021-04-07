
# Interfacing to Kafka from Prolog

## compilation

```sh
aclocal --force && autoheader --force && autoconf --force
```
then run
```sh
./configure
```
then call `make swi`

## examples

### dump default configuration

the following code gets a list of the default top-level options and prints them on the console line by line.
```prolog
use_module(sbcl(kafka)).
kafka_conf_new(Cid), kafka_conf_dump(Cid, L), member(X,L), format("~p~n", [X]), fail.
```

