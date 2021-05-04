FROM swipl

LABEL description="Prolog interface to Apache Kafka" \
      version="1.0.0" \
      license="GPL-3.0" \
      copyright="Copyright (C) 2021 Alexander Diemand" \
      maintainer="codieplusplus@apax.net" \
      homepage="https://github.com/CodiePP/pl_kafka"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      automake \
      autoconf \
      pkg-config \
      git \
      wget \
      build-essential \
      libssh-dev \
      zlib1g-dev \
      gcc

#RUN mkdir -vp /SRC && cd /SRC && git clone https://github.com/CodiePP/pl_kafka.git pl_kafka.git

WORKDIR /SRC

RUN wget https://github.com/edenhill/librdkafka/archive/refs/tags/v1.6.1.tar.gz && tar xzf v1.6.1.tar.gz

WORKDIR /SRC/librdkafka-1.6.1

RUN mkdir BUILD
RUN ./configure --prefix=$(pwd)/BUILD --CFLAGS=-fPIC --LDFLAGS=-fPIC --no-download --disable-sasl --enable-lz4 --enable-ssl --enable-zlib --disable-devel --disable-valgrind --disable-refcnt-debug && make && make install

WORKDIR /SRC

COPY . .

RUN aclocal --force && autoheader --force && autoconf --force

RUN ./configure

RUN make swi

RUN mkdir -v -p ${HOME}/lib/sbcl

RUN mkdir -v -p ${HOME}/.config/swi-prolog

RUN cp -v plkafka-Linux ${HOME}/lib/sbcl/plkafka

RUN cp -v src/kafka.qlf ${HOME}/lib/sbcl/

RUN echo ":- assertz(file_search_path(sbcl,'${HOME}/lib/sbcl'))." >> ${HOME}/.config/swi-prolog/init.pl

CMD ["swipl","-l","test/t1.pl","-g","test,halt."]
CMD ["swipl","-l","test/t2.pl","-g","test,halt."]
CMD ["swipl","-l","test/t3.pl","-g","test,halt."]

