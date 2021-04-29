
FROM swipl

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      librdkafka-dev \
      automake \
      autoconf \
      pkg-config \
      git \
      build-essential \
      libssh-dev \
      zlib1g-dev \
      gcc

RUN mkdir -vp /SRC && cd /SRC && git clone https://github.com/CodiePP/pl_kafka.git pl_kafka.git

WORKDIR /SRC/pl_kafka.git

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

