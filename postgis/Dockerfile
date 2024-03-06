FROM postgis/postgis:16-master

RUN apt-get update && \
    apt-get install -y postgresql-16-periods git build-essential postgresql-server-dev-16

WORKDIR /root
RUN git clone https://github.com/sraoss/pg_ivm.git
WORKDIR /root/pg_ivm
RUN make && make install
