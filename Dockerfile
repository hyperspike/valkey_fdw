FROM alpine:3.21 AS builder

# Install required packages
RUN apk add --no-cache \
	curl \
	git \
	postgresql17-dev \
	gcc \
	musl-dev \
	openssl-dev \
	make \
	clang18-dev \
	&& curl -OL https://github.com/tianon/gosu/releases/download/1.17/gosu-amd64 \
	&& mv gosu-amd64 /usr/local/bin/gosu \
	&& chmod +x /usr/local/bin/gosu

# Install libvalkey
RUN git clone https://github.com/valkey-io/libvalkey.git \
	&& cd libvalkey \
	&& make PREFIX=/usr USE_TLS=1 DESTDIR=/ install \
	&& make PREFIX=/usr USE_TLS=1 DESTDIR=/home/valkey install

COPY ping.c /ping.c
COPY valkey_fdw--1.0.sql /valkey-fdw/valkey_fdw--1.0.sql
COPY Makefile /valkey-fdw/Makefile
COPY valkey_fdw.c /valkey-fdw/valkey_fdw.c
COPY valkey_fdw.control /valkey-fdw/valkey_fdw.control
COPY META.json /valkey-fdw/META.json

RUN  gcc -Wall -fPIC -o /ping /ping.c -L/usr/lib   -Wl,--as-needed -fvisibility=hidden -lvalkey

# Install valkey_fdw
RUN cd /valkey-fdw \
	&& make \
	&& make DESTDIR=/home/valkey install

#FROM postgres:17.2-alpine3.21

#COPY --from=builder /home/valkey/usr/lib/postgresql17/valkey_fdw.so                    /usr/local/lib/postgresql/valkey_fdw.so
#COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw.index.bc      /usr/local/lib/postgresql/bitcode/valkey_fdw.index.bc
#COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw/valkey_fdw.bc /usr/local/lib/postgresql/bitcode/valkey_fdw/valkey_fdw.bc
#COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw.control   /usr/local/share/postgresql/extension/valkey_fdw.control
#COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw--1.0.sql  /usr/local/share/postgresql/extension/valkey_fdw--1.0.sql

FROM alpine:3.21

RUN apk add --no-cache \
	postgresql17 \
	openssl \
	bash

COPY --from=builder /usr/local/bin/gosu /usr/local/bin/gosu
COPY --from=builder /home/valkey/usr/lib/postgresql17/valkey_fdw.so                    /usr/lib/postgresql17/valkey_fdw.so
COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw.index.bc      /usr/lib/postgresql17/bitcode/valkey_fdw.index.bc
COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw/valkey_fdw.bc /usr/lib/postgresql17/bitcode/valkey_fdw/valkey_fdw.bc
COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw.control   /usr/share/postgresql17/extension/valkey_fdw.control
COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw--1.0.sql  /usr/share/postgresql17/extension/valkey_fdw--1.0.sql
COPY --from=builder /ping /ping
COPY --from=builder /home/valkey/usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so.0.1
RUN ln -s /usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so.0 \
	&& ln -s /usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so

ENV PGDATA /var/lib/postgresql17/data
# this 1777 will be replaced by 0700 at runtime (allows semi-arbitrary "--user" values)
RUN install --verbose --directory --owner postgres --group postgres --mode 1777 "$PGDATA" \
	&& mkdir /docker-entrypoint-initdb.d
VOLUME /var/lib/postgresql/data

RUN set -eux; \
	cp -v /usr/share/postgresql17/postgresql.conf.sample /usr/share/postgresql17/postgresql.conf.sample.orig; \
	sed -ri "s!^#?(listen_addresses)\s*=\s*\S+.*!\1 = '*'!" /usr/share/postgresql17/postgresql.conf.sample; \
	grep -F "listen_addresses = '*'" /usr/share/postgresql17/postgresql.conf.sample

COPY docker-entrypoint.sh docker-ensure-initdb.sh /usr/local/bin/
RUN ln -sT docker-ensure-initdb.sh /usr/local/bin/docker-enforce-initdb.sh
ENTRYPOINT ["docker-entrypoint.sh"]

STOPSIGNAL SIGINT
#
# An additional setting that is recommended for all users regardless of this
# value is the runtime "--stop-timeout" (or your orchestrator/runtime's
# equivalent) for controlling how long to wait between sending the defined
# STOPSIGNAL and sending SIGKILL (which is likely to cause data corruption).
#
# The default in most runtimes (such as Docker) is 10 seconds, and the
# documentation at https://www.postgresql.org/docs/12/server-start.html notes
# that even 90 seconds may not be long enough in many instances.

EXPOSE 5432
CMD ["postgres"]
