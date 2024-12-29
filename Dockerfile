FROM alpine:3.21 AS builder

COPY valkey_fdw--1.0.sql /valkey-fdw/valkey_fdw--1.0.sql
COPY Makefile /valkey-fdw/Makefile
COPY valkey_fdw.c /valkey-fdw/valkey_fdw.c
COPY valkey_fdw.control /valkey-fdw/valkey_fdw.control
COPY META.json /valkey-fdw/META.json

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
	&& git clone https://github.com/valkey-io/libvalkey.git \
	&& cd libvalkey \
	&& make PREFIX=/usr USE_TLS=1 \
	&& make install \
	&& make PREFIX=/usr DESTDIR=/home/valkey install \
	&& cd /valkey-fdw \
	&& make \
	&& make DESTDIR=/home/valkey install

FROM postgres:17.2-alpine3.21

COPY --from=builder /home/valkey/usr/lib/postgresql17/valkey_fdw.so /usr/lib/postgresql17/valkey_fdw.so
COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw.index.bc /usr/lib/postgresql17/bitcode/valkey_fdw.index.bc
COPY --from=builder /home/valkey/usr/lib/postgresql17/bitcode/valkey_fdw/valkey_fdw.bc /usr/lib/postgresql17/bitcode/valkey_fdw/valkey_fdw.bc
COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw.control /usr/share/postgresql17/extension/valkey_fdw.control
COPY --from=builder /home/valkey/usr/share/postgresql17/extension/valkey_fdw--1.0.sql /usr/share/postgresql17/extension/valkey_fdw--1.0.sql

COPY --from=builder /home/valkey/usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so.0.1

RUN ln -s /usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so.0 \
	&& ln -s /usr/lib/libvalkey.so.0.1 /usr/lib/libvalkey.so
