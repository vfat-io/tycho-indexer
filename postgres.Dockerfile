FROM ghcr.io/dbsystel/postgresql-partman:15-5
ARG PGCRON_VERSION="1.6.2"
USER root
RUN cd /tmp \
    && wget "https://github.com/citusdata/pg_cron/archive/refs/tags/v${PGCRON_VERSION}.tar.gz" \
    && tar zxf v${PGCRON_VERSION}.tar.gz \
    && cd pg_cron-${PGCRON_VERSION} \
    && make \
    && make install \
    && cd .. && rm -r pg_cron-${PGCRON_VERSION} v${PGCRON_VERSION}.tar.gz
RUN echo "cron.database_name = 'tycho_indexer_0'" >> /opt/bitnami/postgresql/conf/postgresql.conf
USER 1001
