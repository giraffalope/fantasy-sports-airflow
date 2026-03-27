FROM mariadb:11.8-noble

COPY /db/sql/ddl/* /docker-entrypoint-initdb.d
# COPY /db/sql/data/* /docker-entrypoint-initdb.d
RUN mkdir -p /bulk
# COPY /db/sql/bulk/* /bulk

COPY /db/init.sh /init.sh

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]
