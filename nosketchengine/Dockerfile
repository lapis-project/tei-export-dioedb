FROM ghcr.io/acdh-oeaw/noske-ubi9/noske:latest

ENV CORPLIST=dioedbcorpus \
    LANG=C.UTF8
COPY verticals/ /var/lib/manatee/data/verticals/dioedbcorpus-corpus-data/
COPY nosketchengine/dioedbcorpus /var/lib/manatee/registry/dioedbcorpus
# Build will not save anything in /var/lib/manatee/data/ as this is defined as VOLUME
USER root
RUN path=$(grep -h ^PATH $MANATEE_REGISTRY/$CORPLIST | sed -e 's/^PATH[^"]*"\(.*\)"$/\1/g'); \
    mkdir -p $path && (compilecorp $CORPLIST || /bin/true) &&\
    mkdir -p /data.precompiled &&\
    mv $path /data.precompiled/ &&\
    chown -R www-data:www-data /data.precompiled
USER www-data
CMD ["/bin/sh", "-c", "mv /data.precompiled/* /var/lib/manatee/data; exec /bin/sh /run_lighttpd.sh"]