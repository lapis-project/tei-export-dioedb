# syntax=docker/dockerfile:1
FROM ghcr.io/acdh-oeaw/noske-ubi9/noske:latest AS corpus

ENV CORPLIST=dioedbcorpus \
    LANG=C.UTF8
COPY verticals/ /var/lib/manatee/verticals/dioedb-corpus-data
COPY nosketchengine/dioedbcorpus /var/lib/manatee/registry/dioedbcorpus
# Build will not save anything in /var/lib/manatee/data/ as this is defined as VOLUME
USER root
RUN path=$(grep -h ^PATH $MANATEE_REGISTRY/$CORPLIST | sed -e 's/^PATH[^"]*"\(.*\)"$/\1/g'); \
    mkdir -p $path && (compilecorp $CORPLIST || /bin/true) &&\
    mkdir -p /data.precompiled &&\
    mv $path /data.precompiled/ &&\
    chown -R www-data:www-data /data.precompiled

FROM corpus AS authenticated
COPY nosketchengine/auth.conf /etc/lighttpd/noske-auth.conf
COPY nosketchengine/validate-auth.py /usr/local/bin/validate-noske-auth.py
COPY nosketchengine/healthz /var/www/healthz
# Only the bcrypt verifier persists; the secret mount is never an image layer.
RUN --mount=type=secret,id=noske_htpasswd,required=true \
    python3 /usr/local/bin/validate-noske-auth.py /run/secrets/noske_htpasswd && \
    install -m 0640 -o root -g www-data /run/secrets/noske_htpasswd /etc/lighttpd/noske.htpasswd && \
    sed -i '\|include_shell "/etc/lighttpd/add_auth.sh |d' /etc/lighttpd/lighttpd.conf && \
    printf '\ninclude "/etc/lighttpd/noske-auth.conf"\n' >> /etc/lighttpd/lighttpd.conf && \
    /usr/sbin/lighttpd -tt -f /etc/lighttpd/lighttpd.conf 3>&1
USER www-data
HEALTHCHECK --timeout=3s CMD curl -fsS http://127.0.0.1:8080/healthz >/dev/null || exit 1
CMD ["/bin/sh", "-c", "mv /data.precompiled/* /var/lib/manatee/data; exec /bin/sh /run_lighttpd.sh"]
