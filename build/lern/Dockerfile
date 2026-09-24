ARG DHI_IMAGE_DEV=dhi.io/alpine-base:3.22-dev
ARG DHI_IMAGE_RUNTIME=dhi.io/eclipse-temurin:11-jre-alpine

FROM dhi.io/busybox:1.38.0-alpine3.24 as shell

FROM ${DHI_IMAGE_DEV} AS builder
RUN apk update \
    && apk add unzip curl "zlib>=1.3.2-r0"

WORKDIR /app
COPY modules/lern/service/target/lern-service-impl-1.0-SNAPSHOT-dist.zip .
RUN unzip lern-service-impl-1.0-SNAPSHOT-dist.zip

# --- runtime
FROM ${DHI_IMAGE_RUNTIME}
COPY --from=shell /bin/busybox /bin/sh
COPY --from=builder /usr/bin/curl /usr/bin/curl
COPY --from=builder /usr/lib/libcurl* /usr/lib/
COPY --from=builder /lib/libz* /lib/

ENTRYPOINT []

# Cloud Storage Provider selection — pass --build-arg CSP=<azure|aws|gcloud|oci> at build time.
# Defaults to azure. The Maven build must have been run with the matching profile (-P <csp>).
# cloud_storage_auth_type defaults to OIDC (Kubernetes Workload Identity).
# Override at runtime via K8s ConfigMap or: docker run -e sunbird_cloud_storage_auth_type=ACCESS_KEY
ARG CSP=azure
ENV sunbird_cloud_service_provider=${CSP}
ENV sunbird_cloud_storage_auth_type=OIDC

USER nonroot
COPY --from=builder --chown=nonroot:nonroot /app/lern-service-impl-1.0-SNAPSHOT /home/sunbird/lern-service-impl-1.0-SNAPSHOT
COPY modules/lern/service/conf/logback.xml /home/sunbird/lern-service-impl-1.0-SNAPSHOT/conf/logback.xml
WORKDIR /home/sunbird/

EXPOSE 9000

CMD java -XX:+PrintFlagsFinal $JAVA_OPTIONS \
  -Dlog4j2.formatMsgNoLookups=true \
  -Dplay.server.http.idleTimeout=180s \
  -Dlogback.configurationFile=/home/sunbird/lern-service-impl-1.0-SNAPSHOT/conf/logback.xml \
  -cp '/home/sunbird/lern-service-impl-1.0-SNAPSHOT/lib/lern-service-impl-1.0-SNAPSHOT.jar:/home/sunbird/lern-service-impl-1.0-SNAPSHOT/lib/*' \
  play.core.server.ProdServerStart /home/sunbird/lern-service-impl-1.0-SNAPSHOT