# Sunbird Viewer Service Dockerfile (standalone / distributed deployment)
# Stage 1: Extraction
FROM alpine:3.20 AS builder
RUN apk update && apk add unzip
WORKDIR /app
COPY modules/viewer/service/target/viewer-service-1.0-SNAPSHOT-dist.zip .
RUN unzip viewer-service-1.0-SNAPSHOT-dist.zip

# Stage 2: Runtime
FROM eclipse-temurin:11-jre-alpine

# Cloud Storage Provider selection — pass --build-arg CSP=<azure|aws|gcloud|oci> at build time.
# Defaults to azure. The Maven build must have been run with the matching profile (-P <csp>).
# cloud_storage_auth_type defaults to OIDC (Kubernetes Workload Identity).
# Override at runtime via K8s ConfigMap or: docker run -e sunbird_cloud_storage_auth_type=ACCESS_KEY
ARG CSP=azure
ENV sunbird_cloud_service_provider=${CSP}
ENV sunbird_cloud_storage_auth_type=OIDC

RUN apk upgrade --no-cache \
    && apk add --no-cache curl "zlib>=1.3.2-r0" \
    && adduser -u 1001 -h /home/sunbird/ -D sunbird \
    && mkdir -p /home/sunbird/

WORKDIR /home/sunbird/
COPY --from=builder --chown=sunbird:sunbird /app/viewer-service-1.0-SNAPSHOT /home/sunbird/viewer-service-1.0-SNAPSHOT
COPY modules/viewer/service/conf/logback.xml /home/sunbird/viewer-service-1.0-SNAPSHOT/conf/logback.xml

USER sunbird
EXPOSE 9000

CMD java -XX:+PrintFlagsFinal $JAVA_OPTIONS \
  -Dlog4j2.formatMsgNoLookups=true \
  -Dplay.server.http.idleTimeout=180s \
  -Dlogback.configurationFile=/home/sunbird/viewer-service-1.0-SNAPSHOT/conf/logback.xml \
  -cp '/home/sunbird/viewer-service-1.0-SNAPSHOT/lib/viewer-service-1.0-SNAPSHOT.jar:/home/sunbird/viewer-service-1.0-SNAPSHOT/lib/*' \
  play.core.server.ProdServerStart /home/sunbird/viewer-service-1.0-SNAPSHOT
