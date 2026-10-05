FROM maven:3.10.0-amazoncorretto-25-al2023 AS backend-builder

WORKDIR /kitu

COPY server /kitu/server

WORKDIR /kitu/server

RUN mvn package -DskipTests

FROM amazoncorretto:25.0.4-al2023

# amazoncorretto:25.0.3-al2023 on minimaalinen AL2023 -image ilman
# shadow-utils-pakettia, joten lisätään ei-root-käyttäjä suoraan
# /etc/passwd- ja /etc/group-tiedostoihin pakettien asentamisen sijaan.
RUN echo 'app:x:10001:' >> /etc/group \
 && echo 'app:x:10001:10001::/kitu:/sbin/nologin' >> /etc/passwd \
 && mkdir -p /kitu/server/target \
 && chown -R 10001:10001 /kitu

WORKDIR /kitu/server/target

COPY --from=backend-builder --chown=10001:10001 /kitu/server/target/kitu-0.0.1-SNAPSHOT.jar /kitu/server/target/kitu-0.0.1-SNAPSHOT.jar

USER 10001

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "kitu-0.0.1-SNAPSHOT.jar"]
