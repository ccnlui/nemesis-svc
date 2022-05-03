FROM gradle:7.4.2-jdk17 AS builder
WORKDIR /nemesis-svc
COPY . .
RUN gradle installDist
ENTRYPOINT [ "/nemesis-svc/app/build/install/nemesis/bin/nemesis" ]

# TODO: multi-stage build.
# FROM eclipse-temurin:17-jre
# COPY --from=builder /nemesis-svc/app/build/install/nemesis /nemesis
# ENTRYPOINT [ "/nemesis/bin/nemesis" ]
