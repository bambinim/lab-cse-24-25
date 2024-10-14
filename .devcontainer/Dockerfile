FROM alpine:3.19

ARG JBR_RELEASE=jbr-21.0.4-linux-musl-x64-b620.4
ARG SPARK_VERSION=3.5.1

WORKDIR /root

RUN apk update
RUN apk add bash bash-completion vim git curl sudo zip unzip openssh-client \
    libxext libxrender libxtst libxi freetype procps gcompat
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz --output ./spark.tgz && \
    tar -xzf ./spark.tgz && \
    mv ./spark-${SPARK_VERSION}-bin-hadoop3 / && \
    rm ./spark.tgz
RUN curl https://d2xrhe97vsfxuc.cloudfront.net/${JBR_RELEASE}.tar.gz --output ./${JBR_RELEASE}.tar.gz && \
    tar -xzf ${JBR_RELEASE}.tar.gz && \
    mv ${JBR_RELEASE} /jbr && \
    rm ./${JBR_RELEASE}.tar.gz
RUN apk add openjdk17-jdk aws-cli aws-cli-bash-completion
RUN apk add python3 py3-pip py3-virtualenv python3-dev gcc musl-dev linux-headers
RUN mkdir /home/user && \
    addgroup -S -g 1000 user && \
    adduser --system --home /home/user --shell /bin/bash -D -G user -u 1000 user

USER user
WORKDIR /home/user
ENV PATH="${PATH}:/${SPARK_VERSION}/bin:/home/user/.local/bin"
RUN python3 -m pip install --break-system-packages jupyter findspark spylon-kernel && \
    python3 -m spylon_kernel install --user

ENV HADOOP_HOME=/${SPARK_VERSION}
ENV SPARK_HOME=/${SPARK_VERSION}
ENV AWS_PROFILE=default
ENV IDEA_JDK=/jbr
