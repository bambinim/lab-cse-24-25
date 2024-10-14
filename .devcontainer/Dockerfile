FROM alpine:3.19

RUN apk update
RUN apk add bash bash-completion vim git curl sudo zip unzip openssh-client \
    libxext libxrender libxtst libxi freetype procps gcompat
RUN cd / && \
    curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz --output ./spark.tgz && \
    tar -xzf ./spark.tgz
RUN apk add openjdk17-jdk aws-cli aws-cli-bash-completion
RUN apk add python3 py3-pip py3-virtualenv python3-dev gcc musl-dev linux-headers
RUN mkdir /home/user && \
    addgroup -S -g 1000 user && \
    adduser --system --home /home/user --shell /bin/bash -D -G user -u 1000 user

USER user
WORKDIR /home/user
ENV PATH="${PATH}:/spark-3.5.1-bin-hadoop3/bin:/home/user/.local/bin"
RUN python3 -m pip install --break-system-packages jupyter findspark spylon-kernel && \
    python3 -m spylon_kernel install --user

ENV HADOOP_HOME=/spark-3.5.1-bin-hadoop3
ENV SPARK_HOME=/spark-3.5.1-bin-hadoop3
ENV AWS_PROFILE=default
ENV AWS_ACCESS_KEY=''
ENV AWS_SECRET_ACCESS_KEY=''
ENV AWS_SESSION_TOKEN=''
