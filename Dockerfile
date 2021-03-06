FROM ubuntu:16.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y \
      apt-utils \
      apt-transport-https \
      lsb-release \
      curl \
      dnsutils \
      gcc \
      git \
      openssh-client \
      python2.7 \
      python-dev \
      python-pip \
      python-setuptools \
      vim \
      less \
      jq \
      ssh \
      ftp \
      wget

RUN  easy_install -U pip \
    && pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install -U crcmod \
    && pip install awscli --upgrade \
    && pip install yq --upgrade

RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk \
        google-cloud-sdk-app-engine-python \
        google-cloud-sdk-app-engine-java \
        google-cloud-sdk-app-engine-go \
        google-cloud-sdk-datalab \
        google-cloud-sdk-datastore-emulator \
        google-cloud-sdk-pubsub-emulator \
        google-cloud-sdk-bigtable-emulator \
        google-cloud-sdk-cbt \
        kubectl && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image && \
    gcloud --version 
 
 COPY . /GoogleReplication
 WORKDIR /GoogleReplication

 RUN  pip install -r requirements.txt

 CMD /bin/bash

