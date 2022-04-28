#
##FROM bde2020/spark-python-template:3.1.1-hadoop3.2
##
##MAINTAINER You <you@example.org>
##
##ENV SPARK_APPLICATION_PYTHON_LOCATION /app/etl_main.py
#
#
#From python:3.6.15-slim-buster
#
#WORKDIR /code
#
#RUN apt-get update && \
#    apt-get install -y openjdk-8-jdk && \
#    apt-get install -y ant && \
#    apt-get clean;
#
#RUN apt-get update && \
#    apt-get install ca-certificates-java && \
#    apt-get clean && \
#    update-ca-certificates -f;
#
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
#RUN export JAVA_HOME
#
#COPY requirements.txt ./
#RUN pip install --no-cache-dir -r requirements.txt
#
#COPY . .
#
#CMD [ "python", "./etl_main.py" ]


FROM openjdk:8-jdk-slim-buster

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3.7 \
        python3-pip \
        python3.7-dev \
        python3-setuptools \
        python3-wheel

WORKDIR /code

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python3", "./etl_main.py" ]