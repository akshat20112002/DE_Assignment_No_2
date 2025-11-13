# FROM ubuntu:22.04

# # ------------ System setup ------------------
# RUN apt-get update && \
#     DEBIAN_FRONTEND=noninteractive apt-get install -y \
#         default-jdk \
#         scala \
#         curl \
#         unzip \
#         vim \
#         python3 \
#         python3-pip \
#         python3-dev \
#         build-essential \
#         libssl-dev \
#         libffi-dev \
#         libpq-dev \
#         software-properties-common && \
#     apt-get clean

# # ------------ Install Spark (COPY instead of wget) ------------------
# COPY spark-3.4.1-bin-hadoop3.tgz /tmp/spark.tgz

# RUN tar -xzf /tmp/spark.tgz -C /usr/local/ && \
#     mv /usr/local/spark-3.4.1-bin-hadoop3 /usr/local/spark && \
#     ln -s /usr/local/spark /spark && \
#     rm /tmp/spark.tgz

# ENV SPARK_HOME=/usr/local/spark
# ENV PATH="$SPARK_HOME/bin:$PATH"
# ENV PYSPARK_PYTHON=python3

# WORKDIR /app

# COPY requirements.txt /app/requirements.txt

# RUN pip3 install --upgrade pip && \
#     pip3 install -r /app/requirements.txt --break-system-packages

# COPY . /app
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y default-jdk scala python3 python3-pip curl unzip libpq-dev \
                       build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

# Copy Spark from local file instead of downloading
ADD spark-3.4.1-bin-hadoop3.tgz /usr/local/
RUN mv /usr/local/spark-3.4.1-bin-hadoop3 /usr/local/spark && ln -s /usr/local/spark /spark

WORKDIR /app
COPY . /app
RUN pip3 install --no-cache-dir -r requirements.txt

ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

CMD ["python3", "main.py"]