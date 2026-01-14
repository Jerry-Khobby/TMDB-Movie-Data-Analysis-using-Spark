FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu
USER root
# Create directories with proper permissions upfront

RUN 
WORKDIR /streaming/app
COPY requirements.txt .
RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y --no-install-recommends wget && \
  wget -q https://jdbc.postgresql.org/download/postgresql-42.7.6.jar \
  -O /streaming/spark/jars/postgresql-42.7.6.jar && \
  pip3 install --no-cache-dir -r requirements.txt && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
COPY . /streaming/app
# Set ownership
RUN chown -R spark:spark /streaming
USER spark
EXPOSE 8080
CMD ["/bin/bash"]