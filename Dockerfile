FROM python:3.8-slim

#  Required for pymssql
RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-common \
    freetds-dev

# Copy our own application
WORKDIR /app
COPY . /app/atd-kits

RUN chmod -R 755 /app/*

## Proceed to install the requirements...do
RUN cd /app/atd-kits && apt-get update && \
    pip install -r requirements.txt
