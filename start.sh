#!/bin/bash
export environment=LOCAL
docker build --no-cache -t alpha_airflow:2.3.4 .
#docker build -t alpha_airflow:2.3.4 .
docker tag alpha_airflow:2.3.4 alpha_airflow:2.3.4
docker-compose up -d
