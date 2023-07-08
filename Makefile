include .env
export

CURRENT_DATE := $(shell date +'%Y-%m-%d')

help:
	@echo "## airflow		- Run a airflow container, including its all dependency. \n"
	@echo "## app			- Run a local app for visualization dashboard using dash, spark and plotly \n"
	@echo "## all           - Run all ETL and visualization pipeline. \n"

all: airflow app 

airflow: airflow-create

airflow-create:
	@mkdir -p ./docker/dags ./docker/logs ./docker/plugins ./docker/config
	@docker-compose -f ./docker/airflow/docker-compose.yaml up airflow-init
	@docker-compose -f ./docker/airflow/docker-compose.yaml up

app: app-create

app-create:
	@docker build ./docker/web/
	@docker-compose -f ./docker/web/docker-compose-app.yaml --env-file .env up -d --no-deps --build dashboard
