# INDONESIA WEATHER ETL

This repository has the objective of offering an overview of Indonesia's weather, although it is currently in the development phase and includes an AI model for weather prediction. The weather data is acquired through an ETL process from visualcrosing, a website that provides a weather API. The technology stack employed for this project consists of Airflow DAG for workflow management, and a local website built using dash, plotly, and flask.

## Prerequisite
- ### Requirements
    - Install Make ~ `sudo apt install make`
    - Install Docker ~ `sudo apt install docker`
    - Install Docker Compose ~ `sudo apt install docker-compose`
- ### Deployment Scripts
    - I have grouped some of the deployment scripts into a make commands available at `Makefile`, the `help` options is available upon command `sudo make help`
        ```bash
    
        ## airflow		    - Run a airflow container, including its all dependency. \n"
	
        ## app			    - Run a local app for visualization dashboard using dash, spark and plotly \n"
	
        ## all              - Run all ETL and visualization pipeline. \n"

        ```

            Please note that this repo is using docker to build up a development environment, thus it may take a couple of minutes on pulling images from the hub. 

`Owner :    Raden Bimo`