# GLaMoR-DataPipleine
Contains the Code created to Transform OWL Ontologies into suitable input for diverse machine learning models. It is part of the [GLaMoR](github.com/JustinMuecke/GLaMoR) Project. The model training is provided in the Model Training Submodule.

## Project Structure
```
├──GLaMoR-DataPipeline
│  ├──Data_Retrieval/ 
│  ├──Preprocessing/
│  ├──Initial_Publish/
│  ├──OAPT/
│  ├──OWL_Ontology_Modification/
│  ├──Prefix Removal/
│  ├──Translation/
│  ├──Tokenization/
│  ├──Embed/
│  ├──Analysis/
├──docker-compose.yml
├──init.sql
├──rabbitmq.conf
├──.gitignore
├──.gitmodules
├──README.md
├──LICENSE
```

## Requirements
If you want to use the code as provided, it is enough to have docker-compose installed on the system.

## Execution
When in the root folder containing the `docker-compose.yml` file, run
```
> docker-compose build 
> docker-compose up -d 
```
This will create multiple Docker images, container and networks. Specifcally
 - 1 Postgress container for Meta-Data Tracking
 - 1 RabbitMq container for message queueing between the different workers
 - 2 Networks with a Bridge Driver
 - N worker container for each processing step as definded in the `docker-compose.yml`

We recommend to deploy the most workers for the Modularization, Modification and Tokenization steps as these are the most resource intensive steps.