# twitter_player_performance
## Overview
This project aims to develop a web application that analyzes the correlation between football player performance and public sentiment on Twitter. The project focuses on 5 Bundesliga teams and 5 players from each team. The application collects daily tweets where one of the players was tagged and Bundesliga match stats, including the players' ratings in these matches.

The app is developed in Python and gathers tweets by querying the Twitter API. It also gathers players' ratings during matches by querying a football-API (RapidAPI). The raw data is stored in a data lake, which is a Hadoop Distributed File System (HDFS) cluster with 3 data nodes. The data is stored in a serialized form with Avro files. The data is loaded to the data lake in daily batches, with one Avro file per batch.

After the data is loaded into the data lake, a consolidation step is performed where unprocessed batches in the data lake are checked, data is extracted, and analyzed. This analysis involves applying a machine learning algorithm for sentiment analysis. This algorithm assigns a score for every tweet based on whether the tweet is negative or positive about the player. The analyzed tweet data, along with the players' ratings, is then loaded into the data warehouse.

The Data Warehouse is a Postgres relational database, with well-defined tables. In the end, Tableau is connected to the Postgres database in order to visualize the data and analyze the correlation between players' ratings and the public tweets about them.
## Technologies
Python
Twitter API
Football-API (RapidAPI)
Hadoop Distributed File System (HDFS)
Avro
Machine Learning Algorithms
Postgres
Tableau

## Dashboard Exemple
![](imgs/jude.png "Jude Bellingham Example")

## Credits
This project was developed by Dbira Moez