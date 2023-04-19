# twitter_player_performance
## Overview
This project aims to develop a web application that analyzes the correlation between football player performance and public sentiment on Twitter. The project focuses on 5 Bundesliga teams and 5 players from each team. The application collects daily tweets where one of the players was tagged and Bundesliga match stats, including the players' ratings in these matches.

The app is developed in Python and gathers tweets by querying the Twitter API. It also gathers players' ratings during matches by querying a football-API (RapidAPI). The raw data is stored in a data lake, which is a Hadoop Distributed File System (HDFS) cluster with 3 data nodes. The data is stored in a serialized form with Avro files. The data is loaded to the data lake in daily batches, with one Avro file per batch.

After the data is loaded into the data lake, a consolidation step is performed where unprocessed batches in the data lake are checked, data is extracted, and analyzed. This analysis involves applying a machine learning algorithm for sentiment analysis. This algorithm assigns a score for every tweet based on whether the tweet is negative or positive about the player. The analyzed tweet data, along with the players' ratings, is then loaded into the data warehouse.

The Data Warehouse is a Postgres relational database, with well-defined tables. In the end, Tableau is connected to the Postgres database in order to visualize the data and analyze the correlation between players' ratings and the public tweets about them.
## Technologies
* Python
* Twitter API
* Football-API (RapidAPI)
* Hadoop Distributed File System (HDFS)
* Avro
* Machine Learning Algorithms
* Postgres
* Tableau

## Data Collection
The project collects data from two sources: Twitter and a football-API. To collect data from Twitter, we use the Twitter API to search for tweets where one of the players was tagged. We then extract relevant information such as the tweet text, user information, and creation time.

To collect data from the football-API, we use RapidAPI to access data on Bundesliga matches, including the players' ratings in these matches.

Both sets of data are then stored in the data lake in serialized form using Avro files.
![](imgs/hdfs.png "Avro files per batch example")
## Data Processing
The data collected from Twitter and the football-API is processed through a machine learning algorithm for sentiment analysis. The sentiment analysis algorithm analyzes the tweet data and assigns a score to each tweet based on whether the tweet is positive or negative about the player.

Once the data has been analyzed, it is loaded into the data warehouse in a relational database format. The data warehouse is a Postgres database with well-defined tables that are optimized for data analysis.

![](imgs/postgres.png "Postgres DataBase tables")
## Data Analysis
Tableau is used to connect to the Postgres database and visualize the data. The goal is to analyze the correlation between players' ratings and public sentiment on Twitter. By visualizing the data, we can identify patterns and trends that may provide insights into how public sentiment affects player performance.

![](imgs/jude.png "Jude Bellingham Example")

## Credits
This project was developed by Dbira Moez