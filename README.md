# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

# YouTube Data Analysis Streamlit App

This project aims to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application includes several features to retrieve, store, and analyze channel data.

## Features

1. **Retrieve YouTube Channel Data**: Users can input a YouTube channel ID and retrieve relevant data such as channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments of each video using the Google API.

2. **Store Data in MongoDB Data Lake**: Collected data can be stored in a MongoDB database, serving as a flexible and unstructured data lake.

3. **Data Collection from Channel**: Users can collect data from 1 YouTube channel. A single button click initiates data collection and storage in the data lake.

4. **Migrate to SQL Data Warehouse**: The application offers an option to select a channel name and migrate its data from the MongoDB data lake to a SQL database. This allows data to be structured and optimized for analysis.

5. **Query and Analyze Data**: Users can search and retrieve data from the SQL database using various search options, including joining tables to acquire detailed channel information.

6. **Interactive Streamlit UI**: The entire application is built using the Streamlit framework, providing an interactive and user-friendly interface for data access and visualization.


## Usage

1. Input a YouTube channel ID to retrieve channel data.
2. Use the provided button to collect and store data of the channel.
3. Select a channel and migrate its data from the MongoDB data lake to the SQL database.
4. Utilize the search options in the SQL database to query and retrieve specific data.
5. The Streamlit app will display data.

## Contributions

Contributions to the project are welcome! If you find any issues or have improvements to suggest, feel free to open a pull request.


