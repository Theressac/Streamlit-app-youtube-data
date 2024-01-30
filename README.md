YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Introduction

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL database.

Table of Contents

    1.	Key Technologies
    2.	Installation
    3.	Usage
    4.	Features

Key Technologies
    •	Python scripting
    •	Data Collection
    •	API integration
    •	Streamlit
    •	Database Management using MongoDB and Postgresql
    
Installation

    To run this project, you will need to install the following packages
        pip install google-api-python-client
        pip install streamlit
        pip install pymongo
        pip install psycopg2
        pip install streamlit_option_menu
        pip install pandas
    
Usage

   To use this project, kindly follow the following steps:
        1.	Clone the repository: git clone https://github.com/Theressac/Streamlit-app-youtube-data
        2.	Install the required packages
        3.	Run the Streamlit app: streamlit run yt_data_harvesting.py
        4.	Access the app in your browser at http://localhost:8501

Features

        •	Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
        •	Store the retrieved data in the MongoDB database.
        •	Migrate the data into Postgresql
        •	Perform queries on the SQL database

Author

    @Theressac

Skills take away from this project

    With this project, you will gain hands-on experience in working with APIs, database management using MongoDB and SQL, streamlit, data collection and Python scripting. I built a simple system that allows seamless data harvesting, efficient warehousing, and user-friendly data exploration.


