from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
#Connection to mongo DB database
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import urllib

#set up the connection with mongo DB
encoded_username = urllib.parse.quote_plus("dummy_user")
encoded_password = urllib.parse.quote_plus("Amarnath@06022014")
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.inzqiif.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
  client.admin.command('ping')
  print("Pinged your deployment.You successfully connected to MongoDB!")
except Exception as e:
  print(e)

  #Create db in the Mongo DB
db = client["youtube_db"]

#This helps to connect to the API
def main():
    api_key = "AIzaSyBdj-YxSEJmxW4Zl-bgtPzEU6Ne-Vbuq0M"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube


youtube = main()


#This helps to get all et the channel details based on the provided channel id
def get_channel_details(channel_id):
  request = youtube.channels().list(
      part = "snippet,contentDetails, statistics",
      id = channel_id
  )
  response = request.execute()

  for i in response['items']:
    data = dict(Channel_Name=i['snippet']['title'],
                Channel_Id = i['id'],
                Subcription_Count = i['statistics']['subscriberCount'],
                Channel_Views = i['statistics']['viewCount'],
                Total_videos = i["statistics"]["videoCount"],
                Channel_Description=i['snippet']['description'],
                Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])
  return data




#To get all the videos IDs
def get_videos_ids(channel_id):
  video_ids = []
  response = youtube.channels().list(
      id = channel_id,
      part='contentDetails'
  ).execute()
  Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token = None
  while True:
    response1 = youtube.playlistItems().list(
        part='snippet',
        playlistId=Playlist_Id,maxResults = 50,
        pageToken = next_page_token
    ).execute()
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token = response1.get('nextPageToken')

    if next_page_token is None:
      break
  return video_ids






#To get all the videos details

def get_video_details(video_ids):
  video_data=[]
  for video_id in video_ids:
    request = youtube.videos().list(
        part="snippet,ContentDetails,statistics",
        id = video_id
    )
    response = request.execute()
    for item in response["items"]:
      data = dict(Channel_Name = item['snippet']['channelTitle'],
                  Channel_Id = item['snippet']['channelId'],
                  Video_Id = item['id'],
                  Title = item['snippet']['title'],
                  Tags = item['snippet'].get('tags'),
                  Thumbnail = item['snippet']['thumbnails']['default']['url'],
                  Description = item['snippet'].get('description'),
                  Published_Date = item['snippet']['publishedAt'],
                  Duration = item['contentDetails']['duration'],
                  Views_count = item['statistics'].get('viewCount'),
                  Like_count = item['statistics'].get('likeCount'),
                  Comments = item['statistics'].get('commentCount'),
                  Favorite_Count = item['statistics']['favoriteCount'],
                  Definition = item['contentDetails']['definition'],
                  Caption_status = item['contentDetails']['caption']
                  )
      video_data.append(data)
  return video_data


#to get all the comment details
#try except is not working properly need to check before submitting the project
def get_comment_details(video_ids):
  Comments_list =[]
  try:
    for video_id in video_ids:
      request = youtube.commentThreads().list(
        part = 'snippet',
        videoId= video_id,
        maxResults=100
      )
      response=request.execute()
      for item in response['items']:
        data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                    Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'],
        )
        Comments_list.append(data)
  except:
    pass
  return Comments_list




#to get all the playlist details
#UCuI5XcJYynHa5k_lqDzAgwQ
def get_playlist_details(channel_id):
  playlist =[]
  next_page_token=None
  while True:
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=50,
        pageToken=next_page_token
    )
    response = request.execute()

    for i in response['items']:
      data = dict(Playlist_Id = i['id'],
                  Playlist_Title = i['snippet']['title'],
                  Channel_Id = i['snippet']['channelId'],
                  Channel_Name = i['snippet']['channelTitle'],
                  Published_Date = i['snippet']['publishedAt'],
                  Video_Count = i['contentDetails']['itemCount']
                  )
      playlist.append(data)
    next_page_token = response.get('nextPageToken')
    if next_page_token is None:
      break
  return playlist




#the main function to call all the sub functons created above an it will insert the data into the mongo db 
def youtube_channels(channel_id):
  channel_details = get_channel_details(channel_id)
  playlist_details = get_playlist_details(channel_id)
  videos_list = get_videos_ids(channel_id)
  video_details = get_video_details(videos_list)
  comments_details = get_comment_details(videos_list)

  collections1 = db["channel_details"]
  collections1.insert_one({"channel_information":channel_details,
                           "playlist_information":playlist_details,
                           "video_information":video_details,
                           "comment_information":comments_details})


  return "upload successfully completed"


# Create tables for channels, playlists, videos and comments in postgresql
def create_channels_table():
    mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sa",
                          database="youtube",
                          port="5432"
    
    )
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    
   
    create_query='''create table if not exists channels(Channel_Id varchar(200) primary key,
                                                    Channel_Name varchar(250),
                                                    Subcription_Count bigint,
                                                    Channel_Views bigint,
                                                    Total_Videos bigint,
                                                    Channel_Description text,
                                                    Playlist_Id varchar(250))'''
    cursor.execute(create_query)
    mydb.commit()

    
    
    channel_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(data["channel_information"])
    df_channels = pd.DataFrame(channel_list)
    
  
    for index, row in df_channels.iterrows():
        insert_query = '''insert into channels(Channel_Id,
                                        Channel_Name,
                                        Subcription_Count,
                                        Channel_Views,
                                        Total_Videos,
                                        Channel_Description,
                                        Playlist_Id)
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Id'],
        row['Channel_Name'],
        row['Subcription_Count'],
        row['Channel_Views'],
        row['Total_videos'],
        row['Channel_Description'],
        row['Playlist_Id'])


        cursor.execute(insert_query,values)
        mydb.commit()


    # unable to fetche the total_videos from the API
def create_playlist_table():
    mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sa",
                          database="youtube",
                          port="5432"
    
    )
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    
    
    create_query='''create table if not exists playlists(Playlist_Id varchar(250) primary key,
                                                      Playlist_Title varchar(200),
                                                      Channel_Id varchar(100),
                                                      Channel_Name varchar(100),
                                                      Published_Date timestamp,
                                                      Video_Count int)'''
                
    cursor.execute(create_query)
    mydb.commit()
    
    playlist_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"playlist_information":1}):
      for i in range (len(data["playlist_information"])):
        playlist_list.append(data["playlist_information"][i])
    df_playlist = pd.DataFrame(playlist_list)
    
    
    for index, row in df_playlist.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Playlist_Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_Date,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Playlist_Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Published_Date'],
                row['Video_Count'])
    
    
        cursor.execute(insert_query,values)
        mydb.commit()


def create_videos_table():
    mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sa",
                          database="youtube",
                          port="5432"
    
    )
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    
    
    create_query='''create table if not exists videos(Channel_Name varchar(250),
                                                      Channel_Id varchar(200),
                                                      Video_Id varchar(50) primary key,
                                                      Title varchar(150),
                                                      Tags text,
                                                      Thumbnail varchar(200),
                                                      Description text,
                                                      Published_Date timestamp,
                                                      Duration interval,
                                                      Views_count bigint,
                                                      Like_count bigint,
                                                      Comments int,
                                                      Favorite_Count int,
                                                      Definition varchar(10),
                                                      Caption_status varchar(50)
                                                      )'''
    
    
                
    cursor.execute(create_query)
    mydb.commit()
    
    
    video_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"video_information":1}):
      for i in range (len(data["video_information"])):
        video_list.append(data["video_information"][i])
    df_videolist = pd.DataFrame(video_list)
    
    for index, row in df_videolist.iterrows():
          insert_query = '''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views_count,
                                                Like_count,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          values=(row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views_count'],
                  row['Like_count'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_status']
                 )
                                            
                                                     
          cursor.execute(insert_query,values)
          mydb.commit()



#Creation comments table
def create_comments_table():
    mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sa",
                          database="youtube",
                          port="5432"
    
    )
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()
    
    
    create_query='''create table if not exists comments(Comment_Id varchar(250) primary key,
                                                      Video_Id varchar(100),
                                                      Comment_Text text,
                                                      Channel_Author varchar(150),
                                                      Comment_Published timestamp)'''
                
    cursor.execute(create_query)
    mydb.commit()
    
    comment_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"comment_information":1}):
      for i in range (len(data["comment_information"])):
        comment_list.append(data["comment_information"][i])
    df_commentlist = pd.DataFrame(comment_list)
    
    
    for index, row in df_commentlist.iterrows():
          insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Channel_Author,
                                                Comment_Published)
                                                
                                                values(%s,%s,%s,%s,%s)'''
          values=(row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  row['Comment_Published'])
          
        
          cursor.execute(insert_query,values)
          mydb.commit()



def tables_creation():
    create_channels_table()
    create_videos_table()
    create_comments_table()
    create_playlist_table()

    return "Tables created successfully"



Tables = tables_creation()


def view_channels_list():
    channel_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(data["channel_information"])
    df = st.dataframe(channel_list)
    return df



def view_playlist():
    playlist_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"playlist_information":1}):
      for i in range (len(data["playlist_information"])):
        playlist_list.append(data["playlist_information"][i])
    df_playlist = st.dataframe(playlist_list)
    return df_playlist


#To show the videos list
def view_videos_list():
    video_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"video_information":1}):
      for i in range (len(data["video_information"])):
        video_list.append(data["video_information"][i])
    df_videolist = st.dataframe(video_list)
    return df_videolist


#To show the list of comments
def view_comments_list():
    comment_list = []
    db = client["youtube_db"]
    collections1 = db["channel_details"]
    for data in collections1.find({},{"_id":0,"comment_information":1}):
      for i in range (len(data["comment_information"])):
        comment_list.append(data["comment_information"][i])
    df_commentlist = st.dataframe(comment_list)
    return df_commentlist



#Streamlit codes


st.set_page_config(layout="wide")

st.header(":red[Youtube Data Harvesting and Warehousing Project]")


selected = option_menu(
menu_title =None,
options=["Channel ID","Filter by category","Queries"],
icons=["search","list","question-square-fill"],
menu_icon="cast",
default_index=0,
orientation="horizontal",
styles={
   "container":{"padding":"0!important","background-color":"#2a9d8f","font-weight":"bold"},
   "icon":{"color":"white","font-size":"25px"},
   "nav-link":{
      "font-size":"18px",
      "text-align":"left",
      "margin":"0px",
      "--hover-color":"#eee",
   },
   "nav-link-selected":{"background-color":"#d4a373!important"}
},
)


if selected == "Channel ID":
  channel_id=st.text_input("Enter the channel ID")

  if st.button("collect and store data"):
      channel_ids=[]
      db = client["youtube_db"]
      collections1 = db["channel_details"]
      for channel_data in collections1.find({},{"_id":0,"channel_information":1}):
          channel_ids.append(channel_data["channel_information"]["Channel_Id"])
      
      if channel_id in channel_ids:
          st.success("The given channel details exist alerady in the MongoDB")
      else:
          insert = youtube_channels(channel_id)
          st.success(insert)

  if st.button("Migrate to SQL"):
      Table=tables_creation()
      st.success(Table)


elif selected == "Filter by category":
  show_table = st.radio("Select the table for view",("Channels","Playlists","Videos","Comments"))

  if show_table == "Channels":
      view_channels_list()

  elif show_table == "Playlists":
      view_playlist()

  elif show_table == "Videos":
      view_videos_list()

  elif show_table == "Comments":
      view_comments_list()






#codes for 10 questions
elif selected == "Queries":   
  mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sa",
                        database="youtube",
                        port="5432"

  )
  cursor = mydb.cursor()    

  question=st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of videos for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

  #Query 1

  if question == "1. What are the names of all the videos and their corresponding channels?":
      query1 = '''select title,channel_name from videos'''
      cursor.execute(query1)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query1=pd.DataFrame(responsequery,columns=["Video title","channel name"])
      st.write(df_query1)

  elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
      query2 = '''select channel_name,total_videos from channels order by total_videos desc'''
      cursor.execute(query2)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query2=pd.DataFrame(responsequery,columns=["Channel name","Total number of videos"])
      st.write(df_query2)

  elif question == "3. What are the top 10 most viewed videos and their respective channels?":
      query3 = '''select channel_name,title, views_count from videos where views_count is not null order by views_count desc limit 10'''
      cursor.execute(query3)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query3=pd.DataFrame(responsequery,columns=["Channel name","Video title","Video count"])
      st.write(df_query3)

  elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
      query4 = '''select comments,title from videos where comments is not null'''
      cursor.execute(query4)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query4=pd.DataFrame(responsequery,columns=["No of comments","Video title"])
      st.write(df_query4)

  elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
      query5 = '''select channel_name,title, like_count from videos where like_count is not null order by like_count desc'''
      cursor.execute(query5)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query5=pd.DataFrame(responsequery,columns=["Channel name ","Video title","Like count"])
      st.write(df_query5)

  elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
      query6 = '''select title, like_count from videos'''
      cursor.execute(query6)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query6=pd.DataFrame(responsequery,columns=["Video title","Like count"])
      st.write(df_query6)

  #Question 7
  elif question == "7. What is the total number of videos for each channel, and what are their corresponding channel names?":
      query7 = '''select channel_name, channel_views from channels'''
      cursor.execute(query7)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query7=pd.DataFrame(responsequery,columns=["Channel name","Views count"])
      st.write(df_query7)

  #Question 8
  elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
      query8 = '''select channel_name, title,published_date from videos where extract(year from published_date)=2022'''
      cursor.execute(query8)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query8=pd.DataFrame(responsequery,columns=["Channel name","Video Title","Published date"])
      st.write(df_query8)

  #Question 9
  elif question == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
      query9 = '''select channel_name, AVG(duration) as avg_duration from videos group by channel_name'''
      cursor.execute(query9)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query9=pd.DataFrame(responsequery,columns=["Channel name","Average duration"])
      split_response = []
      for index,row in df_query9.iterrows():
        channel_title = row["Channel name"]
        average_duration = str(row["Average duration"])
        split_response.append(dict(ChannelTitle=channel_title,avgduration=average_duration))
      df_query9_string = pd.DataFrame(split_response)
      st.write(df_query9_string)

  #Question 10
  elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
      query10 = '''select title, channel_name,comments from videos where comments is not null order by comments desc'''
      cursor.execute(query10)
      mydb.commit()
      responsequery = cursor.fetchall()
      df_query10=pd.DataFrame(responsequery,columns=["Video title","Channel name","No of comments"])
      st.write(df_query10)