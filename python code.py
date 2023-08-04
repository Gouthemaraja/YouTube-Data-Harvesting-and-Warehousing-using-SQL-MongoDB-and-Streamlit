#importing required packages
import pandas as pd
import numpy as np
import pymongo
import isodate

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from IPython.display import JSON
import json
from datetime import datetime

from mysqlx.errors import get_mysql_exception
from pymongo.errors import DuplicateKeyError


# -------------------------------------------------------------------------------------------------------------------------
api_service_name = "youtube"
api_version = "v3"
api_key = "YourAPIkey"
youtube = build(api_service_name, api_version,developerKey=api_key)

#get channel details
def get_channel_details(youtube,id):
    request = youtube.channels().list(part="snippet,statistics",id=id)
    response = request.execute()
    channel_name={}
    channel_name["name"] = response["items"][0]["snippet"]["title"]
    channel_name["id"]=response["items"][0] ["id"]
    channel_name["description"]=response["items"][0] ["snippet"]["description"]
    channel_name["subscriberCount"]=response["items"][0] ["statistics"]["subscriberCount"]
    channel_name["videoCount"]=response["items"][0] ["statistics"]["videoCount"]
    channel_name["viewsCount"] = response["items"][0]["statistics"]["viewCount"]
    return {response["items"][0]["snippet"]["title"]:[channel_name]}
# ---------------------------------------------------------------------------------------------------------------------------
#get playlist playlist details
def get_playlist(youtube,id):
    playlit=[]
    playlistname={}
    id=id
    requestp = youtube.playlists().list(part="snippet,contentDetails",channelId=id,maxResults=50)
    responsep = requestp.execute()
    for x in responsep["items"]:
        id=x["id"]
        name = x["snippet"]["title"]
        playlit.append(id)
        playlistname.update({id : name})

    return playlit,playlistname
#----------------------------------------------------------------------------------------------------------------------------------
# getting videoid of the specific playlist
def get_video_list(youtube,playlist_id):
    videoid=[]
    request = youtube.playlistItems().list(
        part="snippet",
        maxResults=100,
        playlistId=playlist_id,

    )
    response = request.execute()


    for x in response["items"]:
        videoid.append(x["snippet"]["resourceId"]["videoId"])
    return videoid
#-----------------------------------------------------------------------------------------------------------
# getting videodetails
def getvideo_details(youtube,videoid):
    id=videoid
    videodetails={}
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=id
    )
    response = request.execute()
    for x in response["items"]:
        try:
            videodetails["Video_Id"] =x["id"]
        except:
            videodetails["Video_Id"] = x["id"]
        try:
            videodetails["Video_Name"] =x["snippet"]["title"]
        except:
            videodetails["Video_Name"] = x["snippet"]["title"]
        try:
            videodetails["Video_Description"] =x["snippet"]["description"]
        except:
            videodetails["Video_Description"] = x["snippet"]["description"]
        try:
            videodetails["thumbnail"] = x["snippet"]["thumbnails"]["default"]["url"]
        except:
            videodetails["thumbnail"] = x["snippet"]["thumbnails"]["default"]["url"]
        try:
            videodetails["Tags"] =x["snippet"]["tags"]
        except:
            videodetails["Tags"] =None
        videodetails["PublishedAt"] =x["snippet"]["publishedAt"]
        try:
            videodetails["View_Count"] =x["statistics"]["viewCount"]
        except:
            videodetails["View_Count"] =None
        videodetails["Like_Count"] =x["statistics"]["likeCount"]
        videodetails["Favorite_Count"] =x["statistics"]["favoriteCount"]
        videodetails["Comment_Count"] =x["statistics"]["commentCount"]
        videodetails["Duration"] =x["contentDetails"]["duration"]
        videodetails["Caption_Status"] = x["contentDetails"]["caption"]
    return videodetails
#-----------------------------------------------------------------------------------------------------------------------------------
#to get comment details
def get_comment_details(youtube,videoid):
    id=videoid
    comments=[]
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=id,maxResults=100)
        while request:
            response = request.execute()

            for x in response["items"]:
                data= dict(Comment_Id=x["snippet"]["topLevelComment"]["id"],
                Comment_Text= x["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                Comment_Author=x["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                Comment_PublishedAt=x["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comments.append(data)

            request = youtube.commentThreads().list_next(request, response)
    except:
        comments=None
    return(comments)




#------------------------------------------------------------------------------------------------------------------
#combining all the data
def combining_data(youtube,channelname):
    channelnameid = channelname
    result={}
    videolist=[]
    channel_details = get_channel_details(youtube, channelnameid)#dictionary
    result=channel_details
    key=list(channel_details.keys())
    playlist = {"playlist" :get_playlist(youtube,channelnameid)}#list
    result.update(playlist)
    y=1
    for i in playlist["playlist"][0]:
        x = get_video_list(youtube,i)

        for x in x:

            data=getvideo_details(youtube,x)
            data.update({"playlist_id":i})
            data.update({"Comments":get_comment_details(youtube,x)})
            srt=f'video{y}'
            result.update({srt:data})
            y=y+1
            break
    return result,key[0]



#----------------------------------------------------------------------------------------------------------------------------
# updating into mongodb
#ping mongodb
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://username:password@cluster0.bxhjcc8.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

def update_in_mongodb(data):
    try:
        client = MongoClient(
            "mongodb+srv://username:password@cluster0.bxhjcc8.mongodb.net/?retryWrites=true&w=majority")
        db = client.youtube
        records = db.records
        records.insert_one(data)
        st.success("Channel Data Has Got Succesfully", icon="✅")
    except DuplicateKeyError:
        st.error("Already exist")


#--------------------------------------------------------------------------------------------------------------------------
# fetching data from mongodb
def fetch_data_from_mongodb(id):
    client = MongoClient("mongodb+srv://username:password@cluster0.bxhjcc8.mongodb.net/?retryWrites=true&w=majority")
    db = client.youtube
    records = db.records
    out = records.find({"_id": id})
    return out
#-------------------------------------------------------------------------------------------------------
# connectin gto MySQL

import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pw",
    database='youtube'

)
mycursor = mydb.cursor(buffered=True)
#--------------------------------------------------------------------------------------------------------------------
# #storing data to sql table
def convert_df(channel_name):
    id = channel_name
    cx = fetch_data_from_mongodb(id)
    for x in cx:
       d = x

    channel_data = pd.DataFrame(d[id])
    channel_data["subscriberCount"] = pd.to_numeric(channel_data["subscriberCount"])
    channel_data["videoCount"] = pd.to_numeric(channel_data["videoCount"])
    channel_data["viewsCount"] = pd.to_numeric(channel_data["viewsCount"])
    # playlisttable
    playlist_data = d["playlist"][1]
    # video data
    return channel_data, playlist_data


def convert_df_video(channel_name):
    id = channel_name
    cx = fetch_data_from_mongodb(id)
    for x in cx:
        d = x
    count=1
    for x in range(len(d)-3):
        insert_into_video_table(d[f'video{count}'])
        count=count+1


def insert_into_video_table(v):
    import mysql.connector
    d = v
    if d['Video_Id']:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="pw",
            database='youtube'

        )
        mycursor = mydb.cursor(buffered=True)
        video_id = d['Video_Id']
        video_name = d['Video_Name']
        playlist_id = d['playlist_id']
        video_des = d['Video_Description']
        published_date =datetime.strptime(d['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        try:
            view_count = int(d['View_Count'])
        except:
            view_count = 0
        like_count = int(d['Like_Count'])
        fav_count = int(d['Favorite_Count'])
        comment_count = int(d['Comment_Count'])
        duration = int(round((isodate.parse_duration(d['Duration']).total_seconds())))
        thumbnail = d['thumbnail']
        caption_status = d['Caption_Status']
        data=(video_id, video_name, playlist_id, video_des, published_date, view_count, like_count,
             fav_count, comment_count, duration, thumbnail, caption_status)

        video_query = "insert into table_videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.execute(video_query , data)
        mydb.commit()

        # Close the cursor and the connection
        mycursor.close()
        mydb.close()



def update_into_sql(c,p):
    import mysql.connector

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",
        database='youtube'

    )
    mycursor = mydb.cursor(buffered=True)

    df = c
    # channel_query = "insert into channel values(%s,%s,%s,%s,%s)"
    name = df.loc[0, "name"]
    idx = df.loc[0, "id"]
    views = int(df.loc[0, "viewsCount"])
    des = df.loc[0, "description"]
    video = int(df.loc[0, "videoCount"])
    channel_query = f'insert into channel (channel_id, channel_name, channel_views, channel_des, videos_count) values (%s,%s,%s,%s,%s)'
    data = (idx, name, views, des, video)
    mycursor.execute(channel_query, data)

    #updating playlist
    play=p
    playlist_query = f'insert into table_playlist (playlist_id, playlist_name, channel_id) values (%s,%s,%s)'
    for k, v in play.items():
        data1 = (k, v, idx)
        mycursor.execute(playlist_query, data1)

    #updating video details
    mydb.commit()
    # Close the cursor and the connection
    mycursor.close()
    mydb.close()






#main code
#importing Srramlit
import streamlit as st
# Page title and introduction
st.title("Welcome to YouTube Data Harvesting Project")
st.write("This is a Streamlit-powered welcome page for our YouTube data harvesting project.")
options = st.selectbox("",("Fetch data from youtube and load into mongodb","Load data into SQL Table","Analysis"))
ids=[]
keys=[]
if options == "Fetch data from youtube and load into mongodb":
    id = st.text_input("Enter the id","")
    if len(id) == 24:
        if st.button("CLick to Fetch and store"):
            data, key = combining_data(youtube, id)
            data.update({"_id": key})
            update_in_mongodb(data)
            ids.append(id)
            keys.append(key)
    else:
        st.error("Enter the valid id")
    st.write(ids)
    st.write(keys)

elif options == "Load data into SQL Table":
    id = st.text_input("Enter the id", ""   )
    if st.button("CLick to Upload"):
        try:
            c, p = convert_df(id)
            update_into_sql(c,p)
            convert_df_video(id)
            st.success("Channel Data uploaded to mySql Succesfully", icon="✅")
        except:
            st.success("Channel Data uploaded to mySql Succesfully", icon="✅")

elif options =="Analysis":
    options = st.selectbox("",
                           ("1.What are the names of all the videos and their corresponding channels?",

                            "2.Which channels have the most number of videos, and how many videos do they have?",

                            "3.What are the top 10 most viewed videos and their respective channels?",

                            "4.How many comments were made on each video, and what are their corresponding video names?",

                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",

                            "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",

                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",

                            "8.What are the names of all the channels that have published videos in the year 2022?",

                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",

                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",
        database='youtube'

    )
    mycursor = mydb.cursor(buffered=True)
    if options=="1.What are the names of all the videos and their corresponding channels?":
        query1 = "select video_name,channel_name from table_combained"
        mycursor.execute(query1)
        result=mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Video Name']))
        mycursor.close()
        mydb.close()
    elif options=="2.Which channels have the most number of videos, and how many videos do they have?":
        query2 = "SELECT channel_name,(videos_count)  FROM youtube.channel order by channel_views desc limit 1"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Video Count']))
        mycursor.close()
        mydb.close()
    elif options=="3.What are the top 10 most viewed videos and their respective channels?":
        query2 = "SELECT channel_name,channel_views,video_name FROM youtube.table_combained  order by view_count desc limit 10"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'channel_views','video_name']))
        mycursor.close()
        mydb.close()
    elif options=="4.How many comments were made on each video, and what are their corresponding video names?":
        query2 = "SELECT video_name,comment_count FROM youtube.table_combained  order by comment_count"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['video_name', 'comment_count']))
        mycursor.close()
        mydb.close()
    elif options=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query2 = "SELECT channel_name,video_name,like_count FROM  youtube.table_combained order by  like_count desc limit 1"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Video Name','like_count']))
        mycursor.close()
        mydb.close()
    elif options=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query2 = "select channel_name,sum(like_count) as likecount from youtube.table_combained group by channel_name order by likecount"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'likecount']))
        mycursor.close()
        mydb.close()
    elif options=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query2 = "select channel_name,channel_views from youtube.channel order by channel_views"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'channel_views']))
        mycursor.close()
        mydb.close()
    elif options=="8.What are the names of all the channels that have published videos in the year 2022?":
        query2 = "select distinct channel_name,year(published_date) as year22 from youtube.table_combained where year(published_date) = 2022"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Year']))
        mycursor.close()
        mydb.close()
    elif options=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query2 = "select channel_name,avg(duration)as average  from  youtube.table_combained group by(channel_name)"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Average']))
        mycursor.close()
        mydb.close()
    elif options=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query2 = "select channel_name,video_name from youtube.table_combained  order by(comment_count) desc limit 1"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        st.write(pd.DataFrame(result,columns=['Channel Name', 'Video Name']))
        mycursor.close()
        mydb.close()


if st.button("Click to view channels"):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",
        database='youtube'

    )
    mycursor = mydb.cursor(buffered=True)

    query1 = "select channel_name from channel"
    mycursor.execute(query1)
    result = mycursor.fetchall()
    st.write(pd.DataFrame(result, columns=['Channel Name']))
    mycursor.close()
    mydb.close()
