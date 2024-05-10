import os
from googleapiclient.discovery import build
from google.oauth2 import credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
from sshtunnel import SSHTunnelForwarder
import psycopg2 as pg
import pandas as pd
import sshtunnel
import datetime
import botocore
import config
import boto3
import re

def authorize_credentials(scopes):
    # Load credentials from the JSON file
    creds = None
    token_path = config.youtube_token_path
    if os.path.exists(token_path):
        creds = credentials.Credentials.from_authorized_user_file(token_path, scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds

def get_video_title(verse_text, sub_verse_text):
    
    # Calculate the available space for combining the texts
    available_space = 100 - len(sub_verse_text) - 1  # Subtract 1 for the space

    # Check if there's enough space to include the entire verse_text
    if available_space >= len(verse_text):
        final_text = sub_verse_text + " " + verse_text
    else:
        last_word_index = available_space
        while last_word_index > 0 and verse_text[last_word_index] != ' ':
            last_word_index -= 1

        truncated_verse_text = verse_text[:last_word_index]
        final_text = sub_verse_text + " " + truncated_verse_text

    return final_text


def limit_tag_length(tag_string, max_length=500):
    tags = tag_string.split(' ')
    limited_tags = []
    current_length = 0

    for tag in tags:
        if current_length + len(tag) + 1 <= max_length:
            limited_tags.append(tag)
            current_length += len(tag) + 1
        else:
            break

    return limited_tags


host = config.host
port = config.port
username = config.username
password = config.password
database = config.database
ssh_host = config.ssh_host
ssh_username = config.ssh_username
ssh_key_path = config.ssh_key_path

current_date = datetime.datetime.now().date().strftime("%Y-%m-%d")
try:
    print("Create SSH Tunnel")
    ssh_tunnel = SSHTunnelForwarder(
                    ssh_host,
                    ssh_username=ssh_username,
                    ssh_private_key= ssh_key_path,
                    remote_bind_address=(host, port)
                )
    ssh_tunnel.start()
    print("SSH Tunnel Created")

    print("Connect Postgres")
    conn = pg.connect(
                   host="localhost",
                   port=ssh_tunnel.local_bind_port,
                   user=username,
                   password= password,
                   database=database
                )
    print("Postgres Connected")
    cursor = conn.cursor()

    query = '''
            with load_videos as
		(
			Select
				*
			    from
			    	vid001.public.load_channel
			    where
			    	upload_status in ('NOT_UPLOADED','FAILED')
				and upload_retry_count < 3
			    order by
			    	record_insert_timestamp
			    limit 100
		)
		select
			load.*,
			inp.verse_text verse_text,
			concat(inp.book_name, ' ', inp.chapter, ':', inp.verse_number) sub_verse_text ,
			inp.description description,
			inp.tags tags
		from
			vid001.public.video_input_source inp
		join
			load_videos load
		on
			inp.load_channel_id = load.id
		'''
    print("Fetch Records to be Processed")
    df = pd.read_sql(query,con=conn)

    print("Input Records Records Fetched")

    print("Create AWS Session")
    scopes = ['https://www.googleapis.com/auth/youtube.upload']
    aws_session = boto3.Session(
                                aws_access_key_id = config.aws_access_key_id,
                                aws_secret_access_key = config.aws_secret_access_key)

    bucket_name = config.bucket_name
    key = config.key
    s3_client = aws_session.client('s3')
    print("AWS Session created")

    # Authorize the credentials
    print("Read YouTube Credentials")
    creds = authorize_credentials(scopes)

    # Create the YouTube Data API client
    youtube = build('youtube', 'v3', credentials=creds)
    print("YouTube Object Created")

    for i in range(len(df)):
        file_name = df.iloc[i]['s3_upload_video_path'].split("/")[-1]
        tags = limit_tag_length(df.iloc[i]['tags'])

        print("Download and Upload ",file_name)
        
        output_video_path = config.output_video_path + file_name
        
        print("Downloading ", file_name, " from s3 to Local")
        
        title = get_video_title(df.iloc[i]['verse_text'], df.iloc[i]['sub_verse_text'])
        try:
            response = s3_client.download_file(bucket_name, key + file_name, output_video_path)
                
            print("Downloaded ", file_name)

            # Set video metadata
            request = youtube.videos().insert(
                part='snippet,status',
                body={
                    'snippet': {
                        'title' : title,
                        'description' : df.iloc[i]['description'],
			'tags' : tags
                        },
                    'status': {
                        'privacyStatus': 'public'  # Set as private, public, or unlisted
                    }
                },
            # Set the path to the video file
            media_body = MediaFileUpload(output_video_path)
            )
            try:
                # Execute the API request to upload the video
                print("Try YouTube Execute Start",request)
                response = request.execute()
                print(response,'\n')
                
                # Get the uploaded video's ID
                video_id = response['id']
        #        response = {'kind': 'youtube#video', 'etag': 'yYd2qQVt-i7Keh7euQPWkz8tMAQ', 'id': 'Xpo7ol7mgVk', 'snippet': {'publishedAt': '2023-07-19T13:37:07Z', 'channelId': 'UCrc4aQ1yTedgOp-hWE5NrqA', 'title': 'Your video title', 'description': 'Your video description', 'thumbnails': {'default': {'url': 'https://i9.ytimg.com/vi/Xpo7ol7mgVk/default.jpg?sqp=CITN36UG&rs=AOn4CLC8dGEgNkiGUwzvsxpViemL6V-P8w', 'width': 120, 'height': 90}, 'medium': {'url': 'https://i9.ytimg.com/vi/Xpo7ol7mgVk/mqdefault.jpg?sqp=CITN36UG&rs=AOn4CLAipGCX9AbY7NSpFsAvaJ2KV7VqNg', 'width': 320, 'height': 180}, 'high': {'url': 'https://i9.ytimg.com/vi/Xpo7ol7mgVk/hqdefault.jpg?sqp=CITN36UG&rs=AOn4CLDFIZjvbCp8U_XNijT2DFtUQ1xc4w', 'width': 480, 'height': 360}}, 'channelTitle': 'Akash Gupta', 'categoryId': '22', 'liveBroadcastContent': 'none', 'localized': {'title': 'Your video title', 'description': 'Your video description'}}, 'status': {'uploadStatus': 'uploaded', 'privacyStatus': 'private', 'license': 'youtube', 'embeddable': True, 'publicStatsViewable': True}} 

                print(f'{file_name} uploaded successfully! ID: {video_id}')    

                update_load_channel_query = f'''
                                                update vid001.public.load_channel
                                                set 
                                                    upload_status = 'UPLOADED' ,
                                                    upload_success_date = '{datetime.datetime.now()}'
                                                where 
                                                    id = '{df.iloc[i]['id']}'
                                            '''
            except Exception as upload_error:
                print("Exception YouTube Upload Enetered")
                upload_error = re.sub(r'[^a-zA-Z0-9\s]', '-', str(upload_error))
                reason = "YouTube Upload Failed " + upload_error
                print('Video Upload Failed ', reason)
                
                update_load_channel_query = f'''
                                            update vid001.public.load_channel
                                            set 
                                                upload_status = 'FAILED' ,
                                                upload_error = '{reason}',
                                                upload_retry_count = upload_retry_count + 1
                                            where 
                                                id = '{df.iloc[i]['id']}'
                                        '''
                
        except botocore.exceptions.ClientError as e:
            print(file_name, " Download Failed to Local")       
            if e.response['Error']['Code'] == "404":
                reason = "Download Failed to Local because the object does not exist."
                print(reason)
            else:
                reason = "Download Failed to Local - Unknown Reason"
            update_load_channel_query = f'''
                                        update vid001.public.load_channel
                                        set 
                                            upload_status = 'FAILED' ,
                                            upload_error = '{reason}',
                                            upload_retry_count = upload_retry_count + 1
                                        where 
                                            id = '{df.iloc[i]['id']}'
                                    '''
        if os.path.exists(output_video_path):
            os.remove(output_video_path)
            print(f"File '{output_video_path}' deleted successfully.")
        else:
            print(f"File '{output_video_path}' does not exist.")
            
        print("Update upload_status in DB")
        try:
            cursor = conn.cursor()
            cursor.execute(update_load_channel_query)
            cursor.execute("COMMIT")
            print("upload_status Updated")
        except Exception as update_db_error:
            print("Error updating status of ", df.iloc[i]['id'], " in DB - ", update_db_error)
            print("Update Query - ", update_load_channel_query) 

except Exception as e:
    print("Unexpected Error Occured - ",e)
finally:
    cursor.close()
    conn.close()
    ssh_tunnel.close()

