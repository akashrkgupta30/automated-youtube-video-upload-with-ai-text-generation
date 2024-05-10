from PIL import ImageFont, ImageDraw, Image
from sshtunnel import SSHTunnelForwarder
import psycopg2 as pg
import pandas as pd
import subprocess
import sshtunnel
import datetime
import textwrap
import random
import boto3
import json
import uuid
import os
import re
import config as config
import traceback

def get_text_size(text, font_name, font_size):
    font = ImageFont.truetype(font_name, font_size)
    image = Image.new("RGB", (1,1), "white")
    draw = ImageDraw.Draw(image)
    text_width, text_height = draw.textsize(text, font=font)
    return [text_width, text_height]
    
def split_paragraph(paragraph):
    midpoint = len(paragraph) // 2
    last_period_before_midpoint = paragraph.rfind('.', 0, midpoint)

    if last_period_before_midpoint != -1:
        part1 = paragraph[:last_period_before_midpoint + 1].strip()
        part2 = paragraph[last_period_before_midpoint + 1:].strip()
        return [part1, part2]
    else:
        return [paragraph[:midpoint].strip(), paragraph[midpoint:].strip()]

print("Read Config Params")
host = config.host
port = config.port
username = config.username
password = config.password
database = config.database
ssh_host = config.ssh_host
ssh_username = config.ssh_username
ssh_key_path = config.ssh_key_path
video_count = config.video_count
count = 0
video_config_path = config.video_config_path
print("Config Params Read")

current_date = datetime.datetime.now().date().strftime("%Y-%m-%d")

try:
    
    #Connecting to Database
    print("Create SSH Tunnel-",datetime.datetime.now())
    ssh_tunnel = SSHTunnelForwarder(
                    ssh_host,
                    ssh_username=ssh_username,
                    ssh_private_key= ssh_key_path,
                    remote_bind_address=(host, port)
                )
    ssh_tunnel.start()
    print("SSH Tunnel Created-",datetime.datetime.now())

    print("Connect Postgres")
    conn = pg.connect(
                   host="localhost",
                   port=ssh_tunnel.local_bind_port,
                   user=username,
                   password= password,
                   database=database
                )
    print("Postgres Connected-",datetime.datetime.now())

    #Connecting to AWS
    print("Create AWS Session")
    aws_session = boto3.Session(
                                aws_access_key_id = config.aws_access_key_id,
                                aws_secret_access_key = config.aws_secret_access_key
                                )

    s3 = aws_session.client('s3')
    bucket_name = config.bucket_name
    key = config.key
    print("AWS Session created")
    
    object_url = '<s3 bucket path>' + '/Input/Movie/long/'
    video_name = ['desert_long.mp4','sun_long.mp4','land_long.mp4','tree_long.mp4','water_long.mp4']
    
    input_videos = [ object_url + video for video in video_name]
                             
    while (count < video_count):
        
        #Query to fetch 50 records
        query = '''
                Select * 
                from vid001.public.video_input_source 
                where creation_status in ('NOT_CREATED','FAILED')
                      and retry_count < 3
                order by id 
                limit 1
                '''
        print("Fetch Records to be Processed")
        df = pd.read_sql(query,con=conn)
               
        
        if df.empty:
            break;
        
        print("Input Records Records Fetched")


        #Casting Chapter and verse number as string
        df['chapter'] = df['chapter'].astype(int).astype(str)
        df['verse_number'] = df['verse_number'].astype(int).astype(str)

        #Generating output file name and sub verse text
        df['output_file_name'] = df[['book_name','chapter','verse_number']].apply(lambda x: '_'.join(x), axis=1)
        df['verse_sub_text'] = r'\"' + df['book_name'] + r'\" ' + df['chapter'] + r'\:' + df['verse_number']


        #Query to update creation_status = 'IN_PROGRESS'
        li = df['id'].astype(str).to_list()
        update_ids = "','".join(li)

        update_query = f'''
                            update vid001.public.video_input_source
                            set creation_status = 'IN_PROGRESS' 
                            where id in ('{update_ids}');
        '''

        print("Update creation_status to IN_PROGRESS in DB")
        cursor = conn.cursor()
        cursor.execute(update_query)

        cursor.execute("COMMIT")
        print("creation_status Updated")

        with open(video_config_path) as config_file:
            video_config = json.load(config_file)
        print("Video Config Read")


        print("Initialse Video Config Params")
        verse_config_path = video_config['scenes'][0]['elements'][0]['verse_text']
        verse_sub_config_path = video_config['scenes'][0]['elements'][0]['verse_sub_text']

        verse_box_color = verse_config_path['settings']['boxcolor']
        verse_font = verse_config_path['settings']['font']
        verse_font_path = verse_config_path['settings']['fontfile']
        verse_font_size = verse_config_path['settings']['fontsize']
        verse_font_color = verse_config_path['settings']['fontcolor']
        verse_shadowx = verse_config_path['settings']['shadowx']
        verse_shadowy = verse_config_path['settings']['shadowy']
        verse_line_spacing = verse_config_path['settings']['line_spacing']


        verse_sub_font = verse_sub_config_path['settings']['font']
        verse_sub_font_size = verse_sub_config_path['settings']['fontsize']
        verse_sub_font_color = verse_sub_config_path['settings']['fontcolor']
        verse_sub_shadowx = verse_sub_config_path['settings']['shadowx']
        verse_sub_shadowy = verse_sub_config_path['settings']['shadowy']
        verse_sub_x_pos = verse_sub_config_path['x']
        verse_sub_y_pos = verse_sub_config_path['y']

        resolution = video_config['width'] + '*' + video_config['height']

        print("Video Config Params Initialised")


        #Generating output video for each input video 
        print("Start Generating Output Video-", datetime.datetime.now())
        all_line_count = []
        all_width = []
        alpha_check = ('b','d','f','h','i','j','k','l')                                   
        
        for i in range(len(df)):
            try:
                # Path to the input video
                print('Iteration -  ',i)
                # input_video = re.sub(r'\.[^.]+$', '.mp4', df.iloc[i]['s3_input_video_path'])
                input_video = random.choice(input_videos)
                
                print("Input Video Read")
                # Path to the output video
                output_file_name = df.iloc[i]['output_file_name'] + "_long.mp4"
                output_video_path = config.output_video_path + output_file_name 
                
                # Text to be added in the video
                print("Format Verse Text")
                verse_text = df.iloc[i]['verse_text']
                box_width = 60
                max_width = 0
                input_str = re.sub(r':', r'\\:', verse_text)   
                input_str = input_str.replace("'", "'\\\\\\''")
                
                wrapped_lines = textwrap.wrap(input_str, width=box_width)
                
                all_line_count.append(len(wrapped_lines))                             
                line_count = len(wrapped_lines) + 2.75
                drawtext_filter =""
                
                for k,line in enumerate(wrapped_lines):
                    vertical_start = ( ( k - (line_count/2) ) * verse_font_size ) + (k*20)
                    height = "+" + str(vertical_start) if vertical_start >= 0 else str(vertical_start)
                    center_x = '(w-text_w)/2'
                    center_y = '(h-text_h)/2 ' + height 
                    drawtext_filter += f"drawtext=text='{line}':fontfile={verse_font_path}:fontsize={verse_font_size}:fontcolor={verse_font_color}:x={center_x}:y={center_y}:shadowx={verse_shadowx}:shadowy={verse_shadowy}:enable='between(t,0,9.8)',"
                    drawtext_filter += f"drawtext=text='{line}':fontfile={verse_font_path}:fontsize={verse_font_size}:fontcolor={verse_font_color}:x={center_x}:y={center_y}:shadowx={verse_shadowx}:shadowy={verse_shadowy}:enable='between(t,70.2,101.5)',"

                    width = get_text_size(line, verse_font_path, verse_font_size)
                    if(width[0] > max_width):
                        max_width = width[0]
                 
                all_width.append(max_width)
                y = get_text_size(wrapped_lines[0], verse_font_path, verse_font_size)[1]
                
                verse_sub_text = df.iloc[i]['verse_sub_text']
                center_y = center_y + "+205"
                drawtext_filter += f"drawtext=text='{verse_sub_text}':x={verse_sub_x_pos}:y={center_y}:fontsize={verse_sub_font_size}:fontcolor={verse_sub_font_color}:fontfile={verse_font_path}:shadowx={verse_sub_shadowx}:shadowy={verse_sub_shadowy}:enable='between(t,0,9.8)',"
                drawtext_filter += f"drawtext=text='{verse_sub_text}':x={verse_sub_x_pos}:y={center_y}:fontsize={verse_sub_font_size}:fontcolor={verse_sub_font_color}:fontfile={verse_font_path}:shadowx={verse_sub_shadowx}:shadowy={verse_sub_shadowy}:enable='between(t,70.2,101.5)',"
                print("Verse Text Formatted and Verse Sub Text Read")

                description = split_paragraph(df.iloc[i]['description'])                           
                for p,desc in enumerate(description):
                    input_str = re.sub(r':', r'\\:', desc)   
                    input_str = input_str.replace("'", "'\\\\\\''")
                    word_count = len(input_str.split())
                    increment = 0
                    wrapped_lines = textwrap.wrap(input_str, width=box_width)
                    
                    all_line_count.append(len(wrapped_lines))
                    line_count = len(wrapped_lines) + 2.75
                    duration_per_word = ( 30 / word_count ) / 1.25

                    for k,line in enumerate(wrapped_lines):
                        vertical_start = ( ( k - (line_count/2) ) * verse_font_size ) + (k*20)
                        height = "+" + str(vertical_start) if vertical_start >= 0 else str(vertical_start)
                        line_width, line_height = get_text_size(line, verse_font_path, verse_font_size)
                        all_width.append(line_width)
                        center_y = f'(h-{line_height})/2 ' + height
                        new_width = 0
                        for word in line.split():
                            center_x = f'(w-{line_width})/2' + "+" + str(new_width)

                            if 't' in word and (not set(word).intersection(alpha_check) ) and word.islower():
                                padding = '+10'
                            elif ( (not set(word).intersection(alpha_check) ) and word.islower() ) or re.search(r'\d', word):
                                padding = '+15'                    
                            else:
                                padding='+0'                                                                
                                
                            start_time = 10 + (30 * p) + (duration_per_word * increment)
                            end_time = 10 + ( 30 * (2**p) )
                            drawtext_filter += f"drawtext=text='{word}':fontfile={verse_font_path}:fontsize={verse_font_size}:fontcolor={verse_font_color}:x={center_x}:y={center_y+padding}:shadowx={verse_shadowx}:shadowy={verse_shadowy}:enable='between(t,{start_time},{end_time})',"

                            width = get_text_size(word, verse_font_path, verse_font_size)
                            new_width = new_width + width[0] + 15
                            increment = increment + 1                
    
                max_width = max(all_width)
                line_count = max(all_line_count) + 2.75    
                                
                                
                bx = (1920-max_width)/2 -10
                by = ((1080-y)/2) - ( (line_count/2) * verse_font_size) - (10 if line_count%2==0 else 5)
                
                buffer_length = 215 if line_count%2==0 else 220 
                drawbox_filter = f"drawbox=x={bx}:y={by}:w={max_width+20}:h={((line_count-2.75)*(verse_font_size+15)) + buffer_length}:color={verse_box_color}:t=fill:enable='between(t,0,101.5)'"
                
                # FFmpeg command to add text to the video
                ffmpeg_cmd = [
                                "ffmpeg",
                                "-i", input_video,
                                "-vf", f'{drawbox_filter},{drawtext_filter.rstrip(",")}',
                                "-c:a",'copy',
                                "-s", resolution,
                               output_video_path
                            ]
                print("\n", ffmpeg_cmd ,"\n")
                
                #Run ffmpeg command
                print("Execute FFMPEG Command-",datetime.datetime.now())
                
                response = subprocess.run(ffmpeg_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                print("FFMPEG Command Executed-",datetime.datetime.now())
                print("\n", response,"\n")
                
                #Upload output video to s3 and update the creation_status to CREATED in database
                if response.returncode == 0:
                    print("Video Generated Successfully")
                    random_id = uuid.uuid4()
                    try:
                        s3.upload_file(output_video_path, bucket_name, key + "long/" + output_file_name)
                        print("Video Uplaoded to S3")                        
                        
                        s3_video_path = "s3://" + bucket_name + "/" + key + "long/" + output_file_name
                        insert_load_channel_query = f'''
                                                        insert into vid001.public.load_channel (id,upload_planned_date,channel,s3_upload_video_path, upload_status)
                                                        values ('{random_id}', '{current_date}', 'YouTube', '{s3_video_path}', 'NOT_UPLOADED')                                        
                                                    '''
                        cursor.execute(insert_load_channel_query)
                        cursor.execute("COMMIT")
                        print("New Record Inserted to Load Table")
                        
                        update_input_source_query = f'''
                                                        update vid001.public.video_input_source
                                                        set 
                                                            creation_status = 'CREATED' ,
                                                            creation_date = '{datetime.datetime.now()}',
                                                            s3_output_video_path = '{s3_video_path}',
                                                            load_channel_id = '{random_id}'
                                                        where 
                                                            id = {df.iloc[i]['id']}
                                                    '''
                        print("Input Table Updated")
                        if os.path.exists(output_video_path):
                            os.remove(output_video_path)
                            print(f"File '{output_video_path}' deleted successfully.")
                        else:
                            print(f"File '{output_video_path}' does not exist.")
                        
                        count = count + 1
                        
                    except Exception as s3_error:
                        s3_error = s3_error.replace("\r\n", " ")
                        cleaned_s3_error = re.sub(r'[^a-zA-Z0-9\s]', '-', s3_error)
                        cleaned_s3_error = "Error while uploading to S3 or inserting or updating tables - " + cleaned_s3_error
                        print(s3_error)
                        update_input_source_query = f'''
                                                update vid001.public.video_input_source
                                                set 
                                                    creation_status = 'FAILED',
                                                    retry_count = retry_count + 1,
                                                    error_log = '{cleaned_s3_error}'
                                                where 
                                                    id = {df.iloc[i]['id']}
                                            '''
                else:
                    print("Error Generating Video")
                    error_description = response.stderr.decode('utf-8')
                    error_description = error_description.replace("\r\n", " ")
                    cleaned_error = re.sub(r'[^a-zA-Z0-9\s]', '-', error_description)
                    
                    update_input_source_query = f'''
                                                update vid001.public.video_input_source
                                                set 
                                                    creation_status = 'FAILED',
                                                    retry_count = retry_count + 1,
                                                    error_log = '{cleaned_error}'
                                                where 
                                                    id = {df.iloc[i]['id']}
                                            '''
                    print("Input Table Updated with Error Received")
                
                cursor.execute(update_input_source_query)
                cursor.execute("COMMIT")
                print("Iteration - ",i," Completed-",datetime.datetime.now())
                
                if ( count == video_count ):
                    reupdate_ids = li[i+1:]
                    if len(reupdate_ids) > 0:
                        update_query = f'''
                                        update vid001.public.video_input_source
                                        set creation_status = 'NOT_CREATED' 
                                        where id in ('{"','".join(reupdate_ids)}');
                                    '''

                        print("Re-Update creation_status back to IN_PROGRESS in DB")
                        cursor.execute(update_query)
                        cursor.execute("COMMIT")
                    break;
                    
            except Exception as for_loop_error:
                for_loop_error = re.sub(r'[^a-zA-Z0-9\s]', '-', for_loop_error)
                cleaned_for_loop_error = "Error Occured while Video Processing in FOR loop - " + cleaned_for_loop_error                      
                print(cleaned_for_loop_error)
                update_input_source_query = f'''
                                                update vid001.public.video_input_source
                                                set 
                                                    creation_status = 'FAILED',
                                                    retry_count = retry_count + 1,
                                                    error_log = '{cleaned_for_loop_error}'
                                                where 
                                                    id = {df.iloc[i]['id']}
                                            '''
                cursor.execute(update_input_source_query)
                cursor.execute("COMMIT")
                
except Exception as e:
    print("Unexpected Error Occured : ", e)
    
    traceback.print_exc()

