from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import open_ai_config
import config as config
import requests as req
import psycopg2 as pg
import pandas as pd
import sshtunnel
import datetime
import time

def connect_database(creds):
    
    print("Create SSH Tunnel")
    ssh_tunnel = SSHTunnelForwarder(
                                      creds['ssh_host'],
                                      ssh_username = creds['ssh_username'],
                                      ssh_private_key = creds['ssh_key_path'] ,
                                      remote_bind_address = (creds['host'], creds['port'])
                                   )
    ssh_tunnel.start()
    print("SSH Tunnel Created")
    
    print("Connect Postgres")
    connection = pg.connect(
                               host = "localhost",
                               port = ssh_tunnel.local_bind_port,
                               user = creds['username'],
                               password = creds['password'],
                               database = creds['database']
                            )
    connection.autocommit=True
    print("Postgres Connected")
    
    local_port = ssh_tunnel.local_bind_port
    db_url = f"postgresql://{creds['username']}:{creds['password']}@localhost:{local_port}/{creds['database']}"
    engine = create_engine(db_url)
    
    return connection, engine


def get_open_ai_response(url, prompt, headers):
    
    # Create a data payload
    data = {
                "model": "gpt-3.5-turbo",
                "messages": [
                                {
                                    "role": "user", 
                                    "content": prompt
                                }
                            ],
                "temperature" : 0.5, 
            }

    # Send the API request
    response = req.post(url, json=data, headers=headers, timeout=20)
    
    res = response.json()
    res_body = {
                'reply' : res['choices'][0]['message']['content'],
                'input_tokens' : res['usage']['prompt_tokens'] ,
                'output_tokens' : res['usage']['completion_tokens'],
                'tokens_used' : res['usage']['total_tokens']
                }
    
    return res_body

try:
    db_creds = {
                'host' : config.host,
                'port' : config.port,
                'username' : config.username,
                'password' : config.password,
                'database' : config.database,
                'ssh_host' : config.ssh_host,
                'ssh_username' : config.ssh_username,
                'ssh_key_path' : config.ssh_key_path
                }

    conn, db_engine = connect_database(db_creds)

    #Open-AI configs
    open_ai_api_key = open_ai_config.api_key
    open_ai_url = "https://api.openai.com/v1/chat/completions"

    # Set headers with your API key
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {open_ai_api_key}",
    }

    #Query to fetch 100 records
    query = '''
            Select 
                id, 
                book_name, 
                chapter, 
                verse_number 
            from 
                video.input 
            where
                description is null
            order by 
                id 
            limit 
                50
            '''

    video_count = open_ai_config.video_count
    update_data = []
    count = 0

    while (count < video_count):
        df = pd.read_sql(query,con=conn)
        update_data = []

        for i in range(len(df)):
            actual_values = {
                                'book_name' : df.iloc[i]['book_name'], 
                                'verse_number' : df.iloc[i]['verse_number'], 
                                'chapter' : df.iloc[i]['chapter']
                            }
            try:
                description_prompt = open_ai_config.description.format(**actual_values)
                tags_prompt = open_ai_config.tags.format(**actual_values)

                description = get_open_ai_response(open_ai_url, description_prompt, headers)
                time.sleep(2)
                tags = get_open_ai_response(open_ai_url, tags_prompt, headers)

                tags = ("#" + tags['reply'].replace(", ", " #") ) if ", " in tags['reply'] else tags['reply']

                temp_data = {
                                'id' : df.iloc[i]['id'], 
                                'description' : description['reply'].replace('"',''),
                                'tags' : tags
                              }
                print(count, temp_data['id'])
                update_data.append(temp_data)
                count = count + 1

            except Exception as open_ai_error:
                print("Error occured while sending OpenAI request -", open_ai_error)
                try:
                    tags = get_open_ai_response(open_ai_url, tags_prompt, headers)

                    tags = ("#" + tags['reply'].replace(", ", " #") ) if ", " in tags['reply'] else tags['reply']

                    temp_data = {
                                    'id' : df.iloc[i]['id'], 
                                    'description' : description['reply'].replace('"',''),
                                    'tags' : tags
                                  }
                    print(count, temp_data['id'])
                    update_data.append(temp_data)
                    count = count + 1
                except Exception as open_ai_error_2:
                    print("Second Error occured while sending OpenAI request -", open_ai_error_2)
                    
            if count == video_count:
                break;

        update_data_df = pd.DataFrame(update_data)

        if(~update_data_df.empty):
            temp_table_name = 'temp_table'
            try:
                update_data_df.to_sql(temp_table_name, db_engine, if_exists="replace", index=False, schema='public')

                # Perform the bulk update using an UPDATE query with a JOIN
                update_query = """
                    UPDATE 
                        video.input m
                    SET 
                        description = t.description, 
                        tags = t.tags
                    from
                        temp_table AS t 
                    where 
                        m.id = t.id;
                """

                cursor = conn.cursor()
                cursor.execute(update_query)
                cursor.execute("DROP TABLE IF EXISTS temp_table")
            except Exception as db_error:
                print('Error Occured while updating in DB ',db_error)

except Exception as script_error:
    print('Error Occured in Script ', script_error)
