import psycopg2
import pandas as pd

host = 'localhost'
dbname = 'mochache_demo'
user = 'postgres'
password = '****'
port = 5432
conn = None
cur = None

csv_file = '/home/mocha/projects/postgres/data/archive/USvideos.csv'

try:
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )

    cur = conn.cursor()
    print("Connection successful")

    
    create_script = ''' 
    CREATE TABLE IF NOT EXISTS youtube_trending (
        video_id TEXT PRIMARY KEY,
        trending_date DATE,
        title TEXT,
        channel_title TEXT,
        category_id INT,
        publish_time TIMESTAMP,
        tags TEXT,
        views BIGINT,
        likes BIGINT,
        dislikes BIGINT,
        comment_count BIGINT,
        thumbnail_link TEXT,
        comments_disabled BOOLEAN,
        ratings_disabled BOOLEAN,
        video_error_or_removed BOOLEAN,
        description TEXT
    );
    '''
    cur.execute(create_script)

    # load CSV
    df = pd.read_csv(csv_file)

    insert_script = '''
    INSERT INTO youtube_trending (
        video_id, trending_date, title, channel_title, category_id,
        publish_time, tags, views, likes, dislikes, comment_count,
        thumbnail_link, comments_disabled, ratings_disabled, 
        video_error_or_removed, description
    )
    VALUES (%s, to_date(%s, 'YY.DD.MM'), %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s)
    ON CONFLICT (video_id) DO NOTHING;
    '''

    # insert rows
    for _, row in df.iterrows():
        insert_values = (
            row['video_id'],
            row['trending_date'],
            row['title'],
            row['channel_title'],
            row['category_id'],
            row['publish_time'],
            row['tags'],
            row['views'],
            row['likes'],
            row['dislikes'],
            row['comment_count'],
            row['thumbnail_link'],
            row['comments_disabled'],
            row['ratings_disabled'],
            row['video_error_or_removed'],
            row['description']
        )
        cur.execute(insert_script, insert_values)

    conn.commit()
    print("Table populated successfully!")

except Exception as error:
    print("Error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
