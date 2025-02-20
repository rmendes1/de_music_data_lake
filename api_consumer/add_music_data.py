import deezer
import psycopg2
import time
from psycopg2 import sql
import sys

# Database configuration
DB_CONFIG = {
    "dbname": "music_data",
    "user": "",
    "password": "",
    "host": "",
    "port": ""
}

# Initialize the Deezer API client
client = deezer.Client()


# Function to connect to the database
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        sys.exit(1)


# Function to insert or update data in the Artists table
def upsert_artist(conn, artist):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                INSERT INTO Artists (artist_id, artist_name, link, followers, photo)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id) DO UPDATE
                SET artist_name = EXCLUDED.artist_name,
                    link = EXCLUDED.link,
                    followers = EXCLUDED.followers,
                    photo = EXCLUDED.photo;
            """)
            cur.execute(query, (
                artist.id,
                artist.name,
                artist.link,
                artist.nb_fan,
                artist.picture
            ))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error upserting artist {artist.id}: {e}")
        conn.rollback()


# Function to insert or update data in the Albums table
def upsert_album(conn, album):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                INSERT INTO Albums (album_id, album_title, artist_id, release_date, link, cover)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (album_id) DO UPDATE
                SET album_title = EXCLUDED.album_title,
                    artist_id = EXCLUDED.artist_id,
                    release_date = EXCLUDED.release_date,
                    link = EXCLUDED.link,
                    cover = EXCLUDED.cover;
            """)
            cur.execute(query, (
                album.id,
                album.title,
                album.artist.id,
                album.release_date,
                album.link,
                album.cover
            ))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error upserting album {album.id}: {e}")
        conn.rollback()


# Function to insert or update data in the Tracks table
def upsert_track(conn, track):
    try:
        with conn.cursor() as cur:
            # Ensure the artist exists in the Artists table
            artist = track.artist  # Get the artist object from the track
            upsert_artist(conn, artist)  # Upsert the artist

            # Insert or update the track
            query = sql.SQL("""
                INSERT INTO Tracks (track_id, track_title, album_id, artist_id, duration, link, preview)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id) DO UPDATE
                SET track_title = EXCLUDED.track_title,
                    album_id = EXCLUDED.album_id,
                    artist_id = EXCLUDED.artist_id,
                    duration = EXCLUDED.duration,
                    link = EXCLUDED.link,
                    preview = EXCLUDED.preview;
            """)
            cur.execute(query, (
                track.id,
                track.title,
                track.album.id,
                track.artist.id,
                track.duration,
                track.link,
                track.preview
            ))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error upserting track {track.id}: {e}")
        conn.rollback()

# Function to insert or update data in the Genres table
def upsert_genre(conn, genre):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                INSERT INTO Genres (genre_id, genre_name, picture_link)
                VALUES (%s, %s, %s)
                ON CONFLICT (genre_id) DO UPDATE
                SET genre_name = EXCLUDED.genre_name,
                    picture_link = EXCLUDED.picture_link;
            """)
            cur.execute(query, (
                genre.id,
                genre.name,
                genre.picture
            ))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error upserting genre {genre.id}: {e}")
        conn.rollback()


# Function to associate genres with albums
def upsert_album_genres(conn, album_id, genres):
    try:
        with conn.cursor() as cur:
            # Remove old associations (optional, depending on use case)
            cur.execute("DELETE FROM Albums_Genres WHERE album_id = %s;", (album_id,))

            # Insert new associations
            for genre in genres:
                try:
                    # Ensure the genre exists in the Genres table
                    upsert_genre(conn, genre)

                    # Associate the genre with the album
                    cur.execute("""
                        INSERT INTO Albums_Genres (album_id, genre_id)
                        VALUES (%s, %s)
                        ON CONFLICT (album_id, genre_id) DO NOTHING;
                    """, (album_id, genre.id))
                except psycopg2.Error as e:
                    print(f"Error associating genre {genre.id} with album {album_id}: {e}")
                    conn.rollback()
                    continue  # Skip this genre and continue with the next one

            conn.commit()
    except psycopg2.Error as e:
        print(f"Error upserting genres for album {album_id}: {e}")
        conn.rollback()


# Main function to ingest data
def ingest_data(artist_id):
    conn = None
    try:
        conn = connect_db()

        artist = client.get_artist(artist_id)
        upsert_artist(conn, artist)

        # Fetch the artist's albums
        albums = artist.get_albums()
        for album in albums:
            upsert_album(conn, album)

            # Fetch the album's tracks
            tracks = album.get_tracks()
            for track in tracks:
                upsert_track(conn, track)

            # Fetch the album's genres
            genres = album.genres
            upsert_album_genres(conn, album.id, genres)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
