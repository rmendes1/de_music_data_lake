# Banco de Dados
import os
from pathlib import Path
import dotenv

env_path = Path(__file__).parent / '.env'
dotenv.load_dotenv(env_path)

DB_URL = os.getenv("DB_URL")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPICS_TABLES = {
    "postgres.public.tracks": "tracks",
    "postgres.public.albums": "albums",
    "postgres.public.artists": "artists",
    "postgres.public.genres": "genres",
    "postgres.public.albums_genres": "albums_genres"
}

# Google Cloud Storage
GCS_BUCKET = os.getenv("GCS_BUCKET")
