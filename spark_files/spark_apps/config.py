# Banco de Dados
DB_URL = ''
DB_USER = ''
DB_PASSWORD = ''

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
GCS_BUCKET = ''
