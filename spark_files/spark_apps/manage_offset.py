import psycopg2
from psycopg2.extras import DictCursor
from config import DB_USER, DB_PASSWORD

# Função para carregar o último offset de um tópico e partição
def load_last_offset(topic, partition):
    """Retorna o último offset armazenado no banco para um tópico e partição."""
    try:
        with psycopg2.connect(
            dbname="music_data",
            user=DB_USER,
            password=DB_PASSWORD,
            host="db"
        ) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("""
                    SELECT "offset" FROM kafka_offsets
                    WHERE topic = %s AND "partition" = %s
                """, (topic, partition))
                result = cursor.fetchone()
                return result["offset"] if result else None  # Retorna None se não houver offset
    except psycopg2.Error as e:
        print(f"Erro ao carregar offset do Kafka: {e}")
        return None  # Se houver erro, assume que não há offset salvo

# Função para salvar o último offset de um tópico e partição
def save_last_offset(topic, partition, offset):
    """Salva o último offset processado no banco, garantindo atualização eficiente."""
    try:
        with psycopg2.connect(
            dbname="music_data",
            user=DB_USER,
            password=DB_PASSWORD,
            host=""
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO kafka_offsets (topic, "partition", "offset")
                    VALUES (%s, %s, %s)
                    ON CONFLICT (topic, "partition")
                    DO UPDATE SET "offset" = EXCLUDED."offset", updated_at = CURRENT_TIMESTAMP
                    RETURNING "offset"
                """, (topic, partition, offset))
                conn.commit()
    except psycopg2.Error as e:
        print(f"Erro ao salvar offset no Kafka: {e}")
