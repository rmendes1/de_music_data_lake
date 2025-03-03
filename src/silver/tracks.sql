SELECT track_id AS trackId,
       track_title AS trackTitle,
       album_id AS albumId,
       artist_id AS artistId,
       duration AS duration,
       link AS link,
       preview AS preview,
       created_at AS createdAt,
       updated_at AS updatedAt
FROM bronze.music_data.tracks