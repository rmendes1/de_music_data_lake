SELECT album_id AS albumId,
       album_title AS albumTitle,
       artist_id AS artistId,
       release_date AS releaseDate,
       link AS link,
       cover AS cover,
       created_at AS createdAt,
       updated_at AS updatedAt
FROM bronze.music_data.albums