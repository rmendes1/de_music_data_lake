-- SELECT DISTINCT album_id AS albumId,
--        album_title AS albumTitle,
--        artist_id AS artistId,
--        release_date AS releaseDate,
--        link AS link,
--        cover AS cover,
--        created_at AS createdAt,
--        updated_at AS updatedAt,
--        CURRENT_TIMESTAMP AS ingestionDate 
-- FROM bronze.music_data.albums
-- WHERE album_id IS NOT NULL AND artist_id IS NOT NULL;

SELECT DISTINCT album_id,
       album_id AS albumId,
       album_title AS albumTitle,
       artist_id AS artistId,
       release_date AS releaseDate,
       link AS link,
       cover AS cover,
       created_at AS createdAt,
       updated_at AS updatedAt,
       CURRENT_TIMESTAMP AS ingestionDate 
FROM bronze.music_data.albums
WHERE album_id IS NOT NULL AND artist_id IS NOT NULL;