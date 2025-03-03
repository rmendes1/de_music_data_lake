SELECT DISTINCT genre_id AS genreId,
       genre_name AS genreName,
       picture_link AS genrePictureUrl,
       created_at AS createdAt,
       updated_at AS updatedAt,
       CURRENT_TIMESTAMP AS ingestionDate 
 FROM bronze.music_data.genres
WHERE genre_id IS NOT NULL