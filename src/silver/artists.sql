SELECT DISTINCT artist_id AS artistId,
       artist_name AS artistName,
       link AS link, 
       followers AS followers,
       photo AS artistPhotoUrl,
       created_at AS createdAt,
       updated_at AS updatedAt,
      CURRENT_TIMESTAMP AS ingestionDate 
 FROM bronze.music_data.artists
WHERE artist_id IS NOT NULL;
