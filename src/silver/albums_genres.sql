SELECT id AS albumGenreId,
       album_id AS albumId,
       genre_id as genreId,
      CURRENT_TIMESTAMP AS ingestionDate 
 FROM bronze.music_data.albums_genres
 WHERE id IS NOT NULL;