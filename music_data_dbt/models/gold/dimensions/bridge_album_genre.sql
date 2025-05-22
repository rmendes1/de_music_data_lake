{{ config(
    materialized = 'table'
) }}

SELECT
    d_album.album_sk,
    d_genre.genre_sk
FROM {{ source('silver', 'albums_genres') }} ag
LEFT JOIN {{ ref('dim_albums') }} d_album ON ag.albumId = d_album.albumId
LEFT JOIN {{ ref('dim_genres') }} d_genre ON ag.genreId = d_genre.genreId