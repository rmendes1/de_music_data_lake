{{ config(
    materialized = 'incremental',
    unique_key = 'trackId',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['t.trackId']) }} AS track_sk,
    t.trackId,
    d_artist.artist_sk,
    d_album.album_sk,
    t.trackTitle,
    t.duration,
    t.hasPreview,
    t.createdAt,
    t.updatedAt,
    t.ingestionDate
FROM {{ source('silver', 'tracks') }} t
LEFT JOIN {{ ref('dim_artists') }} d_artist ON t.artistId = d_artist.artistId
LEFT JOIN {{ ref('dim_albums') }} d_album ON t.albumId = d_album.albumId
{% if is_incremental() %}
    WHERE t.ingestionDate > (SELECT MAX(ingestionDate) FROM {{ this }})
{% endif %}
