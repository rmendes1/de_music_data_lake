{{ config(
    materialized = 'incremental',
    unique_key = 'albumId',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.albumId']) }} AS album_sk,
    a.albumId,
    a.albumTitle,
    a.artistId,
    ar.artistName,
    a.releaseDate,
    a.link,
    a.cover,
    a.year,
    a.month,
    a.createdAt,
    a.updatedAt,
    a.ingestionDate
FROM {{ source('silver', 'albums') }} a
LEFT JOIN {{ source('silver', 'artists') }} ar ON a.artistId = ar.artistId
{% if is_incremental() %}
    WHERE a.ingestionDate > (SELECT MAX(ingestionDate) FROM {{ this }})
{% endif %}
