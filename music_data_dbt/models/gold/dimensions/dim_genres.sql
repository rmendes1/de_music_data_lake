{{ config(
    materialized = 'incremental',
    unique_key = 'genreId',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['genreId']) }} AS genre_sk,
    genreId,
    genreName,
    genrePictureUrl,
    createdAt,
    updatedAt,
    ingestionDate
FROM {{ source('silver', 'genres') }}
{% if is_incremental() %}
    WHERE ingestionDate > (SELECT MAX(ingestionDate) FROM {{ this }})
{% endif %}
