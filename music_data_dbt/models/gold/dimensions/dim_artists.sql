{{ config(
    materialized = 'incremental',
    unique_key = 'artistId',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['artistId']) }} AS artist_sk,
    artistId,
    artistName,
    link,
    followers,
    artistPhotoUrl,
    createdAt,
    updatedAt,
    ingestionDate
FROM {{ source('silver', 'artists') }}

{% if is_incremental() %}
    WHERE ingestionDate > (SELECT MAX(ingestionDate) FROM {{ this }})
{% endif %}
