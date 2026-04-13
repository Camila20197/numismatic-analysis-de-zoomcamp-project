{{ config(materialized='table') }}

WITH source_data AS (
    -- Here we call the raw table using the reference we created in sources.yml
    SELECT * FROM {{ source('numismatic_raw', 'raw_banknotes') }}
),

clean_banknotes AS (
    -- We retrieve ONLY the IDs of the banknotes that passed the cleaning and transformation in dim_banknotes.sql
    SELECT banknote_id FROM {{ ref('dim_banknotes') }}
),

price_history AS (
    SELECT
        s.snapshot_id,
        s.id AS banknote_id,
        s.Price,
        s.scraped_at
    FROM source_data s

    INNER JOIN clean_banknotes b ON s.id = b.banknote_id
    WHERE s.Price IS NOT NULL
)

SELECT * FROM price_history