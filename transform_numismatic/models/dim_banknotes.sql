{{ config(materialized='table') }}

WITH source_data AS (
    -- Here we call the raw table using the reference we created in sources.yml
    SELECT * FROM {{ source('numismatic_raw', 'raw_banknotes') }}
),

cleaned_data AS (
    SELECT 
        id AS banknote_id,
        Country,
        Status,
        War,
        
        CASE 
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Country = 'Uruguay' THEN 'Peso'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Country = 'Zaire' THEN 'Zaire'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Country = 'Peru' THEN 'Nuevo Sol'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Country = 'Israel' THEN 'Nuevo Shekel'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Country = 'Mexico' THEN 'Peso'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Year BETWEEN 1800 AND 2002 AND Country = 'Francia' THEN 'Franco'
            WHEN DenomUnit IN ('Nuevo', 'Nuevos') AND Year BETWEEN 1800 AND 2002 AND Country = 'Antillas Francesas' THEN 'Franco'
            ELSE DenomUnit 
        END AS DenomUnit,
        
        DenomValue,
        Year,
        Century,
        Condition,
        ExtraTags
    FROM source_data
    WHERE 
        Year IS NOT NULL 
        AND TRIM(CAST(Year AS STRING)) != '' 
        
        AND Condition IS NOT NULL
        AND UPPER(Condition) != 'UNKNOWN'
),

unique_banknotes AS (
    SELECT DISTINCT * FROM cleaned_data
)

SELECT * FROM unique_banknotes