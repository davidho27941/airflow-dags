WITH old_data AS (
    SELECT * FROM {{ this }}
), new_data AS (
    SELECT
        DISTINCT
            StationId,
            StationName,
            ObsTime,
            Weather,
            AirTemperature,
            AirPressure,
            RelativeHumidity,
            WindSpeed,
            WindDirection,
            WindDirectionGust,
            PeakGustSpeed,
            Precipitation,
            SunshineDuration_10Min,
            Visibility,
            UVIndex,
    FROM {{ ref('extracted_json_v2') }}
)

SELECT
    *
FROM new_data
{% if is_incremental()%}
WHERE ObsTime > (SELECT MAX(ObsTime) FROM old_data)
{% endif %}
