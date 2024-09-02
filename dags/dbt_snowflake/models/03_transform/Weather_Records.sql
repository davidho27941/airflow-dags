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
FROM {{ ref('extracted_json') }}