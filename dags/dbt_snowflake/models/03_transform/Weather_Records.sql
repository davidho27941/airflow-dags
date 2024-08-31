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
FROM {{ source('transformed_json', 'extracted_json') }}