WITH old_data AS (
    SELECT * FROM {{this}}
), new_data AS (
    select 
        TRIM(f_record_nested.value:StationId, '"') as StationId,
        TRIM(f_record_nested.value:StationName, '"') as StationName,
        TO_TIMESTAMP(f_record_nested.value:ObsTime:DateTime) as ObsTime,
        TRIM(f_record_nested.value:WeatherElement:Weather, '"') as Weather,
        f_record_nested.value:WeatherElement:AirTemperature::numeric(10,2) as AirTemperature,
        f_record_nested.value:WeatherElement:AirPressure::numeric(10,2) as AirPressure,
        f_record_nested.value:WeatherElement:RelativeHumidity::numeric(10,2) as RelativeHumidity,
        f_record_nested.value:WeatherElement:WindSpeed::numeric(10,2) as WindSpeed,
        f_record_nested.value:WeatherElement:WindDirection::numeric(10,2) as WindDirection,
        f_record_nested.value:WeatherElement:GustInfo:Occurred_at:WindDirection::numeric(10,2) as WindDirectionGust,
        f_record_nested.value:WeatherElement:GustInfo:PeakGustSpeed::numeric(10,2) as PeakGustSpeed,
        f_record_nested.value:WeatherElement:Now:Precipitation::numeric(10,2) as Precipitation,
        f_record_nested.value:WeatherElement:SunshineDuration::numeric(10,2) as SunshineDuration_10Min,
        TRIM(f_record_nested.value:WeatherElement:VisibilityDescription, '"') as Visibility,
        f_record_nested.value:WeatherElement:UVIndex::numeric(10,2) as UVIndex,
        f_record_nested.value:GeoInfo as GeoInfo,
        raw_data:result:fields as StationFieldsInfo,
    from {{ source('cwb_raw_json', 'raw')}},
        lateral flatten(input => raw_data:records) f_record,
        lateral flatten(input => f_record.value) f_record_nested,
)

SELECT 
    *
FROM new_data

{% if is_incremental() %}
    WHERE ObsTime > (
        SELECT 
            IFNULL(MAX(ObsTime), '2024-01-01')
        FROM old_data
    )
{% endif %}
