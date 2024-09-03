WITH old_data AS (
    SELECT * FROM {{ this }}
), new_basic_data AS (
    SELECT 
        first_layer.value:status::varchar AS StationStatus,
        first_layer.value:StationID::varchar AS StationID,
        first_layer.value:StationName::varchar AS StationName,
        first_layer.value:StationNameEN::varchar AS StationNameEN,
        first_layer.value:StationAltitude::float AS StationAltitude,
        first_layer.value:StationLongitude::float AS StationLongitude,
        first_layer.value:StationLatitude::float AS StationLatitude,
        first_layer.value:CountyName::varchar AS CountyName,
        first_layer.value:Location::varchar AS Location,
        first_layer.value:StationStartDate::varchar AS StationStartDate,
        first_layer.value:StationEndDate::varchar AS StationEndDate,
        first_layer.value:Notes::varchar AS Notes,
        first_layer.value:OriginalStationID::varchar AS OriginalStationID,
        first_layer.value:NewStationID::varchar AS NewStationID,
    FROM {{ source('cwb_raw_json_stn', 'raw_stn')}} AS New_data,
        LATERAL FLATTEN(input => RAW_DATA:records:data:stationStatus:station) first_layer
), geo_data AS (
    SELECT 
        DISTINCT StationId,
        GeoInfo:CountyCode as CountyCode,
        GeoInfo:TownCode as TownCode,
        GeoInfo:TownName as TownName,
    
        GeoInfo:Coordinates[0] as Coordinates_TWD67,
        GeoInfo:Coordinates[1] as Coordinates_WGS84,
    FROM {{ ref('extracted_json_v2') }}
), merged_new_data AS (
    select 
        geo_basic.status,
        geo_basic.StationID,
        geo_basic.StationName,
        geo_basic.StationNameEN,
        geo_basic.StationAltitude,
        geo_basic.StationLongitude,
        geo_basic.StationLatitude,
        geo_basic.CountyName,
        geo_tw97.TownName::varchar AS TownName,
        geo_basic.Location,
        geo_basic.StationStartDate,
        geo_basic.StationEndDate,
        geo_basic.Notes,
        geo_basic.OriginalStationID,
        geo_basic.NewStationID,
        geo_tw97.CountyCode::varchar,
        geo_tw97.TownCode::varchar,
        geo_tw97.Coordinates_TWD67,
        geo_tw97.Coordinates_WGS84,
    FROM geo_with_tw97 as geo_tw97
    RIGHT JOIN new_basic_data as geo_basic
    ON geo_tw97.StationId = geo_basic.StationID
)

SELECT
    *
FROM merged_new_data
{% if is_incremental() %}
    WHERE 
        merged_new_data.StationID NOT IN (
            SELECT DISTINCT StationID FROM old_data
        )
{% endif %}