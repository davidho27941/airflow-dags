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
), new_rain_fall_data AS(
    SELECT 
        data.value:Station_ID::varchar AS StationID,
        data.value:Station_name::varchar AS Stationname,
        data.value:Station_Latitude::number(10,5) AS StationLatitude,
        data.value:Station_Longitude::number(10,5) AS StationLongitude,
        data.value:CITY::varchar AS CITY,
        data.value:CITY_SN::varchar AS CITY_SN,
        data.value:TOWN::varchar AS TOWN,
        data.value:TOWN_SN::varchar AS TOWN_SN,
    FROM  {{ source('cwb_raw_json_stn', 'raw_stn_rain')}},
        lateral flatten(input => RAW_DATA:Data) data
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
    SELECT
        geo.StationID,
        geo.STATIONNAME,
        geo.CountyName,
        geo.CountyCode,
        geo.TownName,
        geo.TownCode,
        geo.StationAltitude,
        COALESCE(basic_geo.status, '現存測站') AS StationStutus,
        COALESCE(geo.Coordinates_TWD67:StationLatitude, basic_geo.StationLatitude, rain.StationLatitude) AS StationLatitude,
        COALESCE(geo.Coordinates_TWD67:StationLongitude, basic_geo.StationLongitude, rain.StationLongitude) AS StationLongitude,
        COALESCE(basic_geo.Notes, '') AS Notes,
        geo.Coordinates_TWD67,
        geo.Coordinates_WGS84,
    FROM geo_with_tw97 as geo
    LEFT JOIN new_rain_fall_data as rain
        ON geo.StationId = rain.StationID
    LEFT JOIN new_basic_data as basic_geo
        ON geo.StationId = basic_geo.StationID
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