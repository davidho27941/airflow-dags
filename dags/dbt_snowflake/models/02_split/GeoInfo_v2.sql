SELECT 
    first_layer.value:status::varchar AS status,
    first_layer.value:StationID::varchar AS StationID,
    first_layer.value:StationName::varchar AS StationName,
    first_layer.value:StationNameEN::varchar AS StationNameEN,
    first_layer.value:StationAltitude::float AS StationAltitude,
    first_layer.value:StationLongitude::float AS StationLongitude,
    first_layer.value:CountyName::varchar AS CountyName,
    first_layer.value:Location::varchar AS Location,
    first_layer.value:StationStartDate::varchar AS StationStartDate,
    first_layer.value:StationEndDate::varchar AS StationEndDate,
    first_layer.value:Notes::varchar AS Notes,
    first_layer.value:OriginalStationID::varchar AS OriginalStationID,
    first_layer.value:NewStationID::varchar AS NewStationID,
FROM {{ source('cwb_raw_json_stn', 'raw_stn')}},
    LATERAL FLATTEN(input => RAW_DATA:records:data:stationStatus:station) first_layer;
{% if is_incremental() %}
    WHERE 
        StationID NOT IN (
            SELECT DISTINCT StationID FROM CWB_TRANSFORMED.GeoInfo_v2
        )
        OR
        (
            StationID IN (SELECT DISTINCT StationID FROM CWB_TRANSFORMED.GeoInfo_v2)
            AND
            TO_DATE(StationStartDate) > TO_DATE(StationEndDate)
        )

{% endif %}