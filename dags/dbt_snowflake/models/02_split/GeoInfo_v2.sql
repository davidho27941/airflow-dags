WITH source_data AS (
    SELECT * FROM {{ this }}
)

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
FROM {{ source('cwb_raw_json_stn', 'raw_stn')}} AS New_data,
    LATERAL FLATTEN(input => RAW_DATA:records:data:stationStatus:station) first_layer;
{% if is_incremental() %}
    WHERE 
        New_data.StationID NOT IN (
            SELECT DISTINCT StationID FROM source_data
        )
        OR
        (
            New_data.StationID IN (SELECT DISTINCT StationID FROM source_data)
            AND
            TO_DATE(New_data.StationStartDate) > (SELECT TO_DATE(StationEndDate) FROM source_data WHERE source_data.StationID = New_data.StationID)
        )

{% endif %}