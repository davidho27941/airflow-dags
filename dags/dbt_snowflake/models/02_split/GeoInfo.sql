SELECT 
    StationId,
    StationName,
    TO_NUMBER(GeoInfo:StationAltitude, 10, 1) as Altitude,
    TRIM(GeoInfo:CountyName, '"') as CountyName,
    TRIM(GeoInfo:CountyCode, '"') as CountyCode,
    TRIM(GeoInfo:TownName, '"') as TownName,
    TRIM(GeoInfo:TownCode, '"') as TownCode,

    GeoInfo:Coordinates[0] as Coordinates_TWD67,
    GeoInfo:Coordinates[1] as Coordinates_WGS84,
FROM {{ ref('extracted_json') }}