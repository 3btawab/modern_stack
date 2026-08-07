select
    geolocation_zip_code_prefix,
    avg(geolocation_lat) as geolocation_lat,
    avg(geolocation_lng) as geolocation_lng

from {{ ref('stg_geolocation') }}
group by 1
