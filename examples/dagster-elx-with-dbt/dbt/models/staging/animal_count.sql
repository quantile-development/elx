SELECT 
    description AS animal,
    COUNT(*) AS count
FROM 
    {{ source("tap_smoke_test", "stream_one") }}
GROUP BY
    description