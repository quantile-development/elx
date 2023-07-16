SELECT 
    description AS animal,
    COUNT(*) AS count
FROM 
    {{ source("source", "stream-one")}}
GROUP BY
    description