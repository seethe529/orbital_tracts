SELECT
  tract_id,
  ST_ZMax(geom),
  ST_ZMin(geom),
  ST_AsText(ST_PointN(ST_ExteriorRing(geom), 1)) AS first_vertex
FROM dev.tract_geometries_leo
WHERE tract_id LIKE 'LEO-A1900%';

SELECT * FROM dev.tracts
WHERE orbit_zone ilike 'LEO%'
LIMIT 100;

--Search for Satellites within Tracts in LEO
SELECT s.satellite_id, s.name, COUNT(DISTINCT t.tract_id) AS num_tracts
FROM dev.tle_snapshots s
JOIN dev.tract_geometries_leo g
  ON ST_Contains(g.geom, s.position)
JOIN dev.tracts t
  ON t.tract_id = g.tract_id
WHERE s.altitude BETWEEN 0 AND 2000
  AND s.altitude >= t.alt_min
  AND s.altitude <  t.alt_max
GROUP BY s.satellite_id, s.name
ORDER BY num_tracts DESC;

-- Tract Density Check - LEO
SELECT
  COUNT(*) AS orbit_points,
  COUNT(DISTINCT s.satellite_id) AS distinct_sats,
  t.tract_id
FROM dev.tle_snapshots s
JOIN dev.tract_geometries_leo g ON ST_Contains(g.geom, s.position)
JOIN dev.tracts t ON g.tract_id = t.tract_id
WHERE s.altitude BETWEEN 0 AND 2000
  AND s.altitude >= t.alt_min AND s.altitude < t.alt_max
GROUP BY t.tract_id
ORDER BY orbit_points DESC;