import json
import psycopg2
from shapely import wkt
from shapely.geometry import mapping

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="extra_orbital",
    user="postgres",
    password="",  # Replace with your actual password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Fetch MEO shell geometries (only valid, non-empty)
cur.execute("""
    SELECT tract_id, ST_AsText(geom)
    FROM dev.tract_geometries_meo
    WHERE ST_IsValid(geom) AND NOT ST_IsEmpty(geom);
""")

czml = [
    {
        "id": "document",
        "name": "MEO Tract Shells",
        "version": "1.0"
    }
]

# Consistent color: teal w/ transparency
color = [0, 200, 180, 40]

for tract_id, geom_wkt in cur.fetchall():
    shape = wkt.loads(geom_wkt)
    coords = []
    for lon, lat, alt in mapping(shape)["coordinates"][0]:
        coords.extend([lon, lat, alt * 1000])  # km → meters

    czml.append({
        "id": tract_id,
        "name": tract_id,
        "polygon": {
            "positions": {
                "cartographicDegrees": coords
            },
            "material": {
                "solidColor": {
                    "color": {"rgba": color}
                }
            },
            "outline": True,
            "outlineColor": {"rgba": [255, 255, 255, 40]},
            "perPositionHeight": True
        }
    })

# Write to file
with open("meo_tracts_czml_v10.czml", "w") as f:
    json.dump(czml, f, indent=2)

print("✅ MEO tracts exported to meo_tracts_czml_v10.czml")
