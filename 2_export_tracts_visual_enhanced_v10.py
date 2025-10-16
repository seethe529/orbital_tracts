import json
import psycopg2
from shapely import wkt
from shapely.geometry import mapping

# DB connection
conn = psycopg2.connect(
    dbname="extra_orbital",
    user="postgres",
    password="",  # Update this
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Query all geometries
cur.execute("""
    SELECT tract_id, ST_AsText(geom)
    FROM dev.tract_geometries_leo;
""")

czml = [
    {
        "id": "document",
        "name": "LEO Tract Shells v10.0 - Visual Enhanced",
        "version": "1.0"
    }
]

# Format each polygon with enhanced styling
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
                    "color": {"rgba": [0, 150, 255, 30]}  # translucent blue
                }
            },
            "outline": True,
            "outlineColor": {"rgba": [255, 255, 255, 80]},  # subtle edge
            "perPositionHeight": True
        }
    })

# Save file
with open("leo_tracts_visual_enhanced_v10.czml", "w") as f:
    json.dump(czml, f, indent=2)

print("✅ CZML with enhanced visuals saved as leo_tracts_visual_enhanced_v10.czml")
