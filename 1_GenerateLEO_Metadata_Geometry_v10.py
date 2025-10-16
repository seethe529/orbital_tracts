GENERATE_METADATA = True
GENERATE_GEOMETRY = True

from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from shapely.validation import make_valid
from shapely.geometry.polygon import orient

Base = declarative_base()

class Tract(Base):
    __tablename__ = 'tracts'
    __table_args__ = {'schema': 'dev'} 
    tract_id = Column(String, primary_key=True)
    alt_min = Column(Float)
    alt_max = Column(Float)
    inc_min = Column(Float)
    inc_max = Column(Float)
    az_min = Column(Float)
    az_max = Column(Float)
    orbit_zone = Column(String, default='LEO')
    theta_start_idx = Column(Integer)
    theta_end_idx = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)


# Single DB connection and session for both sections
engine = create_engine("postgresql://postgres:@localhost:5432/extra_orbital")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

if GENERATE_METADATA:
    # Connect to your database
    # FULL cleanup before regeneration
    session.execute(text("DELETE FROM dev.tract_geometries_leo"))
    session.execute(text("DELETE FROM dev.tracts WHERE orbit_zone = 'LEO'"))
    session.commit()

    # Bin definitions
    alt_bins = [(a, a + 50) for a in range(200, 2001, 50)]
    inc_bins = [(i, i + 5) for i in range(0, 180, 5)]
    # RAAN bins refined to 5-degree intervals for v8.0
    raan_bins = [(r, r + 5) for r in range(0, 360, 5)]

    # Angular resolution: 1 degree → 360 total segments
    n_segments = 360
    segment_span = 360 / n_segments  # = 1.0

    new_tracts = []

    for alt_min, alt_max in alt_bins:
        for inc_min, inc_max in inc_bins:
            for az_min, az_max in raan_bins:
                zone = "LEO"
                theta_start_idx = int(az_min // segment_span)
                theta_end_idx = int(az_max // segment_span)

                tract_id = f"{zone}-A{alt_min}-I{inc_min}-RAAN{az_min}_{az_max}"
                new_tracts.append(Tract(
                    tract_id=tract_id,
                    alt_min=alt_min,
                    alt_max=alt_max,
                    inc_min=inc_min,
                    inc_max=inc_max,
                    az_min=az_min,
                    az_max=az_max,
                    orbit_zone=zone,
                    theta_start_idx=theta_start_idx,
                    theta_end_idx=theta_end_idx
                ))

    # Save to database
    session.bulk_save_objects(new_tracts)
    session.commit()

    print(f"✅ Inserted {len(new_tracts)} updated metadata rows with arc segment indices.")

# Geometry related imports

import numpy as np
from pyproj import Transformer
from shapely.geometry import Polygon, mapping, shape
from shapely.ops import unary_union
from shapely.wkt import dumps
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry
from datetime import datetime

class TractGeometry(Base):
    __tablename__ = 'tract_geometries_leo'
    __table_args__ = {'schema': 'dev'}

    tract_id = Column(String, primary_key=True)
    geom = Column(Geometry(geometry_type='POLYGONZ', srid=4326), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

if GENERATE_GEOMETRY:
    import numpy as np
    from pyproj import Transformer
    from shapely.geometry import Polygon
    from shapely.wkt import dumps

    transformer = Transformer.from_crs("epsg:4978", "epsg:4326", always_xy=True)

    def normalize_longitude(lon):
        return ((lon + 180) % 360) - 180

    def unwrap_lon(lon):
        if lon > 180:
            return lon - 360
        elif lon < -180:
            return lon + 360
        return lon

    def generate_panel_geometry(radius_km, inc_min, inc_max, raan_min, raan_max, steps=16):
        if raan_max < raan_min:
            raan_max += 360
        inc_range = np.linspace(inc_min, inc_max, steps)
        raan_range = np.linspace(raan_min, raan_max, steps)

        outer = []
        inner = []

        for raan_deg in raan_range:
            theta_rad = np.radians(raan_deg % 360)
            inc_rad = np.radians(inc_max if not np.isclose(inc_max, 90.0) else 89.9)
            x = radius_km * np.cos(theta_rad)
            y = radius_km * np.sin(theta_rad) * np.cos(inc_rad)
            z = radius_km * np.sin(theta_rad) * np.sin(inc_rad)
            lon, lat, alt = transformer.transform(x * 1000, y * 1000, z * 1000)
            lat = np.clip(lat, -89.9999, 89.9999)
            outer.append((unwrap_lon(lon), lat, alt / 1000))

        for raan_deg in reversed(raan_range):
            theta_rad = np.radians(raan_deg % 360)
            inc_rad = np.radians(inc_min if not np.isclose(inc_min, 90.0) else 90.1)
            x = radius_km * np.cos(theta_rad)
            y = radius_km * np.sin(theta_rad) * np.cos(inc_rad)
            z = radius_km * np.sin(theta_rad) * np.sin(inc_rad)
            lon, lat, alt = transformer.transform(x * 1000, y * 1000, z * 1000)
            lat = np.clip(lat, -89.9999, 89.9999)
            inner.append((unwrap_lon(lon), lat, alt / 1000))

        full_ring = outer + inner + [outer[0]]

        if len(outer) < 3 or len(inner) < 3:
            print(f"❌ Too few valid vertices for tract with RAAN {raan_min}-{raan_max}, INC {inc_min}-{inc_max}")
            return Polygon()

        # Detect antimeridian crossing and split manually
        def split_at_antimeridian(coords):
            west, east = [], []
            for lon, lat, alt in coords:
                if lon >= 0:
                    east.append((lon, lat, alt))
                else:
                    west.append((lon, lat, alt))
            polygons = []
            if len(west) > 3:
                polygons.append(Polygon(west))
            if len(east) > 3:
                polygons.append(Polygon(east))
            return polygons

        split_polys = split_at_antimeridian(full_ring)

        if not split_polys:
            print(f"❌ Manual antimeridian split produced no valid polygons for RAAN {raan_min}-{raan_max}, INC {inc_min}-{inc_max}")
            return Polygon()

        poly = unary_union(split_polys)

        if not poly.is_valid or poly.is_empty:
            print(f"❌ Geometry creation failed for RAAN {raan_min}-{raan_max}, INC {inc_min}-{inc_max}")
            return Polygon()

        return orient(poly, sign=1.0)

    # Load metadata and regenerate geometry
    tracts = session.query(Tract).filter(Tract.orbit_zone == 'LEO').all()
    session.execute(text("DELETE FROM dev.tract_geometries_leo"))
    session.commit()

    print(f"Loaded {len(tracts)} LEO tracts for geometry generation.")
    count = 0

    # ===================== 🟦 Panel Geometry Validation & Insertion 🟦 =====================
    for tract in tracts:
        radius_km = (tract.alt_min + tract.alt_max) / 2 + 6371
        panel = generate_panel_geometry(radius_km, tract.inc_min, tract.inc_max, tract.az_min, tract.az_max)

        if not isinstance(panel, Polygon) or panel.is_empty:
            print(f"⚠️ Skipping malformed or empty geometry for tract {tract.tract_id}")
            continue

        from shapely.geometry import mapping
        from shapely.geometry.polygon import orient

        try:
            # Force CCW winding
            oriented_panel = orient(panel, sign=1.0)
            geo = mapping(oriented_panel)
            panel = shape(geo)
        except Exception as e:
            print(f"❌ Antimeridian correction failed for tract {tract.tract_id}: {e}")
            continue

        # Ensure panel validity
        if panel.is_empty or not panel.is_valid or panel.geom_type not in ["Polygon", "MultiPolygon"]:
            print(f"⚠️ Invalid or empty panel for tract {tract.tract_id}")
            if not panel.is_empty:
                print("  Polygon points:")
                if hasattr(panel, "exterior"):
                    for coord in panel.exterior.coords:
                        print(f"   - {coord}")
                else:
                    print(f"  Skipped printing coordinates: geometry type is {panel.geom_type}")
            continue  # Skip this invalid panel

        polygon_wkt = dumps(panel, output_dimension=3)

        session.merge(TractGeometry(
            tract_id=tract.tract_id,
            geom=f"SRID=4326;{polygon_wkt}"
        ))
        count += 1

    session.commit()
    print(f"✅ Inserted {count} toroidal LEO shell panels into dev.tract_geometries_leo.")
