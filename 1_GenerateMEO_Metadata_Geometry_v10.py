# === Medium Earth Orbit (MEO) Tract Generation Script v10 ===
# Purpose: Generate tract metadata and toroidal geometry panels for MEO orbital zones

# === 🧱 Database Setup and ORM Models ===
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

# Connect to the PostgreSQL database with PostGIS enabled for spatial data handling.
engine = create_engine("postgresql://postgres:@localhost:5432/extra_orbital")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# === 🗂️ MEO Metadata Generation (Altitude, Inclination, RAAN Binning) ===
if GENERATE_METADATA:
    # This block generates metadata entries defining the orbital tracts based on altitude, inclination, and RAAN bins.
    # It first clears existing MEO-related metadata to avoid duplicates or stale data.
    session.execute(text("DELETE FROM dev.tract_geometries_meo"))
    session.execute(text("DELETE FROM dev.tracts WHERE orbit_zone = 'MEO'"))
    session.commit()

    # Define altitude bins in kilometers, covering typical orbital altitudes for LEO, MEO, and GEO.
    # These bins segment the altitude dimension into discrete layers for tract definition.
    alt_bins = [
        (2000, 3000), (3000, 4000), (4000, 5000),
        (5000, 6000), (6000, 8000), (8000, 12000),
        (12000, 20000), (20000, 25000), (25000, 30000),
        (30000, 35786), (35786, 35786)]
    # Define inclination bins in degrees, from 0° (equatorial) to 180° (retrograde polar) in 5° increments.
    inc_bins = [(i, i + 5) for i in range(0, 180, 5)]
    # Define RAAN bins in degrees, segmenting the full 360° orbit plane orientation into 30° slices.
    raan_bins = [(r, r + 30) for r in range(0, 360, 30)]

    # Angular resolution for segment indexing is 2.5°, resulting in 144 segments around the orbit.
    # This helps map RAAN ranges to discrete segment indices for efficient indexing and referencing.
    n_segments = 144
    segment_span = 360 / n_segments  # = 2.5 degrees per segment

    new_tracts = []

    # Nested loops iterate over all combinations of altitude, inclination, and RAAN bins to define tracts.
    # Each tract is assigned an orbit zone label based on altitude thresholds (LEO, MEO, GEO).
    # The theta_start_idx and theta_end_idx indicate which angular segments the tract covers.
    for alt_min, alt_max in alt_bins:
        for inc_min, inc_max in inc_bins:
            for az_min, az_max in raan_bins:
                # Determine orbit zone based on altitude max:
                # LEO: ≤ 2000 km, MEO: > 2000 km and ≤ 35786 km (approx. geostationary orbit altitude),
                # GEO: > 35786 km (geostationary orbit altitude).
                zone = (
                    "LEO" if alt_max <= 2000 else
                    "MEO" if alt_max <= 35786 else
                    "GEO"
                )
                # Calculate segment indices for RAAN start and end by integer division of RAAN by segment span.
                theta_start_idx = int(az_min // segment_span)
                theta_end_idx = int(az_max // segment_span)

                # Construct a unique tract identifier encoding orbit zone, altitude, inclination, and RAAN ranges.
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

    # Bulk insert all new tracts into the database for efficient storage.
    session.bulk_save_objects(new_tracts)
    session.commit()

    print(f"✅ Inserted {len(new_tracts)} updated metadata rows with arc segment indices.")

    # Filter and display a sample of MEO tracts for verification.
    meo_tracts = [t for t in new_tracts if "MEO" in t.tract_id]

    print(f"✅ Total MEO tracts staged: {len(meo_tracts)}")
    print("🔍 Sample tract IDs:")
    for t in meo_tracts[:5]:  # Show first 5 as a preview
        print(f" - {t.tract_id}")

# === 🌐 Geometry Generation for Toroidal Orbital Panels ===

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
    __tablename__ = 'tract_geometries_meo'
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

    # === 🔧 Panel Geometry Constructor ===
    def generate_panel_geometry(radius_km, inc_min, inc_max, raan_min, raan_max, steps=16):
        """
        Given tract properties, generate a toroidal arc panel segment for a MEO orbital shell.
        Output is a 3D curved polygon projected to WGS84 (EPSG:4326).
        """
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

    # === 🚀 Loop Through Metadata to Build and Insert Panels ===
    tracts = session.query(Tract).filter(Tract.orbit_zone == 'MEO').all()
    session.execute(text("DELETE FROM dev.tract_geometries_meo"))
    session.commit()

    print(f"Loaded {len(tracts)} MEO tracts for geometry generation.")
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
    print(f"✅ Inserted {count} toroidal MEO shell panels into dev.tract_geometries_meo.")
