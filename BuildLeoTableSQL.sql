-- Table: dev.tract_geometries_leo

-- DROP TABLE IF EXISTS dev.tract_geometries_leo;

CREATE TABLE IF NOT EXISTS dev.tract_geometries_leo
(
    tract_id text COLLATE pg_catalog."default" NOT NULL,
    geom geometry(PolygonZ,4326) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT tract_geometries_leo_pkey PRIMARY KEY (tract_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS dev.tract_geometries_leo
    OWNER to postgres;
-- Index: idx_geom_tracts_leo

-- DROP INDEX IF EXISTS dev.idx_geom_tracts_leo;

CREATE INDEX IF NOT EXISTS idx_geom_tracts_leo
    ON dev.tract_geometries_leo USING gist
    (geom)
    TABLESPACE pg_default;