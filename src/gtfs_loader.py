"""
GTFS Data Loader
Loads BKK GTFS feeds into the staging and DWH tables, with support for
multiple overlapping/sequential feeds.

Each subdirectory of data/raw/gtfs/ is treated as a separate feed; its
validity period is read from feed_info.txt and registered in
staging.gtfs_feeds. All staging rows are tagged with the owning feed_id.
"""

import csv
import io
import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

logger = logging.getLogger('GTFSLoader')


class GTFSLoader:
    # Columns we care about per table (matches staging schema)
    TABLE_COLUMNS = {
        'stg_gtfs_stops': [
            'stop_id', 'stop_name', 'stop_lat', 'stop_lon',
            'location_type', 'parent_station', 'wheelchair_boarding'
        ],
        'stg_gtfs_routes': [
            'route_id', 'agency_id', 'route_short_name', 'route_long_name',
            'route_type', 'route_color', 'route_text_color', 'route_desc'
        ],
        'stg_gtfs_trips': [
            'route_id', 'service_id', 'trip_id', 'trip_headsign',
            'direction_id', 'block_id', 'shape_id',
            'wheelchair_accessible', 'bikes_allowed'
        ],
        'stg_gtfs_stop_times': [
            'trip_id', 'arrival_time', 'departure_time', 'stop_id',
            'stop_sequence', 'pickup_type', 'drop_off_type',
            'shape_dist_traveled'
        ]
    }

    FILE_TO_TABLE = {
        'stops.txt': 'stg_gtfs_stops',
        'routes.txt': 'stg_gtfs_routes',
        'trips.txt': 'stg_gtfs_trips',
        'stop_times.txt': 'stg_gtfs_stop_times'
    }

    def __init__(self, config_path="config/config.yaml"):
        load_dotenv()
        self.config = self._load_config(config_path)

        db_conf = self.config['database']
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        if not user or not password:
            raise ValueError("Database credentials not found in environment variables")

        host = os.getenv('DB_HOST', db_conf['host'])
        self.db_url = f"postgresql://{user}:{password}@{host}:{db_conf['port']}/{db_conf['dbname']}"
        self.engine = create_engine(self.db_url)
        self.gtfs_root = Path("data/raw/gtfs")

    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    @staticmethod
    def _parse_gtfs_date(d):
        """YYYYMMDD string → ISO date string."""
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

    def _read_feed_info(self, feed_dir):
        """Read feed_info.txt and return dict with feed_version, feed_start_date, feed_end_date."""
        path = feed_dir / 'feed_info.txt'
        if not path.exists():
            return None
        with open(path) as f:
            reader = csv.DictReader(f)
            row = next(reader, None)
        if not row:
            return None
        return {
            'feed_version': row.get('feed_version') or feed_dir.name,
            'feed_start_date': self._parse_gtfs_date(row['feed_start_date']),
            'feed_end_date': self._parse_gtfs_date(row['feed_end_date']),
        }

    def _upsert_feed(self, conn, feed_meta):
        """Insert or replace the feed record; return its id."""
        result = conn.execute(text("""
            INSERT INTO staging.gtfs_feeds (feed_version, feed_start_date, feed_end_date, loaded_at)
            VALUES (:v, :s, :e, CURRENT_TIMESTAMP)
            ON CONFLICT (feed_version) DO UPDATE
                SET feed_start_date = EXCLUDED.feed_start_date,
                    feed_end_date = EXCLUDED.feed_end_date,
                    loaded_at = CURRENT_TIMESTAMP
            RETURNING id
        """), {
            'v': feed_meta['feed_version'],
            's': feed_meta['feed_start_date'],
            'e': feed_meta['feed_end_date'],
        })
        return result.scalar()

    def _load_feed_tables(self, feed_dir, feed_id):
        """Load all GTFS files from a feed directory, tagging each row with feed_id.

        Uses PostgreSQL COPY via psycopg2 for bulk loading — roughly 50x faster
        than pandas.to_sql(method='multi') on multi-million-row tables.
        """
        # Remove any prior data for this feed_id (in case of re-run)
        with self.engine.begin() as conn:
            for table in self.TABLE_COLUMNS:
                conn.execute(
                    text(f"DELETE FROM staging.{table} WHERE feed_id = :fid"),
                    {'fid': feed_id}
                )

        chunk_size = 200000
        for filename, table_name in self.FILE_TO_TABLE.items():
            file_path = feed_dir / filename
            if not file_path.exists():
                logger.warning(f"File {filename} not found in {feed_dir}, skipping...")
                continue

            logger.info(f"Loading {feed_dir.name}/{filename} → {table_name}...")
            expected_cols = self.TABLE_COLUMNS[table_name]
            total = 0

            for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=str):
                chunk.columns = chunk.columns.str.strip()
                valid_cols = [c for c in expected_cols if c in chunk.columns]
                chunk = chunk[valid_cols].copy()
                chunk['feed_id'] = feed_id
                all_cols = valid_cols + ['feed_id']

                # Serialize to CSV in memory and COPY into the table.
                buf = io.StringIO()
                chunk.to_csv(buf, index=False, header=False, na_rep='')
                buf.seek(0)

                raw_conn = self.engine.raw_connection()
                try:
                    with raw_conn.cursor() as cur:
                        col_list = ", ".join(f'"{c}"' for c in all_cols)
                        cur.copy_expert(
                            f"COPY staging.{table_name} ({col_list}) "
                            f"FROM STDIN WITH (FORMAT csv, NULL '')",
                            buf
                        )
                    raw_conn.commit()
                finally:
                    raw_conn.close()

                total += len(chunk)
                logger.info(f"  loaded {total} rows into {table_name}")
            logger.info(f"Finished loading {table_name} for feed {feed_id} ({total} rows)")

    def load_staging_tables(self):
        """Walk all feed subdirectories and load each one."""
        feed_dirs = sorted(p for p in self.gtfs_root.iterdir() if p.is_dir())
        if not feed_dirs:
            logger.error(f"No feed subdirectories found under {self.gtfs_root}")
            return

        for feed_dir in feed_dirs:
            feed_meta = self._read_feed_info(feed_dir)
            if not feed_meta:
                logger.warning(f"Skipping {feed_dir}: no feed_info.txt")
                continue

            logger.info(
                f"Processing feed {feed_meta['feed_version']} "
                f"({feed_meta['feed_start_date']} → {feed_meta['feed_end_date']})"
            )

            with self.engine.begin() as conn:
                feed_id = self._upsert_feed(conn, feed_meta)

            self._load_feed_tables(feed_dir, feed_id)

    def populate_dimensions(self):
        """
        Populate DWH dimensions from staging. Dimensions are keyed by natural IDs
        (route_id, stop_id) and upserted, so data from multiple feeds merges
        cleanly — later feeds win for conflicting attributes.
        """
        logger.info("Populating DWH dimensions...")

        ROUTE_TYPE_MAP = {
            '0': 'Tram', '1': 'Metro', '2': 'Rail', '3': 'Bus',
            '4': 'Ferry', '11': 'Trolleybus', '109': 'Suburban Railway'
        }
        LOCATION_TYPE_MAP = {
            '0': 'Stop', '1': 'Station', '2': 'Entrance/Exit',
            '3': 'Generic Node', '4': 'Boarding Area'
        }

        with self.engine.begin() as conn:
            logger.info("Populating dim_route...")
            # When multiple feeds provide the same route_id, prefer the most recently loaded.
            df_routes = pd.read_sql("""
                SELECT DISTINCT ON (r.route_id)
                       r.route_id, r.route_short_name, r.route_type
                FROM staging.stg_gtfs_routes r
                JOIN staging.gtfs_feeds f ON f.id = r.feed_id
                ORDER BY r.route_id, f.loaded_at DESC
            """, conn)
            df_routes['type'] = df_routes['route_type'].astype(str).map(ROUTE_TYPE_MAP).fillna('Other')
            df_routes = df_routes[['route_id', 'route_short_name', 'type']].rename(
                columns={'route_short_name': 'short_name'}
            )
            conn.execute(text("CREATE TEMP TABLE temp_dim_route (LIKE dwh.dim_route INCLUDING ALL)"))
            df_routes.to_sql('temp_dim_route', conn, if_exists='append', index=False)
            conn.execute(text("""
                INSERT INTO dwh.dim_route (route_id, short_name, type)
                SELECT route_id, short_name, type FROM temp_dim_route
                ON CONFLICT (route_id) DO UPDATE
                SET short_name = EXCLUDED.short_name,
                    type = EXCLUDED.type;
            """))
            conn.execute(text("DROP TABLE temp_dim_route"))

            logger.info("Populating dim_stop...")
            df_stops = pd.read_sql("""
                SELECT DISTINCT ON (s.stop_id)
                       s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, s.location_type
                FROM staging.stg_gtfs_stops s
                JOIN staging.gtfs_feeds f ON f.id = s.feed_id
                ORDER BY s.stop_id, f.loaded_at DESC
            """, conn)
            df_stops['location_type'] = (
                df_stops['location_type'].fillna('0').astype(int).astype(str)
                .map(LOCATION_TYPE_MAP).fillna('Unknown')
            )
            df_stops = df_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'location_type']].rename(
                columns={'stop_name': 'name', 'stop_lat': 'lat', 'stop_lon': 'lon'}
            )
            conn.execute(text("CREATE TEMP TABLE temp_dim_stop (LIKE dwh.dim_stop INCLUDING ALL)"))
            df_stops.to_sql('temp_dim_stop', conn, if_exists='append', index=False)
            conn.execute(text("""
                INSERT INTO dwh.dim_stop (stop_id, name, lat, lon, location_type)
                SELECT stop_id, name, lat, lon, location_type FROM temp_dim_stop
                ON CONFLICT (stop_id) DO UPDATE
                SET name = EXCLUDED.name,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    location_type = EXCLUDED.location_type;
            """))
            conn.execute(text("DROP TABLE temp_dim_stop"))

            logger.info("Dimensions populated successfully.")

    def run(self):
        if not self.gtfs_root.exists():
            logger.error(f"GTFS directory not found at {self.gtfs_root}")
            return
        self.load_staging_tables()
        self.populate_dimensions()


if __name__ == "__main__":
    loader = GTFSLoader()
    loader.run()
