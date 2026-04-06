#!/usr/bin/env python3
"""
Unified Weather Data Collector - Collects historical weather data for BKK transit data
"""

import json
import requests
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import yaml
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from logging.handlers import RotatingFileHandler


class UnifiedWeatherCollector:
    """Unified weather data collector with multiple operation modes"""

    def __init__(self, config_path="config/config.yaml"):
        """Initialize weather collector with configuration"""
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.setup_logging()
        self.collected_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.weather_cache = {}  # Cache for daily weather data

    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Check if weather API key is configured
        if 'weather_api' not in config or 'api_key' not in config['weather_api']:
            print("ERROR: Weather API key not configured in config.yaml")
            print("Please add:")
            print("weather_api:")
            print("  api_key: 'YOUR_OPENWEATHER_API_KEY'")
            sys.exit(1)

        return config

    def setup_logging(self):
        """Setup logging with rotation"""
        log_config = self.config['logging']

        # Create logs directory if it doesn't exist
        Path(log_config['file']).parent.mkdir(exist_ok=True)

        # Setup logger
        self.logger = logging.getLogger('WeatherCollector')
        self.logger.setLevel(getattr(logging, log_config['level']))

        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_config['file'],
            maxBytes=log_config['max_size_mb'] * 1024 * 1024,
            backupCount=log_config['backup_count']
        )

        # Console handler
        console_handler = logging.StreamHandler()

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def scan_bkk_files(self, days_back: Optional[int] = None) -> List[Tuple[Path, datetime]]:
        """
        Scan BKK data files and extract timestamps
        If days_back is specified, only scan recent files
        """
        bkk_path = Path(self.config['storage']['base_path'])
        files_with_timestamps = []

        if not bkk_path.exists():
            self.logger.error(f"BKK data directory not found: {bkk_path}")
            return []

        if days_back is not None:
            # Scan only recent files
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            self.logger.info(f"Scanning BKK data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

            current_date = start_date
            while current_date <= end_date:
                date_folder = current_date.strftime("%Y-%m-%d")
                day_path = bkk_path / date_folder

                if day_path.exists():
                    for json_file in sorted(day_path.glob("*.json")):
                        files_with_timestamps.extend(self._process_file(json_file))

                current_date += timedelta(days=1)
        else:
            # Scan all files
            self.logger.info("Scanning all BKK data files...")
            for json_file in sorted(bkk_path.rglob("*.json")):
                files_with_timestamps.extend(self._process_file(json_file))

        return files_with_timestamps

    def _process_file(self, json_file: Path) -> List[Tuple[Path, datetime]]:
        """Process a single BKK file and extract timestamp"""
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            if 'metadata' in data and 'timestamp' in data['metadata']:
                timestamp_str = data['metadata']['timestamp']
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                return [(json_file, timestamp)]

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.warning(f"Could not process {json_file}: {e}")

        return []

    def get_weather_file_path(self, bkk_file: Path) -> Path:
        """Generate corresponding weather file path for a BKK file"""
        weather_path = str(bkk_file).replace('/bkk/', '/weather/')
        weather_path = weather_path.replace('vehicles_', 'weather_')
        return Path(weather_path)

    def weather_data_exists(self, weather_file: Path) -> bool:
        """Check if weather data already exists for this timestamp"""
        return weather_file.exists()

    def fetch_weather_for_day(self, date: datetime, lat: float, lon: float) -> Optional[Dict]:
        """Fetch historical weather data for a specific day"""
        api_key = self.config['weather_api']['api_key']

        # Check cache first
        date_key = date.strftime("%Y-%m-%d")
        if date_key in self.weather_cache:
            return self.weather_cache[date_key]

        # Calculate start and end timestamps for the day
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)

        # OpenWeather History API
        url = "http://history.openweathermap.org/data/2.5/history/city"

        params = {
            'lat': lat,
            'lon': lon,
            'type': 'hour',
            'start': int(start_of_day.timestamp()),
            'end': int(end_of_day.timestamp()),
            'appid': api_key,
            'units': 'metric'
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Cache the response
            self.weather_cache[date_key] = data
            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                self.logger.error("Invalid API key")
            else:
                self.logger.error(f"API error for date {date_key}: {e}")
            return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching weather for {date_key}: {e}")
            return None

    def extract_weather_for_timestamp(self, daily_data: Dict, timestamp: datetime) -> Optional[Dict]:
        """Extract weather data for a specific timestamp from daily data"""
        if not daily_data or 'list' not in daily_data:
            return None

        target_unix = int(timestamp.timestamp())
        closest_data = None
        min_diff = float('inf')

        # Find the closest weather data point
        for item in daily_data['list']:
            if 'dt' in item:
                diff = abs(item['dt'] - target_unix)
                if diff < min_diff:
                    min_diff = diff
                    closest_data = item

        # Accept data within 1 hour
        if closest_data and min_diff <= 3600:
            return closest_data

        return None

    def save_weather_data(self, weather_file: Path, weather_data: Dict, timestamp: datetime):
        """Save weather data to JSON file"""
        weather_file.parent.mkdir(parents=True, exist_ok=True)

        output_data = {
            'metadata': {
                'timestamp': timestamp.isoformat(),
                'source': 'OpenWeather API',
                'location': 'Budapest',
                'lat': self.config['budapest']['lat'],
                'lon': self.config['budapest']['lon']
            },
            'data': weather_data
        }

        with open(weather_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

    def display_progress(self, current: int, total: int, message: str = ""):
        """Display progress bar (only in interactive terminals)"""
        if total == 0 or not sys.stdout.isatty():
            return

        percent = (current / total) * 100
        bar_length = 40
        filled_length = int(bar_length * current / total)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)

        sys.stdout.write(f'\rProgress: [{bar}] {percent:.1f}% - {message}')
        sys.stdout.flush()

        if current == total:
            print()  # New line when complete

    def collect_weather(self, days_back: Optional[int] = None, mode_name: str = "Collection"):
        """
        Main collection method
        days_back: If specified, only collect for recent days. If None, collect all.
        mode_name: Name to display in output (e.g., "Full Collection", "Sync")
        """
        self.logger.info(f"Starting weather data {mode_name.lower()}")

        # Get Budapest coordinates from config
        lat = self.config['budapest']['lat']
        lon = self.config['budapest']['lon']

        # Scan BKK files
        bkk_files = self.scan_bkk_files(days_back)

        if not bkk_files:
            self.logger.warning("No BKK data files found!")
            return

        self.logger.info(f"Found {len(bkk_files)} BKK data files")

        # Group files by date for efficient API usage
        files_by_date = defaultdict(list)
        for bkk_file, timestamp in bkk_files:
            weather_file = self.get_weather_file_path(bkk_file)

            if self.weather_data_exists(weather_file):
                self.skipped_count += 1
            else:
                date_key = timestamp.strftime("%Y-%m-%d")
                files_by_date[date_key].append((bkk_file, timestamp, weather_file))

        self.logger.info(f"Weather data already exists: {self.skipped_count} files")

        total_to_process = sum(len(files) for files in files_by_date.values())

        if total_to_process > 0:
            self.logger.info(f"Collecting weather data for: {total_to_process} files across {len(files_by_date)} days")
        else:
            self.logger.info("All weather data already collected!")
            return

        # Process by date to minimize API calls
        processed = 0
        for date_str, files in sorted(files_by_date.items()):
            date = datetime.strptime(date_str, "%Y-%m-%d")

            # Fetch weather data for the entire day
            self.logger.info(f"Fetching weather for {date_str} ({len(files)} files)...")
            daily_weather = self.fetch_weather_for_day(date, lat, lon)

            if not daily_weather:
                self.logger.error(f"Failed to fetch weather for {date_str}")
                self.failed_count += len(files)
                processed += len(files)
                continue

            # Process each file for this day
            for bkk_file, timestamp, weather_file in files:
                processed += 1
                self.display_progress(processed, total_to_process,
                                    f"Processing {timestamp.strftime('%H:%M')}")

                # Extract weather for specific timestamp
                weather_data = self.extract_weather_for_timestamp(daily_weather, timestamp)

                if weather_data:
                    self.save_weather_data(weather_file, weather_data, timestamp)
                    self.collected_count += 1
                else:
                    self.failed_count += 1

        # Print summary
        self.logger.info("=" * 50)
        self.logger.info(f"{mode_name} Summary:")
        self.logger.info(f"  Successfully collected: {self.collected_count}")
        self.logger.info(f"  Already existed: {self.skipped_count}")
        self.logger.info(f"  Failed: {self.failed_count}")
        if self.weather_cache:
            self.logger.info(f"  API calls made: {len(self.weather_cache)}")
        self.logger.info("=" * 50)

        if self.failed_count > 0:
            self.logger.warning("Some weather data could not be collected (API rate limits, missing data, or network issues)")



def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Weather Data Collector for BKK transit data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect all historical weather data
  python src/weather_collector.py --all

  # Collect recent data (last 2 days, default)
  python src/weather_collector.py --recent

  # Collect with custom days back
  python src/weather_collector.py --recent --days-back 5
        """
    )

    # Mode selection (required)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--all', action='store_true',
                           help='Collect weather for all BKK data')
    mode_group.add_argument('--recent', action='store_true',
                           help='Collect weather for recent BKK data only')

    # Options
    parser.add_argument('--days-back', type=int, default=2,
                       help='Number of days to look back (default: 2, used with --recent)')

    args = parser.parse_args()

    collector = UnifiedWeatherCollector()

    try:
        if args.all:
            # Full collection mode
            collector.collect_weather(days_back=None, mode_name="Full Collection")
        else:
            # Recent collection mode
            collector.collect_weather(days_back=args.days_back, mode_name="Recent Collection")

    except KeyboardInterrupt:
        collector.logger.info("Collection interrupted by user")
        collector.logger.info(f"Total collected: {collector.collected_count} files")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
