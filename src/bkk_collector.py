#!/usr/bin/env python3
"""
BKK Data Collector - Simple and clean implementation for collecting real-time transit data
"""

import json
import requests
import sys
import time
import os
import logging
from datetime import datetime
from pathlib import Path
import yaml
from logging.handlers import RotatingFileHandler


class BKKCollector:
    """Simple BKK API data collector"""

    def __init__(self, config_path="config/config.yaml"):
        """Initialize collector with configuration"""
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.setup_logging()
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10

    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def setup_logging(self):
        """Setup logging with rotation"""
        log_config = self.config['logging']

        # Create logs directory if it doesn't exist
        Path(log_config['file']).parent.mkdir(exist_ok=True)

        # Setup logger
        self.logger = logging.getLogger('BKKCollector')
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

    def get_current_interval(self):
        """Determine collection interval based on current time"""
        now = datetime.now()
        weekday = now.weekday()  # 0=Monday, 6=Sunday
        hour = now.hour

        intervals = self.config['collection_intervals']
        working_hours = self.config['working_hours']
        weekend_days = self.config['weekend_days']

        # Check if it's weekend (Friday, Saturday, Sunday)
        if weekday in weekend_days:
            return intervals['weekend']

        # Check if it's working hours (Monday-Thursday)
        if working_hours['start'] <= hour < working_hours['end']:
            return intervals['weekday_working_hours']
        else:
            return intervals['weekday_non_working_hours']

    def collect_vehicles(self):
        """Collect vehicle data from BKK API"""
        api_config = self.config['bkk_api']
        budapest = self.config['budapest']

        url = f"{api_config['base_url']}/vehicles-for-location"

        params = {
            'key': api_config['api_key'],
            'lat': budapest['lat'],
            'lon': budapest['lon'],
            'radius': budapest['radius'],
            'version': api_config['version'],
            'includeReferences': 'true'
        }

        retry_config = self.config['retry']

        for attempt in range(retry_config['max_attempts']):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                # Reset failure counter on success
                self.consecutive_failures = 0

                return data

            except requests.exceptions.RequestException as e:
                wait_time = retry_config['initial_delay_seconds'] * (retry_config['backoff_factor'] ** attempt)
                self.logger.warning(f"API request failed (attempt {attempt + 1}/{retry_config['max_attempts']}): {e}")

                if attempt < retry_config['max_attempts'] - 1:
                    self.logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All retry attempts failed: {e}")
                    self.consecutive_failures += 1
                    return None

    def save_data(self, data):
        """Save collected data to JSON file"""
        if data is None:
            self.logger.warning("No data to save (collection failed)")
            return None

        now = datetime.now()
        storage_config = self.config['storage']

        # Create directory structure: data/raw/bkk/YYYY-MM-DD/
        date_folder = now.strftime("%Y-%m-%d")
        base_path = Path(storage_config['base_path'])
        day_path = base_path / date_folder
        day_path.mkdir(parents=True, exist_ok=True)

        # Create filename with timestamp
        timestamp = now.strftime("%H-%M-%S")
        filename = day_path / f"vehicles_{timestamp}.json"

        # Prepare data with metadata
        output_data = {
            'metadata': {
                'timestamp': now.isoformat(),
                'collection_interval_minutes': self.get_current_interval(),
                'day_type': 'weekend' if now.weekday() in self.config['weekend_days'] else 'weekday',
                'hour': now.hour
            },
            'data': data
        }

        # Save to file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, separators=(',', ':'))

            self.logger.info(f"Data saved: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            return None

    def get_collection_stats(self):
        """Get statistics about collected data"""
        storage_path = Path(self.config['storage']['base_path'])

        if not storage_path.exists():
            return None

        total_files = 0
        total_size = 0

        for json_file in storage_path.rglob("*.json"):
            total_files += 1
            total_size += json_file.stat().st_size

        return {
            'total_files': total_files,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'days_collected': len(list(storage_path.iterdir()))
        }

    def run(self):
        """Run a single collection cycle"""
        self.logger.info("Starting collection cycle...")

        # Collect data
        data = self.collect_vehicles()

        # Save data
        saved_file = self.save_data(data)

        if saved_file:
            # Get some basic stats from the data
            if data and 'data' in data:
                vehicle_count = len(data['data'].get('list', []))
                self.logger.info(f"Collected data for {vehicle_count} vehicles")

            # Log stats
            stats = self.get_collection_stats()
            if stats:
                self.logger.info(f"Collection stats: {stats['total_files']} files, "
                               f"{stats['total_size_mb']} MB, "
                               f"{stats['days_collected']} days")

        return saved_file is not None


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='BKK Data Collector - Collect real-time transit data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a single collection
  python src/bkk_collector.py

  # Run with custom config
  python src/bkk_collector.py --config config/custom.yaml
        """
    )

    parser.add_argument('--config', default='config/config.yaml',
                       help='Path to configuration file (default: config/config.yaml)')

    args = parser.parse_args()

    try:
        collector = BKKCollector(config_path=args.config)
        success = collector.run()

        if not success:
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
