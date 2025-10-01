#!/usr/bin/env python3
"""
BKK Collection Monitor - Check status and statistics of data collection
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
import yaml


def load_config(config_path="config/config.yaml"):
    """Load configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_collection_stats(base_path):
    """Get detailed statistics about collected data"""
    storage_path = Path(base_path)

    if not storage_path.exists():
        return None

    stats = {
        'total_files': 0,
        'total_size_mb': 0,
        'days': {},
        'latest_file': None,
        'oldest_file': None,
        'missing_intervals': []
    }

    all_files = list(storage_path.rglob("*.json"))

    if not all_files:
        return stats

    # Sort files by modification time
    all_files.sort(key=lambda x: x.stat().st_mtime)

    stats['oldest_file'] = {
        'path': str(all_files[0]),
        'time': datetime.fromtimestamp(all_files[0].stat().st_mtime).isoformat()
    }

    stats['latest_file'] = {
        'path': str(all_files[-1]),
        'time': datetime.fromtimestamp(all_files[-1].stat().st_mtime).isoformat()
    }

    # Process each file
    for json_file in all_files:
        stats['total_files'] += 1
        file_size = json_file.stat().st_size
        stats['total_size_mb'] += file_size / (1024 * 1024)

        # Group by day
        day = json_file.parent.name
        if day not in stats['days']:
            stats['days'][day] = {
                'files': 0,
                'size_mb': 0,
                'first_collection': None,
                'last_collection': None
            }

        stats['days'][day]['files'] += 1
        stats['days'][day]['size_mb'] += file_size / (1024 * 1024)

        # Track first and last collection times
        file_time = datetime.fromtimestamp(json_file.stat().st_mtime)
        if stats['days'][day]['first_collection'] is None or file_time < datetime.fromisoformat(stats['days'][day]['first_collection']):
            stats['days'][day]['first_collection'] = file_time.isoformat()
        if stats['days'][day]['last_collection'] is None or file_time > datetime.fromisoformat(stats['days'][day]['last_collection']):
            stats['days'][day]['last_collection'] = file_time.isoformat()

    stats['total_size_mb'] = round(stats['total_size_mb'], 2)

    # Round day sizes
    for day in stats['days']:
        stats['days'][day]['size_mb'] = round(stats['days'][day]['size_mb'], 2)

    return stats


def check_recent_collections(base_path, minutes=30):
    """Check if collection is running by looking for recent files"""
    storage_path = Path(base_path)

    if not storage_path.exists():
        return False, "Storage path does not exist"

    cutoff_time = datetime.now() - timedelta(minutes=minutes)
    recent_files = []

    for json_file in storage_path.rglob("*.json"):
        file_time = datetime.fromtimestamp(json_file.stat().st_mtime)
        if file_time > cutoff_time:
            recent_files.append({
                'file': json_file.name,
                'time': file_time.isoformat()
            })

    if recent_files:
        return True, recent_files
    else:
        return False, f"No files collected in the last {minutes} minutes"


def analyze_last_file(base_path):
    """Analyze the most recent data file"""
    storage_path = Path(base_path)

    all_files = list(storage_path.rglob("*.json"))
    if not all_files:
        return None

    # Get the most recent file
    latest_file = max(all_files, key=lambda x: x.stat().st_mtime)

    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)

        analysis = {
            'file': str(latest_file),
            'timestamp': data.get('metadata', {}).get('timestamp', 'Unknown'),
            'interval_minutes': data.get('metadata', {}).get('collection_interval_minutes', 'Unknown'),
            'day_type': data.get('metadata', {}).get('day_type', 'Unknown')
        }

        # Count vehicles
        if 'data' in data and 'data' in data['data']:
            vehicles = data['data']['data'].get('list', [])
            analysis['vehicle_count'] = len(vehicles)

            # Count by status
            status_counts = {}
            for vehicle in vehicles:
                status = vehicle.get('status', 'UNKNOWN')
                status_counts[status] = status_counts.get(status, 0) + 1
            analysis['vehicle_status'] = status_counts

            # Count by route type
            type_counts = {}
            for vehicle in vehicles:
                v_type = vehicle.get('vehicleRouteType', 'UNKNOWN')
                type_counts[v_type] = type_counts.get(v_type, 0) + 1
            analysis['vehicle_types'] = type_counts

        return analysis

    except Exception as e:
        return {'error': str(e)}


def check_log_file(log_path="logs/collector.log"):
    """Check recent log entries for errors"""
    log_file = Path(log_path)

    if not log_file.exists():
        return None

    # Read last 50 lines
    with open(log_file, 'r') as f:
        lines = f.readlines()

    recent_lines = lines[-50:] if len(lines) > 50 else lines

    errors = []
    warnings = []
    last_success = None

    for line in recent_lines:
        if 'ERROR' in line:
            errors.append(line.strip())
        elif 'WARNING' in line:
            warnings.append(line.strip())
        elif 'Data saved:' in line:
            last_success = line.strip()

    return {
        'errors': errors[-5:] if errors else [],  # Last 5 errors
        'warnings': warnings[-5:] if warnings else [],  # Last 5 warnings
        'last_success': last_success
    }


def main():
    """Main monitoring function"""
    config = load_config()
    base_path = config['storage']['base_path']

    print("=" * 60)
    print("BKK DATA COLLECTION MONITOR")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Check if collection is running
    print("COLLECTION STATUS:")
    is_running, recent_info = check_recent_collections(base_path, minutes=30)
    if is_running:
        print("✅ Collection appears to be RUNNING")
        print(f"   Recent files: {len(recent_info)} in last 30 minutes")
    else:
        print("⚠️  Collection may be STOPPED")
        print(f"   {recent_info}")
    print()

    # Get overall statistics
    stats = get_collection_stats(base_path)
    if stats:
        print("OVERALL STATISTICS:")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Total size: {stats['total_size_mb']} MB")
        print(f"  Days collected: {len(stats['days'])}")
        if stats['latest_file']:
            print(f"  Latest: {stats['latest_file']['time']}")
        if stats['oldest_file']:
            print(f"  Oldest: {stats['oldest_file']['time']}")
        print()

        # Show daily breakdown
        if stats['days']:
            print("DAILY BREAKDOWN:")
            for day in sorted(stats['days'].keys())[-7:]:  # Last 7 days
                day_stats = stats['days'][day]
                print(f"  {day}: {day_stats['files']} files, {day_stats['size_mb']} MB")
            print()

    # Analyze last file
    last_analysis = analyze_last_file(base_path)
    if last_analysis:
        print("LAST COLLECTION ANALYSIS:")
        print(f"  Timestamp: {last_analysis.get('timestamp', 'Unknown')}")
        print(f"  Interval: {last_analysis.get('interval_minutes', 'Unknown')} minutes")
        print(f"  Day type: {last_analysis.get('day_type', 'Unknown')}")
        if 'vehicle_count' in last_analysis:
            print(f"  Vehicles: {last_analysis['vehicle_count']}")
            if 'vehicle_types' in last_analysis:
                print("  Vehicle types:")
                for v_type, count in last_analysis['vehicle_types'].items():
                    print(f"    {v_type}: {count}")
        print()

    # Check logs
    log_status = check_log_file()
    if log_status:
        print("LOG STATUS:")
        if log_status['errors']:
            print("  Recent errors:")
            for error in log_status['errors']:
                print(f"    ❌ {error[:100]}...")
        else:
            print("  ✅ No recent errors")

        if log_status['warnings']:
            print("  Recent warnings:")
            for warning in log_status['warnings'][:2]:  # Show only 2 warnings
                print(f"    ⚠️  {warning[:100]}...")

        if log_status['last_success']:
            print(f"  Last success: {log_status['last_success'][:100]}...")

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
