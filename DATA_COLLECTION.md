# BKK Transit Analytics - Data Collection Guide

A simple guide for collecting BKK transit and weather data using automated cron jobs.

## Quick Start

### 1. Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure API keys
cp config/config.yaml.template config/config.yaml
# Edit config.yaml and add your BKK and OpenWeather API keys

# Make scripts executable
chmod +x scripts/run_bkk_collector.sh scripts/run_weather_collector.sh

# Generate and install cron jobs
python scripts/generate_crontab.py
crontab scripts/crontab.txt
```

### 2. Verify

```bash
# Check cron installation
crontab -l

# Monitor collection
python src/monitor.py
```

That's it! Data collection will now run automatically.

## What Gets Collected

### BKK Transit Data
- **What**: Real-time vehicle positions, routes, and status
- **When**:
  - Weekdays (Mon-Thu) 9:00-17:00: Every 15 minutes
  - Other times: Every 5 minutes
- **Where**: `data/raw/bkk/YYYY-MM-DD/vehicles_HH-MM-SS.json`

### Weather Data
- **What**: Historical weather for each BKK data timestamp
- **When**: Twice daily at 9:00 and 21:00 (looks back 2 days)
- **Where**: `data/raw/weather/YYYY-MM-DD/weather_HH-MM-SS.json`

## Monitoring

### Quick Status Check
```bash
python src/monitor.py
```

Shows:
- Collection status
- Total files and storage
- Recent errors
- Last collection details

### View Logs
```bash
# Application logs (both collectors)
tail -f logs/collector.log

# Cron execution logs
tail -f logs/cron_bkk.log
tail -f logs/cron_weather.log
```

## Manual Collection

### BKK Data
```bash
# Run once
python src/bkk_collector.py
```

### Weather Data
```bash
# Collect for recent data (last 2 days)
python src/weather_collector.py --recent

# Collect all historical data (first time setup)
python src/weather_collector.py --all

# Custom lookback period
python src/weather_collector.py --recent --days-back 5
```

## Common Tasks

### Stop Collection
```bash
# Temporarily disable cron jobs
crontab -r

# Or edit and comment out lines
crontab -e
```

### Restart Collection
```bash
# Reinstall cron jobs
crontab scripts/crontab.txt
```

### Change Collection Intervals

Edit `config/config.yaml` intervals, regenerate, and reinstall:
```bash
# 1. Edit config/config.yaml
# Change collection_intervals values (e.g., weekend: 10)

# 2. Regenerate crontab
python scripts/generate_crontab.py

# 3. Reinstall
crontab scripts/crontab.txt
```

### Adjust Weather Lookback

Edit `scripts/run_weather_collector.sh`:
```bash
# Change --days-back parameter
python3 "$PROJECT_DIR/src/weather_collector.py" --recent --days-back 3 2>&1
```

## Troubleshooting

### No Data Being Collected

1. **Check if cron jobs are running**:
   ```bash
   crontab -l
   tail -f logs/cron_bkk.log
   ```

2. **Check for errors**:
   ```bash
   grep ERROR logs/collector.log
   python src/monitor.py
   ```

3. **Verify API keys**:
   ```bash
   cat config/config.yaml | grep api_key
   ```

4. **Test manually**:
   ```bash
   python src/bkk_collector.py
   python src/weather_collector.py --recent
   ```

### Collector Appears Stuck

Remove lock files:
```bash
rm -f /tmp/bkk_collector.lock
rm -f /tmp/weather_collector.lock
```

### Cron Jobs Not Running

1. **Check script permissions**:
   ```bash
   ls -l scripts/*.sh
   # Should show: -rwxr-xr-x
   ```

2. **Test wrapper scripts**:
   ```bash
   ./scripts/run_bkk_collector.sh
   ./scripts/run_weather_collector.sh
   ```

3. **Check cron service** (Linux):
   ```bash
   sudo systemctl status cron
   ```

## Storage Estimates

- **BKK Data**: ~200-500 MB/day, 10-15 GB/month
- **Weather Data**: ~5-10 MB/day, 150-300 MB/month
- **Logs**: ~10-50 MB/month (with rotation)

Monitor storage:
```bash
du -sh data/raw/bkk/
du -sh data/raw/weather/
du -sh logs/
```

## File Structure

```
bkk-transit-analytics/
├── config/
│   └── config.yaml              # API keys and settings
├── data/
│   └── raw/
│       ├── bkk/                 # Transit data
│       │   └── YYYY-MM-DD/
│       │       └── vehicles_*.json
│       └── weather/             # Weather data
│           └── YYYY-MM-DD/
│               └── weather_*.json
├── logs/
│   ├── collector.log            # Application logs
│   ├── cron_bkk.log            # BKK cron logs
│   └── cron_weather.log        # Weather cron logs
├── scripts/
│   ├── run_bkk_collector.sh    # BKK wrapper
│   ├── run_weather_collector.sh # Weather wrapper
│   └── crontab.txt             # Cron schedule
└── src/
    ├── bkk_collector.py        # BKK collector
    ├── weather_collector.py    # Weather collector
    └── monitor.py              # Monitoring tool
```

## Configuration

Edit `config/config.yaml` to customize:

```yaml
# Collection intervals (minutes)
collection_intervals:
  weekday_working_hours: 15      # Mon-Thu 9:00-17:00
  weekday_non_working_hours: 5   # Mon-Thu other hours
  weekend: 5                     # Fri-Sun all day

# Working hours
working_hours:
  start: 9
  end: 17

# Weekend days (0=Mon, 6=Sun)
weekend_days: [4, 5, 6]  # Fri, Sat, Sun

# API keys
bkk_api:
  api_key: "YOUR_BKK_API_KEY"

weather_api:
  api_key: "YOUR_OPENWEATHER_API_KEY"
```

## Best Practices

1. **Monitor regularly**: Run `python src/monitor.py` daily
2. **Check logs weekly**: Look for errors or warnings
3. **Verify data quality**: Ensure files are being created
4. **Monitor storage**: Keep an eye on disk usage
5. **Backup configuration**: Keep `config.yaml` backed up securely

## Support

For issues:
1. Check logs: `tail -f logs/collector.log`
2. Run monitor: `python src/monitor.py`
3. Test manually: `python src/bkk_collector.py`
4. Review this guide's troubleshooting section

## Next Steps

After collecting data:
1. Verify collection with `python src/monitor.py`
2. Check data quality and completeness
3. Proceed with data analysis
4. Combine transit and weather data for insights
