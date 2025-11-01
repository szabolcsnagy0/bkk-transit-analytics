# BKK Transit Analytics - Data Collection System

A clean and simple data collection system for Budapest public transit (BKK) real-time data with automated weather data collection.

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure API keys
cp config/config.yaml.template config/config.yaml
# Edit config.yaml and add your BKK and OpenWeather API keys

# 3. Set up automated collection
chmod +x scripts/run_bkk_collector.sh scripts/run_weather_collector.sh
python scripts/generate_crontab.py
crontab scripts/crontab.txt

# 4. Verify
crontab -l
python src/monitor.py
```

That's it! Data collection will now run automatically via cron.

## 📚 Documentation

**[→ Complete Data Collection Guide](DATA_COLLECTION.md)** - Everything you need to know about collecting and managing data.

## 📊 What Gets Collected

- **BKK Transit Data**: Real-time vehicle positions, routes, and status
  - Weekdays (Mon-Thu) 9:00-17:00: Every 15 minutes
  - Other times: Every 5 minutes

- **Weather Data**: Historical weather for each transit data timestamp
  - Twice daily at 9:00 and 21:00

## 🔍 Monitoring

```bash
# Quick status check
python src/monitor.py

# View logs
tail -f logs/collector.log
tail -f logs/cron_bkk.log
tail -f logs/cron_weather.log
```

## 🛠️ Manual Collection

```bash
# Run BKK collector once
python src/bkk_collector.py

# Run weather collector
python src/weather_collector.py --recent
python src/weather_collector.py --all
```

## 📁 Data Storage

```
data/raw/
├── bkk/YYYY-MM-DD/vehicles_HH-MM-SS.json
└── weather/YYYY-MM-DD/weather_HH-MM-SS.json
```

**Storage estimates**: ~200-500 MB/day for BKK data, ~5-10 MB/day for weather data

## 🐛 Troubleshooting

```bash
# Check status
python src/monitor.py

# Check logs
grep ERROR logs/collector.log

# Test manually
python src/bkk_collector.py
python src/weather_collector.py --recent

# Remove stuck lock files
rm -f /tmp/bkk_collector.lock /tmp/weather_collector.lock
```

See [DATA_COLLECTION.md](DATA_COLLECTION.md) for detailed troubleshooting.

## 📄 License

This project is for educational and research purposes.
