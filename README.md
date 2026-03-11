# Global News Monitor

Global News Monitor is a lightweight Python project for monitoring global events using the GDELT dataset, which updates every 15 minutes.

GDELT, the Global Database of Events, Language and Tone, is a large open dataset that extracts structured events from worldwide news coverage. It tracks actors, locations, event types, and sentiment across reporting from many countries and languages, and it updates continuously.

This project builds a simple monitoring tool on top of that dataset by fetching the newest event export and summarizing recent events directly in the console.

## Features

- Fetch latest GDELT event dataset
- Extract key event fields
- Convert event codes into readable labels
- Display recent world events in the console

## Example Output

```text
Global News Monitor starting...

[EVENT] UNITED KINGDOM → PRESIDENT | Diplomatic consultation (043) | 2
[EVENT] PRESIDENT → UNITED STATES | Diplomatic consultation (043) | 1
```

## Why the Event Dataset Instead of the DOC API

Originally I attempted to use the GDELT DOC API for this project.

However, the DOC API is designed for searching news articles by keywords and tends to return article-level results rather than structured event data.

It also frequently returned HTTP 429 rate-limit errors during development.

The project switched to the official GDELT Event export dataset instead.

Advantages of the Event dataset:

- Updates every 15 minutes
- Contains structured event records
- Includes actor names, event codes, countries, and coordinates
- More stable for continuous monitoring
- Doesnt give me an error 35% of the time haha

This makes it much better for building a real-time event monitoring system.

## Tech Stack

- Python
- Requests
- GDELT Event dataset

## Project Structure

`src/main.py`  
Entry point that runs the monitor and prints event summaries.

`src/gdelt_events.py`  
Handles downloading the newest GDELT dataset and parsing the CSV export.

`tests/`  
Contains unit tests for the project.

## Running the Project

```bash
python -m src.main
```

## Running Tests

```bash
pytest
```

## Future Plans

- global event heatmap
- event spike detection
- trend monitoring
- interactive dashboard
- the visual aspect after i find a way to save/analyze the data well
