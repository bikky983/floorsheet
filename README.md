# Merolagani Floorsheet Scraper

This project scrapes floorsheet data from [Merolagani](https://merolagani.com/Floorsheet.aspx) and saves it as a Parquet file.

## Features

- Scrapes transaction data from Merolagani floorsheet
- Handles pagination automatically
- Saves data in Parquet format with date partitioning
- Runs automatically via GitHub Actions

## GitHub Workflow

The project includes a GitHub workflow (`.github/workflows/merolagani_scraper.yml`) that:

1. Runs daily at 6:00 AM UTC (configurable)
2. Can be triggered manually via the Actions tab
3. Installs all required dependencies
4. Runs the scraper script
5. Uploads the scraped data as an artifact

## How to Access the Data

After the workflow runs, you can download the scraped data by:

1. Going to the Actions tab in your GitHub repository
2. Opening the latest workflow run
3. Downloading the "floorsheet-data" artifact

## Local Usage

To run the scraper locally:

1. Install dependencies: `pip install -r requirements.txt`
2. Run the script: `python merolagani_scraper.py`

The data will be saved in the `floorsheet_data` directory, partitioned by date.

## Dependencies

- Python 3.10 or higher
- See `requirements.txt` for all package dependencies 