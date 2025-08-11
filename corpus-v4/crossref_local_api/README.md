# Crossref Academic Papers API

A Python API for querying academic papers from Crossref data stored in SQLite database on external drive.

## Features

- **External Drive Support**: Store 200GB+ database on external drive
- **Fast Queries**: Indexed database for sub-second searches
- **RESTful API**: Easy-to-use web interface
- **Flexible Search**: Search by DOI, author, journal, title, year, or combinations
- **Batch Import**: Efficiently import thousands of .jsonl.gz files
- **Live Crossref Integration**: Fetch real-time data from Crossref API in XML/JSON formats
- **Data Comparison**: Compare local database entries with live Crossref data

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure External Drive Path

Edit both `crossref_api.py` and `crossref_web_api.py` to set your external drive path:

**For macOS:**
```python
EXTERNAL_DRIVE_PATH = "/Volumes/MyExternalDrive"
```

**For Windows:**
```python
EXTERNAL_DRIVE_PATH = "D:"  # or whatever your external drive letter is
```

**For Linux:**
```python
EXTERNAL_DRIVE_PATH = "/mnt/external"
```

### 3. Import Your Data

Run the import script to load your Crossref .jsonl.gz files:

```bash
python crossref_api.py
```

This will:
- Create the SQLite database on your external drive
- Import all .jsonl.gz files from your data directory
- Create indexes for fast searching
- Show import progress

**Note**: For 200GB of data, this may take several hours depending on your drive speed.

### 4. Start the Web API

```bash
uvicorn crossref_web_api:app --reload
```

## Database Schema

The SQLite database contains a single `papers` table with the following structure:

- `id`: Primary key
- `doi`: Digital Object Identifier (unique)
- `title`: Paper title
- `journal`: Journal name
- `year`: Publication year
- `publisher`: Publisher name
- `created_at`, `indexed_at`: Timestamps

## Performance Tips

### External Drive Performance
- **USB 3.0+**: Use USB 3.0 or higher for better performance
- **SSD**: External SSDs are much faster than mechanical drives
- **Connection**: Direct connection is faster than through hubs


## External Drive Path Examples

**macOS:**
- External drive: `/Volumes/MyDrive/crossref.db`
- Network drive: `/Volumes/NetworkDrive/crossref.db`

**Windows:**
- External drive: `D:/crossref.db`
- Network drive: `\\NetworkDrive\crossref.db`

**Linux:**
- External drive: `/mnt/external/crossref.db`
- Network drive: `/mnt/network/crossref.db`

