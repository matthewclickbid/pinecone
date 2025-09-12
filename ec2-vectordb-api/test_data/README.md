# Test Data Directory

This directory contains test CSV files for local development and testing.

## Structure

```
test_data/
├── csv_files/        # Place your CSV test files here
│   ├── sample.csv    # Example small CSV file
│   ├── 253.csv       # Your actual test files
│   └── ...
└── README.md         # This file
```

## Usage

1. Place your CSV files in the `csv_files/` directory
2. Set `USE_LOCAL_FILES=true` in your `.env.local` file
3. Reference files by their name relative to `csv_files/` directory

## Example

If you have a file at `test_data/csv_files/253.csv`, you would reference it as:
- S3 key: `253.csv` (when calling the API)
- Local path: The system will automatically resolve to `./test_data/csv_files/253.csv`

## Configuration

In `.env.local`:
```bash
USE_LOCAL_FILES=true
LOCAL_CSV_FOLDER=./test_data/csv_files
```

## Testing

To test with local files:
```bash
# Use .env.local configuration
cp .env.local .env

# Start the API
uvicorn app.main:app --reload

# Submit a processing task with local file
curl -X POST http://localhost:8001/api/v1/process \
  -H "X-API-Key: test-api-key-local-development" \
  -H "Content-Type: application/json" \
  -d '{
    "data_source": "s3_csv",
    "s3_key": "sample.csv",
    "batch_size": 100
  }'
```

The system will automatically read from `./test_data/csv_files/sample.csv` instead of S3.