# User Activity Flatten Pipeline

## Overview

This project extracts JSON-based activity records from a PostgreSQL
table, flattens nested `actions` arrays, and loads the structured data
into a target table.

Pipeline Steps: 1. Read data from `master.user_activity_log` 2. Parse
`activity_data` (JSONB) 3. Flatten the `actions` array 4. Insert
structured rows into `master.flattened_user_activity` 5. Log execution
details

------------------------------------------------------------------------

## Source Table

**Table:** master.user_activity_log

  Column          Type
  --------------- -------
  user_id         TEXT
  activity_data   JSONB

------------------------------------------------------------------------

## Target Table

**Table:** master.flattened_user_activity

  Column             Type
  ------------------ -----------
  user_id            TEXT
  device             TEXT
  city               TEXT
  country            TEXT
  action_type        TEXT
  action_target      TEXT
  action_timestamp   TIMESTAMP
  processed_at       TIMESTAMP

------------------------------------------------------------------------

## Project Structure

. ├── flatten_pipeline.py ├── config.ini ├── flatten_pipeline.log └──
README.md

------------------------------------------------------------------------

## Requirements

-   Python 3.8+
-   PostgreSQL

Install dependencies:

pip install psycopg2 pandas

------------------------------------------------------------------------

## Configuration

Create a `config.ini` file:

\[database\] host=localhost db_name=your_database
user_name=your_username password=your_password port=5432

------------------------------------------------------------------------

## Running the Script

python flatten_pipeline.py

Logs will be written to:

flatten_pipeline.log

------------------------------------------------------------------------

## Automating with Cron (Every 5 Minutes)

Open crontab:

crontab -e

Add:

*/5 * \* \* \* /usr/bin/python3 /absolute/path/to/flatten_pipeline.py
\>\> /absolute/path/to/cron.log 2\>&1

If using virtual environment:

*/5 * \* \* \* /absolute/path/to/venv/bin/python
/absolute/path/to/flatten_pipeline.py \>\> /absolute/path/to/cron.log
2\>&1

------------------------------------------------------------------------

## Logging

The script logs: - Script start and end - Extraction completion -
Flattening completion - Insert confirmation

Author: Saravana Kumar
