import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    return pd.read_csv(file_to_process)

import json

def extract_from_json(path):
    # sniff the first non-whitespace char
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().lstrip()
    if not content:
        return pd.DataFrame()

    first = content[0]

    # Case 1: JSON array
    if first == "[":
        return pd.read_json(path)  # lines=False by default

    # Case 2: try NDJSON (one object per line)
    try:
        return pd.read_json(path, lines=True)
    except ValueError:
        pass

    # Case 3: fallback – concatenated JSON objects (not line-delimited)
    # Try to split by newlines, ignoring empty lines
    rows = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return pd.DataFrame(rows)


def extract_from_xml(file_to_process):
    rows = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        rows.append({"name": name, "height": height, "weight": weight})
    return pd.DataFrame(rows, columns=["name","height","weight"])

def extract():
    parts = []
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            parts.append(extract_from_csv(csvfile))
    for jsonfile in glob.glob("*.json"):
        parts.append(extract_from_json(jsonfile))
    for xmlfile in glob.glob("*.xml"):
        parts.append(extract_from_xml(xmlfile))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["name","height","weight"])

def transform(df):
    return df.dropna(subset=["name","height","weight"])

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

def log_progress(message):
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    line = f"{now.strftime(timestamp_format)},{message}"
    print(line)
    with open(log_file, "a") as f:
        f.write(line + "\n")

if __name__ == "__main__":
    # === Pipeline ===
    log_progress("ETL Job Started")
    log_progress("Extract phase Started")
    extracted_data = extract()
    log_progress("Extract phase Ended")

    log_progress("Transform phase Started")
    transformed_data = transform(extracted_data)
    log_progress("Transform phase Ended")

    log_progress("Load phase Started")
    load_data(target_file, transformed_data)
    log_progress("Load phase Ended")
    log_progress("ETL Job Ended")

    # Safe previews
    print("\nExtracted Data Preview:")
    print(extracted_data.head() if not extracted_data.empty else "(empty)")

    print("\nTransformed Data Preview:")
    print(transformed_data.head() if not transformed_data.empty else "(empty)")

    print("\nSaved file:", target_file)
