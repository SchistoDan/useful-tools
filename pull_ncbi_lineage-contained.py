import os
import pathlib
import sys
import pandas as pd
import urllib.error
import time
import random
import logging
import argparse
from Bio import Entrez
from datetime import datetime

Entrez.sleep_between_tries = 20
Entrez.max_tries = 20


def setup_logging(log_dir="./", log_file="ncbi_lineage.log"):
    """Set up logging to file and console."""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def log_and_print(message, level='info'):
    """Log a message and print it to console."""
    logger = logging.getLogger()
    if level == 'info':
        logger.info(message)
    elif level == 'error':
        logger.error(message)
    elif level == 'warning':
        logger.warning(message)


def xlsx2csv(file_path, sheet=None):
    """Read an Excel file, write it to CSV, and return the CSV path."""
    if not pathlib.Path(file_path).is_file():
        logging.getLogger().error(f"Error: The file '{file_path}' does not exist.")
        return None
    try:
        xlsx_read = pd.read_excel(file_path, sheet_name=sheet)
    except Exception as e:
        logging.getLogger().error(f"Failed to read Excel file: {e}")
        return None

    sheet_suffix = f"_sheet{sheet}" if sheet is not None else ""
    csv_name = pathlib.Path(file_path).stem + sheet_suffix
    dirname = os.path.dirname(file_path)
    csv_file_path = os.path.join(dirname, f"{csv_name}.csv")

    xlsx_read.to_csv(csv_file_path, index=None, header=True)
    logging.getLogger().info(f"Converted '{file_path}' (sheet={sheet}) to '{csv_file_path}'")
    return csv_file_path


def get_ncbi_lineage(taxid, email, logger, api_key=None):
    """Get NCBI taxonomic lineage for a given taxid with retry logic."""
    Entrez.email = email
    if api_key and api_key != "None":
        Entrez.api_key = api_key

    log_and_print(f"Getting NCBI lineage for taxid: {taxid}")

    max_retries = 5
    base_delay = 10

    for attempt in range(max_retries):
        try:
            handle = Entrez.efetch(db="Taxonomy", id=taxid, retmode="xml")
            record = Entrez.read(handle)

            if not record:
                raise ValueError(f"No taxonomy record found for taxid {taxid}")

            tax_record = record[0]
            lineage_list = tax_record.get("LineageEx", [])
            lineage_list.append({
                'TaxId': tax_record['TaxId'],
                'ScientificName': tax_record['ScientificName'],
                'Rank': tax_record['Rank']
            })

            lineage = {}
            for item in lineage_list:
                rank = item['Rank']
                name = item['ScientificName']
                if rank in ['species', 'genus', 'family', 'order', 'class', 'phylum', 'kingdom']:
                    lineage[rank] = name

            logger.info(f"Retrieved lineage for {taxid}: {', '.join([f'{k}:{v}' for k,v in lineage.items()])}")
            return lineage

        except (ValueError, urllib.error.HTTPError, urllib.error.URLError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            log_and_print(f"Attempt {attempt+1}/{max_retries}: Error getting lineage for {taxid}: {str(e)}", level="warning")

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                log_and_print(f"Failed to get lineage for {taxid} after {max_retries} attempts.", level="error")
                raise

    raise Exception(f"Failed to get lineage for {taxid}")


def add_ncbi_lineages_to_csv(input_csv, output_csv, taxcolumn, email, logger, api_key=None, sheet=1):
    """Add NCBI taxonomic lineages to a CSV/XLSX file based on taxids."""
    Entrez.email = email
    if api_key and api_key != "None":
        Entrez.api_key = api_key

    filetype = pathlib.Path(input_csv).suffix.lower()
    if filetype == ".xlsx":
        csv_path = xlsx2csv(input_csv, sheet=sheet)
        if csv_path is None:
            logger.error("Failed to convert XLSX to CSV.")
            return
        df = pd.read_csv(csv_path)
    elif filetype == ".csv":
        df = pd.read_csv(input_csv)
    else:
        raise ValueError(f"Unsupported file format: {filetype}")

    if taxcolumn not in df.columns:
        logger.error(f"Taxid column '{taxcolumn}' not found in input CSV.")
        return

    ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    for rank in ranks:
        df[rank] = None

    for index, row in df.iterrows():
        taxid = str(row[taxcolumn])
        try:
            lineage = get_ncbi_lineage(taxid, Entrez.email, logger, Entrez.api_key)
            for rank in ranks:
                if rank in lineage:
                    df.at[index, rank] = lineage[rank]
        except Exception as e:
            logger.error(f"Could not retrieve lineage for taxid {taxid}: {e}")

    df.to_csv(output_csv, index=False)
    logger.info(f"Lineage data added and saved to {output_csv}")


def main():
    if args.log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.log_file = f'ncbi_pull_lineage_{timestamp}.log'
    logger = setup_logging(log_dir="./", log_file=args.log_file)
    add_ncbi_lineages_to_csv(args.input_csv, args.output_csv, args.taxcolumn, args.email, logger, args.api_key, args.sheet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add NCBI taxonomic lineages to a CSV file based on taxids.")
    parser.add_argument("--input_csv", help="Path to the input CSV file.")
    parser.add_argument("--output_csv", help="Path to the output CSV file.")
    parser.add_argument("--email", help="Email address for NCBI Entrez.")
    parser.add_argument("--api_key", help="NCBI API key for increased rate limits.", default=None)
    parser.add_argument("--taxcolumn", help="Column name in CSV that contains taxids.", default="taxid")
    parser.add_argument("--log_file", help="Path to log file.", default="ncbi_lineage.log")
    parser.add_argument("--sheet", help="Sheet number to read if input is XLSX.", type=int, default=1)
    args = parser.parse_args()
    main()
