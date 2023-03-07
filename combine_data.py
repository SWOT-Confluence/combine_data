"""Combine JSON Data Files

This script combines the continent-level data files produced by datagen into
single, global-level files.

The following files are combined and created:
- basin.json
- cycle_passes.json
- hivdisets.json
- metrosets.json
- passes.json
- reach_node.json
- reaches.json
- s3_list.json
- sicsets.json

Files are written out to directory referenced by 'datadir' command line argument.
"""

# Standard imports
import argparse
from collections import OrderedDict
import datetime
import json
import logging
import pathlib
import re
import sys

# Third-party imports
import boto3
import botocore

def combine_data():
    """Combine continent-level JSON files into global files."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Get logger
    logger = get_logger()
    
    # Load continents
    continents = load_continents(pathlib.Path(args.contfile))
    
    # Lists to populate
    json_dict = {
        "basin" : [],
        "cycle_passes" : {},
        "hivdisets" : [],
        "metrosets" : [],
        "passes" : {},
        "reach_node" : [],
        "reaches" : [],
        "s3_list" : [],
        "sicsets" : [],
        "json_files": []
    }    
    
    # Combine continent-level data
    json_dict = combine_continents(continents, pathlib.Path(args.datadir), json_dict, logger)
    
    # Write out global json data
    write_json(pathlib.Path(args.datadir), json_dict, logger)
    
    # Delete continent-level data
    if args.delete:
        delete_continent_json(json_dict["json_files"], logger)
        
    # Disable renew
    try:
        disable_renew(logger)
    except botocore.exceptions.ClientError as e:
        handle_error(e, logger)
        
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
    
def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Combine continent JSON files")
    arg_parser.add_argument("-c",
                            "--contfile",
                            type=str,
                            help="Path to continent.json file.")
    arg_parser.add_argument("-d",
                            "--datadir",
                            type=str,
                            help="Path to directory that contains datagen data.")
    arg_parser.add_argument("-x",
                            "--delete",
                            help="Indicate if continent-level JSON files should be deleted.",
                            action="store_true")
    return arg_parser

def load_continents(cont_json):
    """Load continents from JSON file.
    
    Parameters
    ----------
    cont_json : pathlib.Path
        Path to continent.json file.
    """
    
    with open(cont_json) as jf:
        cont_data = json.load(jf)
    
    return [ key for d in cont_data for key in d.keys() ]

def get_logger():
    """Return a formatted logger object."""
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        # Create a handler to console and set level
        console_handler = logging.StreamHandler()

        # Create a formatter and add it to the handler
        console_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")
        console_handler.setFormatter(console_format)

        # Add handlers to logger
        logger.addHandler(console_handler)

    # Return logger
    return logger

def combine_continents(continents, data_dir, json_dict, logger):
    """Combine continent-level data in to global data.
    
    Parameters
    ----------
    continents: list
        List of continents to combine
    data_dir: pathlib.Path
        Path to datagen directory
    json_dict: dict
        Dictionary of global data lists
    logger: logger
        Logger instance to use for logging statements
        
    Returns
    -------
    dict
        Dictionary of global data lists
    """
    
    for continent in continents:
        # Check if data is present for continent
        basin_file = data_dir.joinpath(f"basin_{continent}.json")
        if not basin_file.is_file():
            logger.info(f"Continent data not present for: {continent.upper()}. Skipping continent.")
            continue
        
        # Concatenate continent-level data
        for key in json_dict.keys():
            if key == "json_files": continue
            json_dict = read_json_data(data_dir, continent, key, json_dict)
        
    return json_dict

def read_json_data(data_dir, continent, filename, json_dict):
    """
    Parameters
    -----------
    data_dir: pathlib.Path
        Path to datagen directory
    continent: str
        Two-letter continent string
    filename: str
        String name of JSON file (basin, reaches, etc...)
    json_dict: dict
        Dictionary of global data lists
    """
    
    json_file = data_dir.joinpath(f"{filename}_{continent}.json")
    with open(json_file) as jf:
        if filename == "cycle_passes" or filename == "passes":
            json_dict[filename].update(json.load(jf))
        else:
            json_dict[filename] += json.load(jf)
        json_dict["json_files"] += [json_file]
    return json_dict    

def write_json(data_dir, json_dict, logger):
    """Combine continent-level data in to global data.
    
    Parameters
    ----------
    data_dir: pathlib.Path
        Path to datagen directory
    json_dict: dict
        Dictionary of global data lists
    logger: logger
        Logger instance to use for logging statements
    """
    
    # Sort cycle pass data
    json_dict["cycle_passes"] = OrderedDict(sorted(json_dict["cycle_passes"].items(), key=sort_cycle_pass)) 
    json_dict["passes"] = OrderedDict(sorted(json_dict["passes"].items(), key=sort_cycle_pass)) 
    
    # Write global JSON files
    for key, value in json_dict.items():
        if key == "json_files": continue
        write_json_file(data_dir, key, value, logger)
        
def strtoi(text):
    return int(text) if text.isdigit() else text

def sort_cycle_pass(cycle_pass):
    """Sort cycle/pass data so that they are in ascending order."""
    
    return [ strtoi(cp) for cp in re.split(r'(\d+)', cycle_pass[0]) ]
            
def write_json_file(data_dir, filename, data, logger):
    """Write data to JSON file.
    
    Paramenters
    -----------
    data_dir: pathlib.Path
        Path to datagen directory
    filename: str
        String name of JSON file (basin, reaches, etc...)
    data: list
        Global data list
    logger: logger
        Logger instance to use for logging statements
    """
    
    with open(data_dir.joinpath(f"{filename}.json"), 'w') as jf:
        json.dump(data, jf, indent=2)
        logger.info(f"Written: {filename}.json.")
        
def delete_continent_json(json_files, logger):
    """Delete files in list."""
    
    for json_file in json_files: 
        json_file.unlink()
        logger.info(f"Deleted: {json_file}")
        
def disable_renew(logger):
    """Disable the hourly renewal of PO.DAAC S3 credentials."""
    
    scheduler = boto3.client("scheduler")
    try:
        # Get schedule
        get_response = scheduler.get_schedule(Name="confluence-renew")
        
        # Update schedule
        update_response = scheduler.update_schedule(
            Name=get_response["Name"],
            GroupName=get_response["GroupName"],
            FlexibleTimeWindow=get_response["FlexibleTimeWindow"],
            ScheduleExpression=get_response["ScheduleExpression"],
            Target=get_response["Target"],
            State="DISABLED"
        )
        logger.info("Disabled 'renew' Lambda function.")
    except botocore.exceptions.ClientError as e:
        raise e
        
def handle_error(error, logger):
    """Print out error message and exit."""
    
    logger.error("Error encountered.")
    logger.error(error)
    logger.error("System exiting.")
    sys.exit(1)
    
if __name__ == "__main__":
    combine_data()