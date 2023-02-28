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

def combine_data():
    """Combine continent-level JSON files into global files."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Load continents
    continents = load_continents(pathlib.Path(args.contfile))
    
    # Get logger
    logger = get_logger()
    
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
    }    
    
    # Combine continent-level data
    json_dict = combine_continents(continents, pathlib.Path(args.datadir), json_dict, logger)
    
    # Write out global json data
    write_json(pathlib.Path(args.datadir), json_dict, logger)
        
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
        basin_file = data_dir.joinpath(f"basin_{continent}.json")
        if not basin_file.is_file():
            logger.info(f"Continent data not present for: {continent.upper()}. Skipping continent.")
            continue
        with open(basin_file) as jf:
            json_dict["basin"] += json.load(jf)
        with open(data_dir.joinpath(f"cycle_passes_{continent}.json")) as jf:
            json_dict["cycle_passes"].update(json.load(jf))
        with open(data_dir.joinpath(f"hivdisets_{continent}.json")) as jf:
            json_dict["hivdisets"] += json.load(jf)
        with open(data_dir.joinpath(f"metrosets_{continent}.json")) as jf:
            json_dict["metrosets"] += json.load(jf)
        with open(data_dir.joinpath(f"passes_{continent}.json")) as jf:
            json_dict["passes"].update(json.load(jf))
        with open(data_dir.joinpath(f"reach_node_{continent}.json")) as jf:
            json_dict["reach_node"] += json.load(jf)
        with open(data_dir.joinpath(f"reaches_{continent}.json")) as jf:
            json_dict["reaches"] += json.load(jf)
        with open(data_dir.joinpath(f"s3_list_{continent}.json")) as jf:
            json_dict["s3_list"] += json.load(jf)
        with open(data_dir.joinpath(f"sicsets_{continent}.json")) as jf:
            json_dict["sicsets"] += json.load(jf)
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
    
if __name__ == "__main__":
    combine_data()