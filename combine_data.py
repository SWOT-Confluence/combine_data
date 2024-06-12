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
import glob
import json
import logging
import pathlib
import re
import sys   
import os

# Third-party imports
import boto3
import botocore
import fnmatch
   
def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Combine continent JSON files")
    arg_parser.add_argument("-c",
                            "--contfile",
                            type=str,
                            default="continent.json",
                            help="Name of continent.json file.")
    arg_parser.add_argument("-d",
                            "--datadir",
                            type=str,
                            help="Path to directory that contains datagen data.")
    arg_parser.add_argument("-u",
                            "--uploadbucket",
                            type=str,
                            help="Name of S3 bucket to upload JSON files to.")
    arg_parser.add_argument("-x",
                            "--delete",
                            help="Indicate if continent-level JSON files should be deleted.",
                            action="store_true")
    return arg_parser

def load_continents(data_dir, cont_file):
    """Load continents from JSON file.
    
    Parameters
    ----------
    cont_json : pathlib.Path
        Path to continent.json file.
    """
    
    continent_dict = {
        "af": {"af" : [1]},
        "as": {"as" : [4, 3]},
        "eu": {"eu" : [2]},
        "na": {"na" : [7, 8, 9]},
        "oc": {"oc" : [5]},
        "sa": {"sa" : [6]}
    }
    
    # Grab datagen JSON files
    json_files = glob.glob(f"{data_dir}/*.json")
    
    # Parse the files and determine continents
    c = []
    for json_file in json_files:
        for key in continent_dict.keys():
            json_file_c = json_file.split('.')[-2].split('_')[-1]
            if key == json_file_c and continent_dict[key] not in c:
                c.append(continent_dict[key])
    
    # Create new continent file
    with open(f"{data_dir}/{cont_file}", 'w') as jf:
        json.dump(c, jf, indent=2)
    
    # Return continents present
    return [ key for d in c for key in d.keys() ]

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
            if key == "json_files" or key == "continent": continue
            try:
                json_dict = read_json_data(data_dir=data_dir, continent=continent, filename=key, json_dict=json_dict)
            except Exception as e:
                print(e)
                print('failed to read', key, 'for', continent)
            if key == 's3_list':
                json_dict[key] = parse_duplicate_files(json_dict[key])
            
            if key == 's3_reach':
                for reach_id in json_dict[key].keys():
                    json_dict[key][reach_id] = parse_duplicate_files(json_dict[key][reach_id])

        
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
        if filename == "cycle_passes" or filename == "passes" or filename == "s3_reach":
            json_dict[filename].update(json.load(jf))
        else:
            json_dict[filename] += json.load(jf)
        json_dict["json_files"] += [json_file]
    return json_dict

def log_totals(continents, json_dict, logger):
    """Log different totals."""
    
    logger.info(f"Number of continents: {len(continents):,}. Continents present: {continents}.")
    logger.info(f"Number of basins: {len(json_dict['basin']):,}.")
    logger.info(f"Number of reaches: {len(json_dict['reaches']):,}.")
    logger.info(f"Number of HiVDI sets: {len(json_dict['hivdisets']):,}.")
    logger.info(f"Number of MetroMan sets: {len(json_dict['metrosets']):,}.")
    logger.info(f"Number of sic4DVar sets: {len(json_dict['sicsets']):,}.")

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
    json_file_list = []
    for key, value in json_dict.items():
        if key == "json_files": 
            continue
        elif key == "continent": 
            json_file_list.append(value)
        else:
            json_file_list.append(write_json_file(data_dir, key, value, logger))
        
    return json_file_list
        
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
    
    json_file = data_dir.joinpath(f"{filename}.json")
    with open(json_file, 'w') as jf:
        json.dump(data, jf, indent=2)
        logger.info(f"Written: {filename}.json.")
    return json_file

def upload(json_file_list, upload_bucket, logger):
    """Upload JSON files to S3 bucket."""
    
    s3 = boto3.client("s3")
    date_prefix = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    for json_file in json_file_list:
        try:
            # Upload under date prefix
            s3.upload_file(str(json_file),
                           upload_bucket,
                           f"{date_prefix}/{json_file.name}",
                           ExtraArgs={"ServerSideEncryption": "aws:kms"})
            # Upload to root of bucket
            s3.upload_file(str(json_file),
                           upload_bucket,
                           json_file.name,
                           ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logger.info(f"Uploaded {json_file} to {upload_bucket}.")    
        except botocore.exceptions.ClientError as e:
            raise e
        
def delete_continent_json(json_files, logger):
    """Delete files in list."""
    
    for json_file in json_files: 
        json_file.unlink()
        logger.info(f"Deleted: {json_file}")
        
def handle_error(error, logger):
    """Print out error message and exit."""
    
    logger.error("Error encountered.")
    logger.error(error)
    logger.error("System exiting.")
    sys.exit(1)



# Function to extract and convert the timestamp to a datetime object
def extract_datetime(url):
    # Find the position of the timestamp in the URL
    timestamp_str = os.path.basename(url).split('_')[8]
    # Convert the timestamp string to a datetime object
    return datetime.datetime.strptime(timestamp_str, '%Y%m%dT%H%M%S')


def parse_duplicate_files(s3_urls:list):

        """
        In some cases, when shapefiles are processed more than once they leave both processings in the bucket, so we need to filter them.

        """
        parsed = []

        for i in s3_urls:
            # print(i[:-6])
            # mult_process_bool = False
            all_processings = fnmatch.filter(s3_urls, i[:-6]+'*')
            if len(all_processings) > 1:
                all_processings_nums = [int(i[-6:].replace('.zip', '')) for i in all_processings]
                padded_max = str("{:02d}".format(max(all_processings_nums)))
                max_path = fnmatch.filter(all_processings, f'*{padded_max}.zip')
                parsed.append(max_path[0])
                print('found a double', i)
            else:
                parsed.append(i)

        parsed = list(set(parsed))

        sorted_urls = sorted(parsed, key=extract_datetime)

        return sorted_urls



def combine_data():
    """Combine continent-level JSON files into global files."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Get logger
    logger = get_logger()
    
    # Load continents
    continents = load_continents(args.datadir, args.contfile)
    logger.info(f"Written: {args.contfile}")
    
    # Lists to populate
    json_dict = {
        "basin" : [],
        "continent": pathlib.Path(args.datadir).joinpath(args.contfile),
        "cycle_passes" : {},
        "hivdisets" : [],
        "metrosets" : [],
        "neosets" :[],
        "passes" : {},
        "reach_node" : [],
        "reaches" : [],
        "s3_list" : [],
        "s3_reach": {},
        "sicsets" : [],
        "json_files": []
    }    
    
    # Combine continent-level data
    json_dict = combine_continents(continents, pathlib.Path(args.datadir), json_dict, logger)
    
    # Write out global json data
    json_file_list = write_json(pathlib.Path(args.datadir), json_dict, logger)
    
    # Upload JSON files to S3
    if args.uploadbucket:

        try:
            upload(json_file_list, args.uploadbucket, logger)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            logger.info("System exiting.")
            sys.exit(1)
    
    # Delete continent-level data
    if args.delete:
        delete_continent_json(json_dict["json_files"], logger)
        
    # Log different totals
    log_totals(continents, json_dict, logger)
        
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
 
    
if __name__ == "__main__":
    combine_data()