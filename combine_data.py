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
# Example deployment

# expanded

# not expanded
# docker run -v /storage/repos/setfinder/testing:/data combine_data -d /data -c expanded_continent.json

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
    arg_parser.add_argument("-s",
                            "--sword_version",
                            type=str,
                            help="version of sword to use.")
    arg_parser.add_argument("-x",
                            "--delete",
                            help="Indicate if continent-level JSON files should be deleted.",
                            action="store_true")
    arg_parser.add_argument("-e",
                            "--expanded",
                            help="Indicate we are looking for expanded set files.",
                            action="store_true")
    return arg_parser

def load_continents(data_dir:str , cont_file:str, expanded):
    """Load continents from JSON file.
    
    Parameters
    ----------
    cont_json : pathlib.Path
        Path to continent.json file.
    """

    if expanded:
        cont_file = 'expanded_' + cont_file


    if len(glob.glob(os.path.join(data_dir, '*'))) == 0:
        raise ValueError('no files found at ', glob.glob(os.path.join(data_dir, '*')))

    continent_dict = {
        "af": {"af" : [1]},
        "as": {"as" : [4, 3]},
        "eu": {"eu" : [2]},
        "na": {"na" : [7, 8, 9]},
        "oc": {"oc" : [5]},
        "sa": {"sa" : [6]}
    }

    # Parses reach jsons to find what continents have data
    all_conts = [ continent_dict[os.path.basename(i).split('_')[-1].replace('.json', '')] for i in glob.glob(os.path.join(data_dir, '*reaches*.json')) if os.path.basename(i).split('_')[-1].replace('.json', '') not in ['interest', 'reaches']]

    # Create new continent file
    with open(os.path.join(data_dir, cont_file), 'w') as jf:
        json.dump(all_conts, jf, indent=2)
    
    return [list(i.keys())[0] for i in all_conts]

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
def parse_reach_list_for_output(reach_list:list, sword_version:int):

#   {
#     "reach_id": 12798000121,
#     "sword": "af_sword_v16_patch.nc",
#     "swot": "12798000121_SWOT.nc",
#     "sos": "af_sword_v16_SOS_priors.nc"
#   }
    continent_codes = { '1': "af", '2': "eu", '3': "as", '4': "as", '5': "oc", '6': "sa", '7': "na", '8': "na", '9':"na" }
    reach_dict_list = []
    for i in reach_list:
        reach_dict_list.append(
            {
            "reach_id": int(i),
            "sword": f"{continent_codes[str(i)[0]]}_sword_v{sword_version}.nc",
            "swot": f"{i}_SWOT.nc",
            "sos": f"{continent_codes[str(i)[0]]}_sword_v{sword_version}_SOS_priors.nc"
            }
        )
    return reach_dict_list
def combine_continents(continents, data_dir, sword_version,expanded, logger):
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
    out_dict = {}
    for continent in continents:
        if expanded:
            prefix = 'expanded_reaches_of_interest'
        else:
            prefix = ''
        
        all_continent_files = glob.glob(os.path.join(data_dir, f'{prefix}*{continent}*'))
        
        if not expanded:
            all_continent_files = [i for i in all_continent_files if not os.path.basename(i).startswith('expanded') ]

        for continent_file in all_continent_files:
            with open(continent_file) as jf:
                data = json.load(jf)
            
            global_file_basename = os.path.basename(continent_file).replace(f'_{continent}.json', '')

            if global_file_basename not in list(out_dict.keys()):
                out_dict[global_file_basename] = []

            out_dict[global_file_basename].extend(data)

            
            if not expanded and os.path.basename(continent_file).startswith('reaches'):
                if 'basin' not in list(out_dict.keys()):
                    out_dict['basin'] = []
                print('making basin from ', continent_file)
                base_reaches = [reach_data["reach_id"] for reach_data in data]
                basin_ids = list(set([str(reach)[:4] for reach in base_reaches]))
                
                basin_data = [create_basin_data(basin_id, base_reaches, sword_version) for basin_id in basin_ids]

                out_dict['basin'].extend(basin_data)


    reaches_json_list = []
    for a_key in list(out_dict.keys()):
        outpath = os.path.join(data_dir, a_key + '.json')
        reaches_json_list.append(outpath)
        with open(outpath, 'w') as jf:
            json.dump(out_dict[a_key], jf, indent=2)
            logger.info(f"Written: {outpath}.")

    return reaches_json_list

def create_basin_data(basin_id, base_reaches, sword_version):
    continent_codes = { '1': "af", '2': "eu", '3': "as", '4': "as", '5': "oc", '6': "sa", '7': "na", '8': "na", '9':"na" }

    return {
        "basin_id": basin_id, 
        "reach_id": [reach_id for reach_id in base_reaches if str(reach_id).startswith(str(basin_id))],
        "sword": f"{continent_codes[str(basin_id)[0]]}_sword_v{sword_version}.nc",
        "sos": f"{continent_codes[str(basin_id)[0]]}_sword_v{sword_version}_SOS_priors.nc"
    }

def read_json_data(data_dir, continent, filename, json_dict, sword_version):
    """
    Parameters
    -----------
    data_dir: pathlib.Path
        Path to datagen directory
    continent: str
        Two-letter continent string
    filename: str
        String name of JSON file (basin, reaches, etc...) this is key from earlier in the script, first time through is empty
    json_dict: dict
        Dictionary of global data lists
    """
    # if expanded this would be expanded_metrosets_na which came from the expanded setfinder
    json_file = data_dir.joinpath(f"{filename}_{continent}.json")
    with open(json_file) as jf:
        if filename == "cycle_passes" or filename == "passes" or filename == "s3_reach" or filename=='reaches':
            if filename == 'reaches':
                return json_dict
        else:
            try:
                
                json_dict[filename] += json.load(jf)
            except Exception as e:
                print(e)

        json_dict["json_files"] += [json_file]
    return json_dict

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

def combine_data():
    """Combine continent-level JSON files into global files."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    # Get logger
    logger = get_logger()
    
    # Load continents
    continents = load_continents(data_dir = args.datadir, cont_file = args.contfile, expanded = args.expanded)
    logger.info(f"Written: {args.contfile}")

    # Combine continent-level data
    json_file_list = combine_continents(continents, args.datadir, args.sword_version, args.expanded,logger)
    
    # Upload JSON files to S3
    if args.uploadbucket:

        try:
            upload(json_file_list, args.uploadbucket, logger)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            logger.info("System exiting.")
            sys.exit(1)
        
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
 
    
if __name__ == "__main__":
    combine_data()