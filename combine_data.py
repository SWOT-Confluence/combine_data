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

CONTINENTS = [
    { "af" : [1] },
    { "as" : [4, 3] },
    { "eu" : [2] },
    { "na" : [7, 8, 9] },
    { "oc" : [5] },
    { "sa" : [6] }
]

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
    arg_parser.add_argument("-k",
                            "--bucketkey",
                            type=str,
                            help="Name of prefix to upload JSON files to.")
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
    arg_parser.add_argument("--ssc",
                            help="Indicate we are looking for expanded set files.",
                            action="store_true")
    return arg_parser

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

def combine_continents(continents, data_dir, sword_version,expanded,ssc, logger):
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
    continent_json = []
    for continent in continents:
        if expanded:
            all_continent_files = glob.glob(os.path.join(data_dir, f'expanded_reaches_of_interest_{continent}.json'))
        else:
            all_continent_files = glob.glob(os.path.join(data_dir, f'*_{continent}.json'))

        if all_continent_files and not expanded:
            key = all_continent_files[0].split("_")[-1].split(".")[0]
            for element in CONTINENTS:
                for c, i in element.items():
                    if c == key:
                        continent_json.append({key: i})
        
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
                logger.info('making basin from %s', continent_file)
                base_reaches = [reach_data["reach_id"] for reach_data in data]
                basin_ids = list(set([str(reach)[:4] for reach in base_reaches]))
                basin_data = [create_basin_data(data_dir, basin_id, base_reaches, sword_version) for basin_id in basin_ids]
                out_dict['basin'].extend(basin_data)

    reaches_json_list = []
    for a_key in list(out_dict.keys()):
        outpath = os.path.join(data_dir, a_key + '.json')
        reaches_json_list.append(outpath)
        with open(outpath, 'w') as jf:
            json.dump(out_dict[a_key], jf, indent=2)
            logger.info(f"Written: {outpath}.")
    
    if not expanded:
        c_file = os.path.join(data_dir, 'continent.json')
        reaches_json_list.append(c_file)
        with open(c_file, 'w') as jf:
            json.dump(continent_json, jf, indent=2)
            logger.info(f"Written: {c_file}")         
    if ssc:
        ssc_json_data = combine_ssc(data_dir=data_dir, logger = logger)

        with open(os.path.join(data_dir,"ssc_hls_list.json"), "w") as jf:
            json.dump(ssc_json_data, jf, indent=2)

    return reaches_json_list

def combine_ssc(data_dir:str, logger):
        """Combine SSC input data into a single file."""
        ssc_input_data = glob.glob(os.path.join(data_dir, "ssc", "*.json"))


        ssc_json_data = {}
        count = 0
        for ssc_input in ssc_input_data:
            with open(ssc_input) as jf:
                data = json.load(jf)
                for key in list(data.keys()):
                    short_key = key[:-10]
                    if short_key in list(ssc_json_data.keys()):
                        prev_len = len(ssc_json_data[short_key])
                        ssc_json_data[short_key].extend(data[key])
                        ssc_json_data[short_key] = list(set(ssc_json_data[short_key]))
                        after_len = len(ssc_json_data[short_key])

                    else:
                        ssc_json_data[short_key] = data[key]


                # ssc_json_data.extend(data)
        single_entry_list = [{k: v} for k, v in ssc_json_data.items()]

        return single_entry_list

def create_basin_data(data_dir, basin_id, base_reaches, sword_version):
    continent_codes = { '1': "af", '2': "eu", '3': "as", '4': "as", '5': "oc", '6': "sa", '7': "na", '8': "na", '9':"na" }

    sword_filepath = os.path.join(data_dir, "sword", f"{continent_codes[str(basin_id)[0]]}_sword_v{sword_version}_patch.nc")
    if os.path.exists(sword_filepath):
        sword_file = os.path.basename(sword_filepath)
    else:
        sword_file = f"{continent_codes[str(basin_id)[0]]}_sword_v{sword_version}.nc"

    return {
        "basin_id": basin_id, 
        "reach_id": [reach_id for reach_id in base_reaches if str(reach_id).startswith(str(basin_id))],
        "sword": sword_file,
        "sos": f"{continent_codes[str(basin_id)[0]]}_sword_v{sword_version}_SOS_priors.nc"
    }

def upload(json_file_list, upload_bucket, bucket_key, input_dir, expanded, logger):
    """Upload JSON files to S3 bucket."""

    s3 = boto3.client("s3")
    try:
        for json_file in json_file_list:
            json_file = pathlib.Path(json_file)
            if json_file.name == "expanded_reaches_of_interest.json": continue

            # Upload under bucket key
            s3.upload_file(str(json_file),
                        upload_bucket,
                        f"{bucket_key}/{json_file.name}",
                        ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logger.info(f"Uploaded {json_file} to {upload_bucket}/{bucket_key}.")
            # Upload to root of bucket
            s3.upload_file(str(json_file),
                        upload_bucket,
                        json_file.name,
                        ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logger.info(f"Uploaded {json_file} to {upload_bucket}.")

        # Upload expanded reaches of interest
        expanded_roi = pathlib.Path(input_dir).joinpath("expanded_reaches_of_interest.json")
        if expanded_roi.exists():
            s3.upload_file(str(expanded_roi),
                                upload_bucket,
                                expanded_roi.name,
                                ExtraArgs={"ServerSideEncryption": "aws:kms"})
        logger.info(f"Uploaded {expanded_roi} to {upload_bucket}.")
        if not expanded and expanded_roi.exists():
            s3.upload_file(str(expanded_roi),
                                upload_bucket,
                                f"{bucket_key}/{expanded_roi.name}",
                                ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logger.info(f"Uploaded {expanded_roi} to {upload_bucket}/{bucket_key}.")

    except botocore.exceptions.ClientError as e:
        raise e

def combine_data():
    """Combine continent-level JSON files into global files."""

    start = datetime.datetime.now()

    # Get logger
    logger = get_logger()

    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()

    for arg in vars(args):
        logger.info("%s: %s", arg, getattr(args, arg))

    # Load continents
    continents = [
        "af",
        "as",
        "eu",
        "na",
        "oc",
        "sa"
    ]

    # Combine continent-level data
    json_file_list = combine_continents(continents, args.datadir, args.sword_version, args.expanded, args.ssc, logger)

    # Upload JSON files to S3
    if args.uploadbucket:
        try:
            upload(json_file_list, args.uploadbucket, args.bucketkey, args.datadir, args.expanded, logger)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            logger.info("System exiting.")
            sys.exit(1)

    end = datetime.datetime.now()
    logger.info(f"Execution time: {end - start}")


if __name__ == "__main__":
    combine_data()