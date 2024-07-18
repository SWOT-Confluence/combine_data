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
    # new_key = os.path.basename(i).split('_')[-1].replace('.json', '')
    all_conts = [ continent_dict[os.path.basename(i).split('_')[-1].replace('.json', '')] for i in glob.glob(os.path.join(data_dir, '*reaches*.json')) if os.path.basename(i).split('_')[-1].replace('.json', '') != 'interest']

    
    # # Create new continent file
    with open(os.path.join(data_dir, cont_file), 'w') as jf:
        json.dump(all_conts, jf, indent=2)
    
    # Return continents present
    # print(all_conts[0])
    # print('all conts', list(all_conts[0].keys()))
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
        
        for continent_file in all_continent_files:
            with open(continent_file) as jf:
                data = json.load(jf)
            
            global_file_basename = os.path.basename(continent_file).replace(f'_{continent}.json', '')
            if global_file_basename not in list(out_dict.keys()):
                out_dict[global_file_basename] = []
            out_dict[global_file_basename].extend(data)

    reaches_json_list = []
    for a_key in list(out_dict.keys()):
        outpath = os.path.join(data_dir, a_key + '.json')
        reaches_json_list.append(outpath)
        with open(outpath, 'w') as jf:
            json.dump(out_dict[a_key], jf, indent=2)
            logger.info(f"Written: {outpath}.")

    return reaches_json_list

        


            


            



        
        # # Concatenate continent-level data
        # key_list = json_dict.keys()
        # # if expanded it will be expanded_key
        # # if it is not it will be key
        # # all empty

        # for key in key_list:
        #     if key == "json_files" or key == "continent" or key == "reaches" or key=="basin": continue
        #     try:
        #         json_dict = read_json_data(data_dir=data_dir, continent=continent, filename=key, json_dict=json_dict, sword_version = sword_version)
        #     except Exception as e:
        #         print(e)
        #         print('failed to read', key, 'for', continent)

        #     if key == 's3_list':
        #         json_dict[key] = parse_duplicate_files(json_dict[key])
            
        #     elif key == 's3_reach':
        #         for reach_id in json_dict[key].keys():
        #             json_dict[key][reach_id] = parse_duplicate_files(json_dict[key][reach_id])

        #     # elif key == 'expanded_reaches_of_interest' or key == 'reaches':
                
        #     # add in original reaches of interest
        #     elif key == 'expanded_reaches_of_interest':
        #         expanded_reaches = list(json_dict[key])
        #         with open(os.path.join(data_dir, 'reaches_of_interest.json')) as f:
        #             base_reaches = json.load(f)
        #             expanded_reaches.extend(base_reaches)
        #             reach_list = list(set(expanded_reaches))
        #             json_dict[key] = reach_list


        # base_reaches = [int(os.path.basename(i).split('_')[0]) for i in glob.glob(os.path.join(data_dir, 'swot', '*.nc'))]
        # reaches_dict = parse_reach_list_for_output(reach_list=base_reaches,sword_version=sword_version)
        # json_dict['reaches'] = reaches_dict

        # basin_ids = set(list(map(lambda x: int(str(x)[0:4]), base_reaches)))

        # basin_data = list(map(lambda basin_id: create_basin_data(basin_id, base_reaches, sword_version), basin_ids))
        # json_dict['basin'] = basin_data

        
    return json_dict

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

def log_totals(continents, json_dict, logger):
    """Log different totals."""
    
    for key in json_dict.keys():
        try:
            logger.info(f"Number of objects in {key}:{len(json_dict[key])} ")
        except:
            pass
    # logger.info(f"Number of continents: {len(continents):,}. Continents present: {continents}.")
    # logger.info(f"Number of basins: {len(json_dict['basin']):,}.")
    # logger.info(f"Number of reaches: {len(json_dict['reaches']):,}.")
    # logger.info(f"Number of HiVDI sets: {len(json_dict['hivdisets']):,}.")
    # logger.info(f"Number of MetroMan sets: {len(json_dict['metrosets']):,}.")
    # logger.info(f"Number of sic4DVar sets: {len(json_dict['sicsets']):,}.")

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
    if 'cycle_passes' in list(json_dict.keys()):
        json_dict["cycle_passes"] = OrderedDict(sorted(json_dict["cycle_passes"].items(), key=sort_cycle_pass))
    if 'passes' in list(json_dict.keys()): 
        json_dict["passes"] = OrderedDict(sorted(json_dict["passes"].items(), key=sort_cycle_pass)) 
    
    # Write global JSON files
    json_file_list = []
    for key, value in json_dict.items():
        if key == "json_files": 
            continue
        elif key == "continent": 
            json_file_list.append(value)
        # elif key == 'reaches':
        #     # Convert each dictionary to a frozenset of its items to make it hashable
        #     # Use a set to remove duplicates
        #     unique_dicts = set(frozenset(d.items()) for d in value)

        #     # Convert frozenset objects back to dictionaries
        #     value = [dict(d) for d in unique_dicts]
        elif key == 'expanded_reaches_of_interest':
            value == list(set(value))
            

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
        return parsed



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
    
    ## Lists to populate
    # json_dict = {
    #     "basin" : [],
    #     "continent": pathlib.Path(args.datadir).joinpath(args.contfile),
    #     # "cycle_passes" : {},
    #     "hivdisets" : [],
    #     "metrosets" : [],
    #     "neosets" :[],
    #     # "passes" : {},
    #     # "reach_node" : [],
    #     "reaches" : [],
    #     # "s3_list" : [],
    #     # "s3_reach": {},
    #     "sicsets" : [],
    #     "json_files": []
    # }
    

    # if args.expanded:
    #     key_list = ['expanded_'+i if i != 'reaches' else 'expanded_reaches_of_interest' for i in list(json_dict.keys())]
    #     # key_list = ['expanded_'+i if i != 'reaches' for i in list(json_dict.keys())]
    #     json_dict = {s: [] for s in key_list}



    # Combine continent-level data
    json_file_list = combine_continents(continents, args.datadir, args.sword_version, args.expanded,logger)
    
    # Write out global json data
    # json_file_list = write_json(pathlib.Path(args.datadir), json_dict, logger)
    
    # Upload JSON files to S3
    if args.uploadbucket:

        try:
            upload(json_file_list, args.uploadbucket, logger)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            logger.info("System exiting.")
            sys.exit(1)
    
    # # Delete continent-level data
    # if args.delete:
    #     delete_continent_json(json_dict["json_files"], logger)
        
    # # Log different totals
    # log_totals(continents, json_dict, logger)
        
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")
 
    
if __name__ == "__main__":
    combine_data()