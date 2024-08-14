import logging
import os
from arcgis.gis import GIS
from datetime import datetime
import shutil
import re
LOGGER = logging.getLogger(__name__)

def convert_date_format_to_regex(date_format: str) -> str:
    """Convert a date format string to a regular expression pattern."""
    format_map = {
        '%Y': r'\d{4}',      # Year
        '%m': r'\d{2}',      # Month
        '%d': r'\d{2}',      # Day
        '%H': r'\d{2}',      # Hour (if needed)
        '%M': r'\d{2}',      # Minute (if needed)
        '%S': r'\d{2}'       # Second (if needed)
    }
    
    regex_pattern = date_format
    for py_format, regex in format_map.items():
        regex_pattern = regex_pattern.replace(py_format, regex)
    
    return regex_pattern

def extract_date_from_filename(filename: str, prefix: str, date_format: str):
    # Convert date format to regex pattern
    date_regex = convert_date_format_to_regex(date_format)
    
    # Define the regular expression pattern for the filename format
    pattern = re.compile(rf'{re.escape(prefix)}({date_regex})')
    match = pattern.search(filename)
    
    if match:
        date_str = match.group(1)  # Extract the date portion
        try:
            # Convert the extracted date string to a datetime object
            return datetime.strptime(date_str, date_format)
        except ValueError:
            print(f"Error: Date format does not match for filename '{filename}'")
    else:
        print(f"Filename '{filename}' does not match the expected format.")
    
    return None


def run(backup_directory: str, backup_prefix: str, backup_tags: list[str], backup_exclude_types: list[str], directory_permissions: int, date_format: str, archive_number: int, arcgis_username: str, arcgis_password: str, arcgis_login_link: str, delete_backup_online: bool):
    LOGGER.INFO("Beginning backup process...")
    
    # ----- Connect to arcgis -----
    LOGGER.info("Connecting to ArcGIS...")
    try:
        gis = GIS(arcgis_login_link, arcgis_username, arcgis_password)
        LOGGER.debug("Successfully connected to ArcGIS Online portal")
    except Exception:
        LOGGER.exception("An error occured connecting to ArcGIS Online portal.")
        raise
    
    # ----- Create backup directory -----
    LOGGER.info("Creating backup directory...")
    current_date = datetime.now().strftime(date_format)
    directory_name = backup_prefix + current_date
    full_directory_path = os.path.join(backup_directory, directory_name)
    LOGGER.debug(f"Creating backup directory '{full_directory_path}'.")
    
    try:
        os.makedirs(name=full_directory_path, mode=oct(directory_permissions), exist_ok=False)
    except FileExistsError:
        LOGGER.exception(f"Backup {directory_name} already exists.")
    except PermissionError:
        LOGGER.exception(f"Permission denied while creating backup directory '{full_directory_path}'.")
    except Exception:
        LOGGER.exception(f"An error occured making backup directory '{directory_name}'.")
    
    # ----- Delete old backups -----
    LOGGER.info("Removing old backups...")
    # Get contents of backup directory
    try:
        entries = os.listdir(path=backup_directory)
    except FileNotFoundError:
        LOGGER.exception(f"Directory '{backup_directory}' not found.")
    except NotADirectoryError:
        LOGGER.exception(f"Error listing contents of directory. '{backup_directory}' is not a directory.")
    except PermissionError:
        LOGGER.exception(f"Permission denied for getting contents of directory '{backup_directory}'.")
    except Exception:
        LOGGER.exception(f"An error occured getting contents of directory '{backup_directory}'.")
        
    # Filter out entries that are not directories
    existing_directories = [entry for entry in entries if os.path.isdir(os.path.join(backup_directory, entry))]
    
    # Delete old backup folders
    while len(existing_directories > archive_number):
        oldest_date = None
        oldest_filename = None

        for filename in existing_directories:
            date = extract_date_from_filename(filename, backup_prefix, date_format)
            if date:
                if oldest_date is None or date < oldest_date:
                    oldest_date = date
                    oldest_filename = filename

        try:
            delete_path = os.path.join(backup_directory, oldest_filename)
            shutil.rmtree(path=delete_path)
            LOGGER.log(f"Removed backup directory '{oldest_filename}'.")
        except FileNotFoundError:
            LOGGER.exception(f"Directory '{delete_path}' not found.")
        except PermissionError:
            LOGGER.exception(f"Permission denied for deleting '{delete_path}'.")
        except Exception:
            LOGGER.exception(f"An error occured deleting directory '{delete_path}'.")
    
    # ----- Start backup -----
    LOGGER.info("Backing up files...")