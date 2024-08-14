import logging
import os
from arcgis.gis import GIS
from datetime import datetime
import shutil
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def run(backup_directory: str, backup_directory_prefix: str, backup_file_suffix: str, backup_tags: list[str], directory_tags: list[str],  uncategorized_save_tag: str, backup_exclude_types: list[str], directory_permissions: int, date_format: str, archive_number: int, arcgis_username: str, arcgis_password: str, arcgis_login_link: str, delete_backup_online: bool, max_concurrent_downloads: int):
    START_TIME = time.now()
    LOGGER.info("Beginning backup process...")
    
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
    directory_name = backup_directory_prefix + current_date
    full_directory_path = os.path.join(backup_directory, directory_name)
    LOGGER.debug(f"Creating backup directory '{full_directory_path}'.")
    
    try:
        # Create the base directory
        os.makedirs(full_directory_path, mode=directory_permissions, exist_ok=True)
        LOGGER.info(f"Base directory '{full_directory_path}' created successfully.")
        
        # Create each subdirectory inside the base directory
        for name in directory_tags:
            subdirectory_path = os.path.join(full_directory_path, name)
            os.makedirs(subdirectory_path, mode=directory_permissions, exist_ok=False)
            LOGGER.info(f"Subdirectory '{subdirectory_path}' created successfully")
    
    except PermissionError:
        LOGGER.exception(f"Permission denied while creating directory '{full_directory_path}' or its subdirectories.")
    except FileExistsError:
        LOGGER.exception(f"An error occured while creating directory, '{full_directory_path}' already exists.")
        raise    
    except Exception:
        LOGGER.exception(f"An error occurred while creating directory '{full_directory_path}' or its subdirectories.")
    
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
    while len(existing_directories) > archive_number:
        oldest_date = None
        oldest_filename = None

        for filename in existing_directories:
            date = extract_date_from_filename(filename, backup_directory_prefix, date_format)
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
    
    # Search for items with the specified tags
    search_query = "tags:(" + " OR ".join(backup_tags) + ")"
    items = gis.content.search(query=search_query, max_items=1000)
    filtered_items = [item for item in items if item.type not in backup_exclude_types]
    LOGGER.info(f"Found {len(filtered_items)} items with tags {backup_tags}, excluding types {backup_exclude_types}.")
    LOGGER.debug(f"Items found: {[item.title for item in filtered_items]}.")
    # Function to back up an item
    def backup_item(item):
        LOGGER.info(f"Backing up '{item.title}' ({item.type}).")
        try:
            
            # Build item save path
            directory_tag = [tag for tag in item.tags if tag in directory_tags]
            if len(directory_tag) > 1:
                LOGGER.warn(f"Multiple directory tags found for '{item.title}', {directory_tag}.")
                save_tag = directory_tag[0]
            elif len(directory_tag) < 1:
                LOGGER.warn(f"No directory tag found for item '{item.title}'.")
                save_tag = uncategorized_save_tag
            else:
                save_tag = directory_tag[0]
            
            save_path = os.path.join(full_directory_path, save_tag)
            
            
            # Download item
            if item.type in ['Feature Service', 'Vector Tile Service']:
                LOGGER.info(f"Exporting '{item.title}' to GeoDatabase.")
                export_item = item.export(title=item.title + backup_file_suffix, export_format="File Geodatabase")
            LOGGER.info(f"Downloading '{item.title}' to '{save_tag}'.")
            export_item.download(save_path=save_path)
            LOGGER.info(f"Backup complete for '{item.title}'.")
            
            # Optionally, delete the exported item if you don't want to keep it online
            if delete_backup_online:
                export_item.delete()
        except Exception:
            LOGGER.exception(f"Failed to back up {item.title}")
    
    with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
        futures = {executor.submit(backup_item, item): item for item in filtered_items}
        
        for future in as_completed(futures):
            item = futures[future]
            try:
                future.result()
            except Exception:
                LOGGER.exception(f"Exception occurred for '{item.title}'.")
    
    
    END_TIME = time.now()    
    LOGGER.info(f"Backup complete - Items ({len(filtered_items)}), Time ({END_TIME-START_TIME}s)")