import logging
import os
from arcgis.gis import GIS
from datetime import datetime
import shutil
import re
import time
import queue
import threading
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


def run(backup_directory: str, backup_directory_prefix: str, backup_file_suffix: str, backup_tags: list[str], directory_tags: list[str],  uncategorized_save_tag: str, backup_exclude_types: list[str], directory_permissions: int, date_format: str, archive_number: int, arcgis_api_key: str | None, arcgis_username: str | None, arcgis_password: str | None, arcgis_login_link: str | None, delete_backup_online: bool, max_concurrent_downloads: int, export_delay=2, max_retries=5):
    START_TIME = time.time()
    LOGGER.info("Beginning backup process...")
    
    # ----- Connect to arcgis -----
    LOGGER.info("Connecting to ArcGIS...")
    try:
        if arcgis_username and arcgis_password and arcgis_login_link:
            gis = GIS(arcgis_login_link, arcgis_username, arcgis_password)
        elif arcgis_api_key and arcgis_login_link:
            gis = GIS(url=arcgis_login_link, api_key=arcgis_api_key)
        else:
            raise ValueError("No ArcGIS credentials specified")
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
        os.makedirs(full_directory_path, mode=directory_permissions, exist_ok=False)
        LOGGER.info(f"Base directory '{full_directory_path}' created successfully.")
        
        # Create each subdirectory inside the base directory
        for name in directory_tags:
            subdirectory_path = os.path.join(full_directory_path, name)
            os.makedirs(subdirectory_path, mode=directory_permissions, exist_ok=True)
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
    
    
    # Delete old backup folders
    while True:
        # Get contents of backup directory
        try:
            entries = os.listdir(path=backup_directory)
        except FileNotFoundError:
            LOGGER.exception(f"Directory '{backup_directory}' not found.")
            raise
        except NotADirectoryError:
            LOGGER.exception(f"Error listing contents of directory. '{backup_directory}' is not a directory.")
            raise
        except PermissionError:
            LOGGER.exception(f"Permission denied for getting contents of directory '{backup_directory}'.")
            raise
        except Exception:
            LOGGER.exception(f"An error occured getting contents of directory '{backup_directory}'.")
            raise
            
        # Filter out entries that are not directories
        existing_directories = [entry for entry in entries if os.path.isdir(os.path.join(backup_directory, entry))]
        
        if len(existing_directories) < archive_number:
            break
        
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
    backup_count = [0]
    count_lock = threading.Lock()
    # Search for items with the specified tags
    search_query = "tags:(" + " OR ".join(backup_tags) + ")"
    items = gis.content.search(query=search_query, max_items=100)
    filtered_items = [item for item in items if item.type not in backup_exclude_types]
    found_items = len(filtered_items)
    if found_items > 0:  
        LOGGER.info(f"Found {found_items} items with tags {backup_tags}, excluding types {backup_exclude_types}.")
        LOGGER.debug(f"Items found: {[item.title for item in filtered_items]}.")
    else:
        LOGGER.error(f"Found {found_items} items with tags {backup_tags}, excluding types {backup_exclude_types}. Aborting backup.")
        exit()
    # Function to back up an item
    def backup_item(item, thread_logger):
        thread_logger.info(f"Backing up '{item.title}' ({item.type}). ({backup_count[0]}/{found_items})")
        try:
            
            # Build item save path
            directory_tag = [tag for tag in item.tags if tag in directory_tags]
            if len(directory_tag) > 1:
                thread_logger.warn(f"Multiple directory tags found for '{item.title}', {directory_tag}.")
                save_tag = directory_tag[0]
            elif len(directory_tag) < 1:
                thread_logger.warn(f"No directory tag found for item '{item.title}'.")
                save_tag = uncategorized_save_tag
            else:
                save_tag = directory_tag[0]
            
            save_path = os.path.join(full_directory_path, save_tag)
            
            # Ensure thread-safe directory creation
            if not os.path.exists(save_path):
                with threading.Lock():
                    if not os.path.exists(save_path):
                        os.makedirs(save_path, mode=directory_permissions)
            
            # Download item
            if item.type in ['Feature Service', 'Vector Tile Service']:
                thread_logger.info(f"Exporting '{item.title}' to GeoDatabase.")
                delete = True
                item_filename = item.title + backup_file_suffix
                count = 1
                while(os.path.exists(item_filename)):
                    item_filename = item.title + backup_file_suffix + "(" + str(count) + ")"
                    count += 1
                export_item = item.export(title=item_filename, export_format="File Geodatabase")
            else:
                thread_logger.debug(f"The type of item '{item.title}' ({item.type}) does not have export capababilities.")
                delete = False
                export_item = item
            thread_logger.info(f"Downloading '{item.title}' to '{save_tag}'.")
            export_item.download(save_path=save_path)
            
            
            with count_lock:
                backup_count[0] += 1
            thread_logger.info(f"Backup complete for '{item.title}'. ({backup_count[0]}/{found_items})")
            # Optionally, delete the exported item if you don't want to keep it online
            if delete_backup_online and delete:
                export_item.delete()
                
        except Exception:
            thread_logger.exception(f"Error with '{item.title}'.")
            if delete_backup_online and delete:
                try:
                    export_item.delete()
                except UnboundLocalError as e:
                    thread_logger.debug(f"Export item was not yet created. {e}")
    
    # ----- Start threads -----
    LOGGER.info("Starting threads...") 
    # Create a queue to hold items to be processed
    request_queue = queue.Queue()
    # Add items to the queue
    for item in filtered_items:
        # Add a tuple for each item containing the item and the number of retries
        request_queue.put([item, 0])
        
        
    def worker(thread_logger):
        while True:
            item = request_queue.get()
            
            try:
                if item[1] >= max_retries:
                    break
                if item is None:
                    break  # Stop the thread if there are no more items
                backup_item(item[0], thread_logger)
            except Exception as e:
                thread_logger.info(f"An exception occured, putting item back in queue. {e}.")
                item[1] += 1
                request_queue.put(item)
            finally:
                request_queue.task_done()
                # Introduce a delay between requests
                time.sleep(export_delay)  # Adjust delay as needed

    # Start worker threads
    threads = []
    for _ in range(max_concurrent_downloads):
        thread_logger = logging.getLogger(__name__ + "." + item.title + ".thread")
        thread = threading.Thread(target=worker, args=(thread_logger,))
        thread.start()
        threads.append(thread)

    

    # Block until all tasks are done
    request_queue.join()

    # Stop workers
    for _ in range(max_concurrent_downloads):
        request_queue.put(None)
    for thread in threads:
        thread.join()
    
    END_TIME = time.time()    
    LOGGER.info(f"Backup complete - Items ({backup_count[0]}/{found_items}), Time ({END_TIME-START_TIME}s)")