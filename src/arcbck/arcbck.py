import logging
import os
from arcgis.gis import GIS
from datetime import datetime
import shutil
import re
import time
import queue
import uuid
import json
LOGGER = logging.getLogger(__name__)

backup_log = {
    'info': {
        'date': None,
        'directory': None,
        'success': None,
        'total items': None,
        'backed up items': 0,
        'size': None
    },
    'items': {}
}


def _convert_date_format_to_regex(date_format: str) -> str:
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


def _extract_date_from_filename(filename: str, prefix: str, date_format: str):
    # Convert date format to regex pattern
    date_regex = _convert_date_format_to_regex(date_format)

    # Define the regular expression pattern for the filename format
    pattern = re.compile(rf'{re.escape(prefix)}({date_regex})')
    match = pattern.search(filename)

    if match:
        date_str = match.group(1)  # Extract the date portion
        try:
            # Convert the extracted date string to a datetime object
            return datetime.strptime(date_str, date_format)
        except ValueError:
            print(
                f"Error: Date format does not match for filename '{filename}'")
    else:
        print(f"Filename '{filename}' does not match the expected format.")

    return None


def _save_json_log(path: str, name: str):
    path = os.path.join(path, name + '.json')
    with open(path, 'w') as f:
        f.write(json.dumps(backup_log, indent=4))


def run(backup_directory: str, backup_directory_prefix: str, backup_file_suffix: str, backup_tags: list[str], directory_tags: list[str],  uncategorized_save_tag: str, backup_exclude_types: list[str], date_format: str, archive_number: int, gis: GIS, delete_backup_online: bool, ignore_existing: bool, export_delay=2, max_retries=5) -> dict:
    START_TIME = time.time()
    LOGGER.info("Beginning backup process...")

    # ----- Create backup directory -----
    LOGGER.info("Creating backup directory...")
    current_date = datetime.now().strftime(date_format)
    directory_name = backup_directory_prefix + current_date
    full_directory_path = os.path.join(backup_directory, directory_name)

    LOGGER.debug(f"Creating backup directory '{full_directory_path}'.")

    try:
        # Create the base directory
        os.makedirs(full_directory_path, exist_ok=False)
        LOGGER.info(
            f"Base directory '{full_directory_path}' created successfully.")

        # Create each subdirectory inside the base directory
        for name in directory_tags:
            subdirectory_path = os.path.join(full_directory_path, name)
            os.makedirs(subdirectory_path, exist_ok=True)
            LOGGER.info(
                f"Subdirectory '{subdirectory_path}' created successfully")
        backup_log['info']['date'] = current_date
        backup_log['info']['directory'] = full_directory_path
        _save_json_log(full_directory_path, directory_name)

    except PermissionError:
        LOGGER.exception(
            f"Permission denied while creating directory '{full_directory_path}' or its subdirectories.")
    except FileExistsError:
        LOGGER.exception(
            f"An error occured while creating directory, '{full_directory_path}' already exists.")
        raise
    except Exception:
        LOGGER.exception(
            f"An error occurred while creating directory '{full_directory_path}' or its subdirectories.")

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
            LOGGER.exception(
                f"Error listing contents of directory. '{backup_directory}' is not a directory.")
            raise
        except PermissionError:
            LOGGER.exception(
                f"Permission denied for getting contents of directory '{backup_directory}'.")
            raise
        except Exception:
            LOGGER.exception(
                f"An error occured getting contents of directory '{backup_directory}'.")
            raise

        # Filter out entries that are not directories
        existing_directories = [entry for entry in entries if os.path.isdir(
            os.path.join(backup_directory, entry))]
        LOGGER.debug(f'Found {len(existing_directories)} directories.')
        if len(existing_directories) - 1 < archive_number:
            break

        oldest_date = None
        oldest_filename = None

        for filename in existing_directories:
            date = _extract_date_from_filename(
                filename, backup_directory_prefix, date_format)
            if date:
                if oldest_date is None or date < oldest_date:
                    oldest_date = date
                    oldest_filename = filename

        try:
            delete_path = os.path.join(backup_directory, oldest_filename)
            shutil.rmtree(path=delete_path)
            LOGGER.info(f"Removed backup directory '{oldest_filename}'.")
        except FileNotFoundError:
            LOGGER.exception(f"Directory '{delete_path}' not found.")
            raise
        except PermissionError:
            LOGGER.exception(
                f"Permission denied for deleting '{delete_path}'.")
            raise
        except Exception:
            LOGGER.exception(
                f"An error occured deleting directory '{delete_path}'.")
            raise

    LOGGER.info("Old backups deleted.")

    # ----- Start backup -----
    LOGGER.info("Backing up files...")
    backup_count = [0]
    # Search for items with the specified tags
    search_query = "tags:(" + " OR ".join(backup_tags) + ")"
    items = gis.content.search(query=search_query, max_items=1000)
    existing_backup_pattern = rf"{backup_file_suffix}[0-9a-fA-F]{32}"
    filtered_items = [item for item in items if item.type not in backup_exclude_types and (
        re.search(existing_backup_pattern, item.title) is None and ignore_existing)]
    found_items = len(filtered_items)
    backup_log['info']['total items'] = found_items
    _save_json_log(full_directory_path, directory_name)
    if found_items > 0:
        LOGGER.info(
            f"Found {found_items} items with tags {backup_tags}, excluding types {backup_exclude_types}.")
        LOGGER.info(f"Items found: {[item.title for item in filtered_items]}.")
    else:
        LOGGER.error(
            f"Found {found_items} items with tags {backup_tags}, excluding types {backup_exclude_types}. Aborting backup.")
        exit()

    for item in filtered_items:
        backup_log['items'][str(item.id)] = {
            'title': item.title,
            'type': item.type,
            'tags': item.tags,
            'status': None,
            'success': False,
            'retries': 0,
            'error': None,
            'backup_path': None
        }
        _save_json_log(full_directory_path, directory_name)

    # Function to back up an item

    def backup_item(item):
        backup_count[0] += 1
        # Function to delete an item

        def delete_item(item_name: str):
            items = gis.content.search(
                query=f"title:{item_name}", max_items=1000)
            if items:
                for item in items:
                    try:
                        item.delete()
                        LOGGER.debug(
                            f"Deleted item '{item_name}' successfully.")
                    except Exception:
                        LOGGER.exception(f"Error deleting item '{item_name}'.")
            else:
                LOGGER.debug(f"Unable to delete, '{item.title}' not found.")
        LOGGER.info(
            f"Backing up '{item.title}' ({item.type}). ({backup_count[0]}/{found_items})")
        backup_log['items'][str(item.id)]['status'] = 'BACKING'
        _save_json_log(full_directory_path, directory_name)
        try:

            # Build item save path
            directory_tag = [tag for tag in item.tags if tag in directory_tags]
            if len(directory_tag) > 1:
                LOGGER.warn(
                    f"Multiple directory tags found for '{item.title}', {directory_tag}.")
                save_tag = directory_tag[0]
            elif len(directory_tag) < 1:
                LOGGER.warn(f"No directory tag found for item '{item.title}'.")
                save_tag = uncategorized_save_tag
            else:
                save_tag = directory_tag[0]

            save_path = os.path.join(full_directory_path, save_tag)

            # Ensure thread-safe directory creation
            if not os.path.exists(save_path):
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
            item_filename = item.title + backup_file_suffix + uuid.uuid4().hex
            # Download item
            if item.type in ['Feature Service', 'Vector Tile Service']:
                LOGGER.info(f"Exporting '{item.title}' to GeoDatabase.")
                delete = True

                backup_log['items'][str(item.id)]['status'] = 'EXPORTING'
                _save_json_log(full_directory_path, directory_name)
                export_item = item.export(
                    title=item_filename, export_format="File Geodatabase")
            else:
                LOGGER.debug(
                    f"The type of item '{item.title}' ({item.type}) does not have export capababilities.")
                delete = False
                export_item = item
            LOGGER.info(f"Downloading '{item.title}' to '{save_tag}'.")

            # make this check/dynamically expand

            backup_log['items'][str(item.id)]['status'] = 'DOWNLOADING'
            backup_log['items'][str(item.id)]['backup_path'] = os.path.join(
                save_path, item_filename)
            _save_json_log(full_directory_path, directory_name)
            export_item.download(save_path=save_path)

            LOGGER.info(
                f"Backup complete for '{item.title}'. ({backup_count[0]}/{found_items})")
            # Optionally, delete the exported item if you don't want to keep it online
            if delete_backup_online and delete:
                delete_item(item_filename)
        except Exception:
            LOGGER.error(f"Error with '{item.title}'.")
            backup_count[0] -= 1
            if delete_backup_online and delete:
                delete_item(item_filename)
            raise

    # ----- Start threads -----
    LOGGER.info("Starting backups...")
    # Create a queue to hold items to be processed
    request_queue = queue.Queue()
    # Add items to the queue
    for item in filtered_items:
        # Add a tuple for each item containing the item and the number of retries
        request_queue.put([item, 0])

    while not request_queue.empty():
        item = request_queue.get()
        if item is None:
            break
        if item[1] < max_retries:
            try:
                backup_item(item[0])
                backup_log['items'][str(item[0].id)]['success'] = True
                backup_log['items'][str(item[0].id)]['status'] = 'COMPLETE'
                _save_json_log(full_directory_path, directory_name)
            except Exception as e:
                LOGGER.exception(
                    f"An error occured with item '{item[0].title}'.")
                item[1] += 1
                backup_log['items'][str(item[0].id)]['retries'] += 1
                backup_log['items'][str(item[0].id)]['error'] = e
                _save_json_log(full_directory_path, directory_name)
                request_queue.put(item)

    def get_folder_size(folder_path):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        return total_size

    backup_log['info']['size'] = get_folder_size(
        full_directory_path) / (1024 * 1024 * 1024)
    backup_log['info']['backed up items'] = backup_count[0]

    for key, value in backup_log['items'].items():
        value = value['success']
        if not value:
            backup_log['info']['success'] = False
            break
        backup_log['info']['success'] = True

    _save_json_log(full_directory_path, directory_name)

    END_TIME = time.time()
    LOGGER.info(
        f"Backup complete - Items ({backup_count[0]}/{found_items}), Time ({END_TIME-START_TIME}s)")
    return backup_log
