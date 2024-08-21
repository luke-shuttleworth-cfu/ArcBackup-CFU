08-19-2024

# ArcGIS Online Backup Script

This script is designed to back up items from ArcGIS Online based on specified tags. It supports creating backups of various item types and organizing them into directories with optional filtering and deletion of old backups.

## Features

- **Directory Creation**: Automatically creates a backup directory with the current date, and subdirectories based on tags.
- **Old Backup Deletion**: Deletes old backups beyond a specified number to manage disk space.
- **Item Backup**: Backs up items based on specified tags, excluding certain item types if needed.
- **Error Handling and Logging**: Logs all actions, errors, and retries, and saves a JSON log file with detailed information about the backup process.
- **Multi-threading**: Uses a queue to manage the backup of items, allowing retries and parallel processing.

## Requirements

- Python 3.x
- `arcgis` Python package
- `shutil`, `os`, `re`, `time`, `uuid`, `json`, and `queue` standard libraries.

## Usage

1. **Setup GIS**: Initialize the GIS object using the `arcgis.gis.GIS` module.

2. **Define Parameters**:
   - `backup_directory`: The root directory where backups will be stored.
   - `backup_directory_prefix`: Prefix for the backup directory name.
   - `backup_file_suffix`: Suffix for the backup filenames.
   - `backup_tags`: List of tags to search for items to back up.
   - `directory_tags`: List of tags used to organize items into subdirectories.
   - `uncategorized_save_tag`: Tag for items without a matching directory tag.
   - `backup_exclude_types`: List of item types to exclude from backup.
   - `date_format`: Date format used for naming directories and extracting dates from filenames.
   - `archive_number`: Number of old backups to keep before deleting.
   - `delete_backup_online`: Boolean to determine if exported items should be deleted from ArcGIS Online.
   - `ignore_existing`: Boolean to determine if items with existing backups should be ignored.
   - `export_delay`: Delay between export attempts in seconds.
   - `max_retries`: Maximum number of retries for each item.

3. **Run the Script**:
   - Call the `run()` function with the necessary parameters.
   - The script will create the necessary directories, back up the items, and delete old backups as specified.

4. **Check Logs**:
   - Logs are saved as JSON files in the backup directory.
   - Check the logs for detailed information about the backup process.

## Example

```python
from arcgis.gis import GIS

gis = GIS("https://www.arcgis.com", "username", "password")

backup_log = run(
    backup_directory="/path/to/backup",
    backup_directory_prefix="backup_",
    backup_file_suffix="_backup_",
    backup_tags=["tag1", "tag2"],
    directory_tags=["tag1", "tag2"],
    uncategorized_save_tag="uncategorized",
    backup_exclude_types=["Feature Service"],
    date_format="%Y-%m-%d",
    archive_number=3,
    gis=gis,
    delete_backup_online=True,
    ignore_existing=False,
    export_delay=2,
    max_retries=5
)