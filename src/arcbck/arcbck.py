import logging
import os
from arcgis.gis import GIS
from datetime import datetime


LOGGER = logging.getLogger(__name__)




def run(backup_directory: str, backup_tags: list, backup_types: list, directory_permissions: int, date_format: str, archive_number: int, arcgis_username: str, arcgis_password: str, arcgis_login_link: str, delete_backup_online: bool):
    
    # ----- Connect to arcgis -----
    try:
        gis = GIS(arcgis_login_link, arcgis_username, arcgis_password)
        LOGGER.debug("Successfully connected to ArcGIS Online portal")
    except Exception:
        LOGGER.exception("An error occured connecting to ArcGIS Online portal.")
        raise
    
    # ----- Create backup directory -----
    
    current_date = datetime.now().strftime(date_format)
    directory_name = f"backup_{current_date}"
    full_directory_path = backup_directory + "/" + directory_name
    LOGGER.debug(f"Creating backup directory '{full_directory_path}'.")
    
    try:
        os.makedirs(name=full_directory_path, mode=directory_permissions, exist_ok=False)
    except FileExistsError:
        LOGGER.exception(f"Backup {directory_name} already exists.")
    except Exception:
        LOGGER.exception(f"An error occured making backup directory '{directory_name}'.")
    
    