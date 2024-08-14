import logging
import os
from arcgis.gis import GIS

LOGGER = logging.getLogger(__name__)




def run(backup_directory: str, backup_tags: list, backup_types: list, date_format: str, archive_number: int, arcgis_username: str, arcgis_password: str, arcgis_login_link: str):
    try:
        gis = GIS(arcgis_login_link, arcgis_username, arcgis_password)
        LOGGER.debug("Successfully connected to ArcGIS Online portal")
    except Exception:
        LOGGER.exception("An error occured connecting to ArcGIS Online portal.")
        raise
    
    