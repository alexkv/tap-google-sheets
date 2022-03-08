import json
import os
import time
import re
from collections import OrderedDict
import urllib.parse
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from singer.messages import RecordMessage
import tap_google_sheets.transform as internal_transform
import tap_google_sheets.schema as schema

# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name which will condition the endpoint called
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#       and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint;
#       default = root (no data_key)

# file_metadata: Queries Google Drive API to get file information and see if file has been modified
#    Provides audit info about who and when last changed the file.
#    cf https://developers.google.com/drive/api/v3/reference/files/get
FILE_METADATA = {
    "key_properties": ["id"],
    "replication_method": "INCREMENTAL",
    "replication_keys": ["modifiedTime"],
    "params": {
        "fileId": "{spreadsheet_id}",
        "fields": "id,name,createdTime,modifiedTime,version,teamDriveId,driveId,lastModifyingUser"
    }
}

# spreadsheet_metadata: Queries spreadsheet to get basic information on spreadhsheet and sheets
#    cf https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
SPREADSHEET_METADATA = {
    "key_properties": ["spreadsheetId"],
    "replication_method": "FULL_TABLE",
    "params": {
        "spreadsheetId": "{spreadsheet_id}"
    }
}

# sheet_metadata: Get Header Row and 1st data row (Rows 1 & 2) from a Sheet on Spreadsheet.
#    This endpoint includes detailed metadata about each cell in the header and first data row
#    incl. data type, formatting, etc.
#    cf https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
SHEET_METADATA = {
    "key_properties": ["sheetId"],
    "replication_method": "FULL_TABLE",
    "params": {
        "spreadsheetId": "{spreadsheet_id}",
        "includeGridData": "true",
        "ranges": "'{sheet_title}'!1:2"
    }
}

# sheets_loaded: Queries a batch of Rows for each Sheet in the Spreadsheet.
#    Each query uses the `values` endpoint, to get data-only, w/out the formatting/type metadata.
#    cf https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
SHEETS_LOADED = {
    "data_key": "values",
    "key_properties": ["spreadsheetId", "sheetId", "loadDate"],
    "replication_method": "FULL_TABLE",
    "params": {
        "spreadsheetId": "{spreadsheet_id}",
        "range": "'{sheet_title}'!{range_rows}",
        "dateTimeRenderOption": "SERIAL_NUMBER",
        "valueRenderOption": "UNFORMATTED_VALUE",
        "majorDimension": "ROWS"
    }

    def sync(self, catalog, state, sheets_loaded_records):
        """
        Write sheets loaded records
        """
        self.state = state
        self.sync_stream(sheets_loaded_records, catalog)


# create OrderDict, as the order matters for syncing the streams
# "file_metadata" -> do not sync other streams, if file is not changed
# "spreadsheet_metadata" -> get sheets in the spreadsheet and load sheet's records
#       and prepare records for "sheet_metadata" and "sheets_loaded" streams
STREAMS = OrderedDict()
STREAMS['file_metadata'] = FileMetadata
STREAMS['spreadsheet_metadata'] = SpreadSheetMetadata
STREAMS['sheet_metadata'] = SheetMetadata
STREAMS['sheets_loaded'] = SheetsLoaded
