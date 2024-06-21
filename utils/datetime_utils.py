from datetime import datetime
import pytz

local_tz_str = "America/Chicago"

def local_to_utc_date(local_time_str):
    local_tz = pytz.timezone(local_tz_str)
    local_time = datetime.strptime(local_time_str, "%Y-%m-%d")
    local_time = local_tz.localize(local_time)
    utc_time = local_time.astimezone(pytz.utc)
    return utc_time.strftime("%Y-%m-%d")

def local_to_utc(local_time_str):
    local_tz = pytz.timezone(local_tz_str)
    local_time = datetime.strptime(local_time_str, "%Y-%m-%dT%H:%M:%S")
    local_time = local_tz.localize(local_time)
    utc_time = local_time.astimezone(pytz.utc)
    return utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")

def utc_to_local(utc_time_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_time = pytz.utc.localize(utc_time)
    local_tz = pytz.timezone(local_tz_str)
    local_time = utc_time.astimezone(local_tz)
    return local_time.strftime("%Y-%m-%dT%H:%M:%SZ")

def get_current_local_date():
    chicago_time = datetime.now(pytz.timezone(local_tz_str))
    formatted_date = chicago_time.strftime('%Y-%m-%d')
    return formatted_date

