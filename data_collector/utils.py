import re
import logging
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def split_list_into_chunks(lst, chunk_size):
    """Splits a list into given chunk sizes"""
    for idx in range(0, len(lst), chunk_size):
        yield lst[idx:idx + chunk_size]


def parse_timerange(from_date_dt: datetime, to_dt: datetime):
    """pareses dates and returns UTC formats"""
    try:
        from_date = datetime.utcfromtimestamp(from_date_dt)
        to = datetime.utcfromtimestamp(to_dt)
    except ValueError:
        logger.info("Invalid date format")
        exit(1)
    if from_date >= to:
        logger.info("Start date must be before end date")
        exit(1)
    return from_date, to

def compile_exclude_patterns(patterns_str: str) -> List[re.Pattern]:
    """Compiles the patterns to be excluded"""
    if not patterns_str:
        return []
    return [re.compile(pattern.strip()) for pattern in patterns_str.split(",")]

def should_exclude(metric_name: str, patterns: List[re.Pattern]) -> bool:
    """Return a boolean on exclusion decision"""
    return any(p.search(metric_name) for p in patterns)

def remove_keys_by_patterns(data: Dict, patterns: List[str]) -> Dict:
    """Removes keys in a dict based on regex list"""
    regexes = [re.compile(p) for p in patterns]
    return {
        k: v for k, v in data.items()
        if not any(r.match(k) for r in regexes)
    }

def recursively_flatten_dict(obj: dict) -> dict:
    """Recursively flatten the dictionary, if a key is repeated it will be overwritten"""
    new_dict = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, dict):
                new_dict.update(recursively_flatten_dict(v))
            else:
                new_dict[k] = v
    return new_dict