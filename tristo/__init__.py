# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:28:22 2022

@author: Leon
"""
from .cleaner import tables_to_db
from .converter import extract_tables, update_file_info
from .database import make_file_index, mark_data
from .scraper import scrape_pages
from .utils import hashf
