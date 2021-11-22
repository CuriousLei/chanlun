# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 22:36:52 2021

@author: leida
"""

import tushare as ts
import requests
import time
import pandas as pd
import sqlite3
import talib as tl
import json
from pathlib import Path
from .dataOperation import FileOperation, formerComplexRights
from concurrent.futures import ThreadPoolExecutor
#import pymysql