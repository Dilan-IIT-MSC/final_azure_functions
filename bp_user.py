# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_user) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from typing import Optional

bp_user = func.Blueprint()