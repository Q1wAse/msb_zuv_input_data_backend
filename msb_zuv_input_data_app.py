import sys
import os
#===================================================================================================================
sys.path.insert(0, '/usr/lib64/python3.8/site-packages')
sys.path.insert(0, 'opt/foresight')
#===================================================================================================================
import logging
from flask import Flask, jsonify, request
from sqlalchemy import  select, distinct, not_, literal, func, and_

from werkzeug.exceptions import HTTPException, InternalServerError
from werkzeug.utils import secure_filename
from flask_restx import Api, Resource, Namespace
#from flask_cors import CORS
#===================================================================================================================
from msb_zuv_input_data_backend.namespaces.ns_input_map_bs_product import ns_input_data
from msb_zuv_input_data_backend.namespaces.ns_download_report import ns_download_report
from msb_zuv_input_data_backend.config import Config, changelog, secret_key
from msb_zuv_input_data_backend.database import cache
#===================================================================================================================
try:
    from access_control_center.access_control_center_app import validate_requester
    from access_control_center.centrilized_database_pool import database_session_pool, get_session
    from central_logging_system.logger import init_logger
except (ImportError, ModuleNotFoundError):
    validate_requester = None
    database_session_pool = None
    get_session = None
    init_logger = None
#===================================================================================================================
app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = secret_key
#===================================================================================================================
if validate_requester:
    validate_requester(app)
if database_session_pool:
    database_session_pool(app)
if init_logger:
    init_logger(app, app_name="msb_zuv_input_data_backend")
#===================================================================================================================
#CORS(app, resources={r"/*": {"origins" : "*"}})
#===================================================================================================================
api = Api(app,
          version=Config.VERSION,
          title=Config.TITLE
          )
#===================================================================================================================
cache.init_app(app)
#===================================================================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#===================================================================================================================
api.add_namespace(ns_input_data, path='')
api.add_namespace(ns_download_report, path='')
#===================================================================================================================
if Config.SERVERBASE_MODE == 'PYTHON':
    app.run(debug=True)
elif Config.SERVERBASE_MODE == 'WSGI':
    application = app