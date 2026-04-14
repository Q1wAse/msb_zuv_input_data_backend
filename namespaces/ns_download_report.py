import json, sys, os, inspect
from decimal import Decimal
import datetime
from email.policy import default
from re import match
from pathlib import Path
import pandas as pd
from typing import Any
from io import BytesIO

from werkzeug.datastructures import FileStorage
from sqlalchemy import text, func, select, and_, distinct, or_, column
from sqlalchemy.orm import Session
from flask import jsonify, session, request
from flask_restx import Namespace, Resource, reqparse, fields, inputs

from msb_zuv_input_data_backend.database import cache, errorhandler
from msb_zuv_input_data_backend.config import Config

import msb_zuv_input_data_backend.functions.utility_functions as uf

#==============================================================================================================================
#==============================================================================================================================
ns_download_report = Namespace('ns_download_report', description='download report')
#==============================================================================================================================
#==============================================================================================================================
container_download_report = reqparse.RequestParser()

# Период планирования
# Версия планирования
# Вариант планирования
# Дочернее общество, завод
# Перерабатывающий комплекс
#
# BVR_0CALYEAR_132_01     2026
# BVR_BCBLM001_132_01     22600
# BVR_BCBLM002_132_01     2260010
# BVR_BCBIM002_132_01     38
# BVR_BCBEM0006_132_01    7

container_download_report.add_argument(
    "year",
    default=datetime.date.today().year,
    help="default value: current year",
    type=int
)
@ns_download_report.route('/download_report')
class ClsDownloadReport(Resource):
    @ns_download_report.expect(container_download_report)
    def get(self):
        try:

            uf.clear_loc_log()

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
