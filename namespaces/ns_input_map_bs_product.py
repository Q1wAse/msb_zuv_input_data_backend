import json, sys, os, inspect
from decimal import Decimal
from datetime import datetime, date
from email.policy import default
from re import match
from pathlib import Path
import pandas as pd
from typing import Any

from io import BytesIO

from scripts.regsetup import description
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
tab_mutable_keys = tuple([ key for key, config in uf.TABLES_MAP.items() if config.get('mutable', False) ])
tab_enum_keys = tuple(uf.TABLES_MAP.keys())
#==============================================================================================================================
#==============================================================================================================================
ns_input_data = Namespace('ns_input_data', description='get/input data')
#==============================================================================================================================
#==============================================================================================================================
container_get_struct_tab_data = reqparse.RequestParser()
container_get_struct_tab_data.add_argument(
    "tab_id",
    default=tab_enum_keys[0],
    type=str,
    required=True,
    choices=tab_enum_keys
    # help=f"Enum keys: {', '.join(tab_enum_keys)}"
)
@ns_input_data.route('/get_struct')
class ClsGetStructTabData(Resource):
    @ns_input_data.expect(container_get_struct_tab_data)
    def get(self):
        try:
            param_list: dict = container_get_struct_tab_data.parse_args()

            uf.clear_loc_log()

            v_tab_id = uf.validate_param(param_list, "tab_id")

            return uf.get_struct_table(v_tab_id)

        except Exception as e:
            ns_input_data.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
container_pagin_data = reqparse.RequestParser()
container_pagin_data.add_argument(
    "tab_id",
    default=tab_enum_keys[0],
    type=str,
    required=True,
    choices=tab_enum_keys
    # help=f"Enum keys: {', '.join(tab_enum_keys)}"
)
container_pagin_data.add_argument(
    "filter",
    type=str
)
container_pagin_data.add_argument(
    "page",
    default=1,
    type=int
)
container_pagin_data.add_argument(
    "limit",
    default=50,
    help="1 <= limit <= 100",
    type=int
)
@ns_input_data.route('/get')
class ClsPaginData(Resource):
    @ns_input_data.expect(container_pagin_data)
    def get(self):
        try:
            param_list : dict = container_pagin_data.parse_args()

            uf.clear_loc_log()

            v_tab_id = uf.validate_param(param_list, "tab_id")
            v_filter = uf.validate_param(param_list,"filter")
            v_page = uf.validate_param(param_list,  "page")
            v_limit = uf.validate_param(param_list, "limit")

            return { "message" : str(uf.get_pagin_data(v_tab_id, v_filter, v_page, v_limit))} , 200

        except Exception as e:
            ns_input_data.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
container_patch_data = reqparse.RequestParser()
container_patch_data.add_argument(
    "tab_id",
    default=tab_mutable_keys[0],
    type=str,
    required=True,
    choices=tab_mutable_keys
    # help=f"Enum keys: {', '.join(tab_enum_keys)}"
)
@ns_input_data.route('/patch')
class ClsPatchMapBsProductData(Resource):
    @ns_input_data.expect(container_patch_data)
    def patch(self):
        try:
            # request.get_json()
            data_list = ns_input_data.payload

            param_list: dict = container_patch_data.parse_args()

            uf.clear_loc_log()

            v_tab_id = uf.validate_param(param_list, "tab_id")

            # data_list = [
            #     {
            #         "id": 98,
            #         "koef": 2.0000000000000000000,
            #         "factory": 998
            #     },
            #     {
            #         "id": 99,
            #         "name": "99 имя 999",
            #         "id_product": 999
            #     },
            #     {
            #         "id": 97,
            #         "name": "97 err name",
            #         "id_product": 997
            #     }
            # ]

            if not isinstance(data_list, list):
                return uf.get_msg_struct(uf.EnumMsg.SYSTEM_ERROR)

            return uf.patch_data(v_tab_id, data_list)

        except Exception as e:
            ns_input_data.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================