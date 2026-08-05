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
import msb_zuv_input_data_backend.functions.funcs_mirror as funcs_mirror

#==============================================================================================================================
#==============================================================================================================================
ns_download_report = Namespace('ns_download_report', description='download report')
#==============================================================================================================================
#==============================================================================================================================
# container_download_report = reqparse.RequestParser()

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

column_model = ns_download_report.model('ReportColumn', {
    'typeData': fields.String(description='Тип данных', required=True),
    'versionPlaning': fields.String(description='Версия планирования', required=True),
    'variantPlaning': fields.String(description='Вариант планирования', required=True),
    'IsByear': fields.Boolean(description='Это трёхлетка', required=False),
    'dateRange': fields.List(
        fields.String,
        description='Диапазон дат [начало, конец]',
        required=True
    )
})

download_report_model = ns_download_report.model('ContainerReport', {
    'selectedTypeDownload': fields.String(
        description='Тип выгрузки',
        required=True,
        default='full',
        example='full'
    ),
    'selectedFactories': fields.List(
        fields.String,
        description='Список выбранных заводов',
        required=True,
        example=["1"]
        # example=["1", "2", "3"]
    ),
    'selectedReports': fields.List(
        fields.String,
        description='Список выбранных отчетов',
        required=True,
        example=[]
        # example=["1","2"]
    ),
    'columns': fields.List(
        fields.Nested(column_model),
        description='Список колонок с параметрами',
        required=True,
        example=[
            {
                'typeData': '1',   #1 - План
                'versionPlaning': '22600',
                'variantPlaning' : '2260099',
                'IsByear' : False,
                'dateRange': ['01.01.2026', '31.12.2026']
            },
            {
                'typeData': '1',   #1 - План
                'versionPlaning': '22600',
                'variantPlaning' : '2260010',
                'IsByear' : False,
                'dateRange': ['01.01.2026', '31.12.2026']
            },
            {
                'typeData': '15',   #2 - Факт
                'IsByear' : False,
                'dateRange': ['01.01.2026', '31.12.2026']
            }
        ]
    )
})

@ns_download_report.route('/download_report')
class ClsDownloadReport(Resource):
    # @ns_download_report.expect(container_download_report,download_report_model)
    @ns_download_report.expect(download_report_model)
    def post(self):
        try:

            uf.clear_loc_log()

            # OLD MODEL
            #param_list: dict = container_download_report.parse_args()

            # v_year = uf.get_validate_param(param_list, "year")
            # v_template_name =  uf.get_validate_param(param_list, "template_name")
            #
            # return uf.download_report(v_year,v_template_name)

            v_selected_type_download = ns_download_report.payload.get('selectedTypeDownload')
            v_selected_factories = ns_download_report.payload.get('selectedFactories')
            v_selected_reports = ns_download_report.payload.get('selectedReports')
            v_columns = ns_download_report.payload.get('columns')

            if not v_selected_type_download in ('full','simple'):
                return uf.get_msg_struct(uf.EnumMsg.INCORRECT_PARAM, 'selectedTypeDownload')

            if v_columns:
                v_selected_factories = [factory_id for factory_id in v_selected_factories]
                return funcs_mirror.main_download_report(v_selected_type_download, v_selected_factories,v_selected_reports,v_columns)
                # return uf.download_report2(v_selected_type_download, v_selected_factories,v_selected_reports,v_columns)
            else:
                return uf.get_msg_struct(uf.EnumMsg.NO_SELECTED_COLUMNS)

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
@ns_download_report.route('/get_struct')
class ClsStructDataDownloadReport(Resource):
    @ns_download_report.expect()
    def get(self):
        try:

            uf.clear_loc_log()

            # TABLES_MAP
            # factories
            # type_reports
            # data_type
            # vers_plan
            # var_plan

            total : dict

            variants = uf.get_pagin_data('var_plan', '', 1, 100)
            version = uf.get_pagin_data('vers_plan', '', 1, 100)

            grouped = {}
            for item in variants:
                v_id = item["tab_vers_plan_ids"]

                if v_id == 0:
                    continue

                if v_id not in grouped:
                    grouped[v_id] = {
                        "version": {
                            'id': v_id,
                            'name': next((item for item in version if item.get('id') == v_id), {}).get('name', ''),
                        },
                        "variants": []
                    }

                grouped[v_id]["variants"].append({
                    "tab_vers_plan_ids": item["id"],
                    "name": item["name"],
                    "utv": 0
                })

            total = {
                'factories': uf.get_pagin_data('factories', '', 1, 100),
                'type_reports': uf.sort_list_by_field(uf.get_pagin_data('type_reports', '', 1, 100)),
                'data_type': uf.get_pagin_data('data_type', '', 1, 100),
                'versions': list(grouped.values()),
                'last_update': uf.get_last_update(),
            }

            return total, 200

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
container_get_report_template = reqparse.RequestParser()
container_get_report_template.add_argument(
    "factory_id",
    type=str,
    required=True
)
@ns_download_report.route('/get_template')
class ClsDownloadTemplate(Resource):
    @ns_download_report.expect(container_get_report_template)
    def get(self):
        try:
            param_list: dict = container_get_report_template.parse_args()

            uf.clear_loc_log()

            v_factory_id = uf.get_validate_param(param_list, "factory_id")

            return funcs_mirror.get_report_template(v_factory_id)

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
container_upload_report_template = reqparse.RequestParser()
container_upload_report_template.add_argument(
    "report_id",
    type=str,
    required=True
)
container_upload_report_template.add_argument(
    "file",
    type=FileStorage,
    required=True,
    location="files"
)
@ns_download_report.route('/upload_template')
class ClsUploadTemplate(Resource):
    @ns_download_report.expect(container_upload_report_template)
    def post(self):
        try:
            param_list: dict = container_upload_report_template.parse_args()

            uf.clear_loc_log()

            v_sheet_id = uf.get_validate_param(param_list, "report_id")
            v_file = uf.get_validate_param(param_list, "file")

            return funcs_mirror.upload_report_template(v_sheet_id, v_file)

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================
@ns_download_report.route('/upload_report')
class ClsUploadReport(Resource):
    @ns_download_report.expect(container_upload_report_template)
    def post(self):
        try:
            param_list: dict = container_upload_report_template.parse_args()

            uf.clear_loc_log()

            v_sheet_id = uf.get_validate_param(param_list, "report_id")
            v_file = uf.get_validate_param(param_list, "file")

            return funcs_mirror.upload_report(v_sheet_id, v_file)

        except Exception as e:
            ns_download_report.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================