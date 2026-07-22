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
import msb_zuv_input_data_backend.functions.funcs_prod_metrics as funcs_prod_metrics

#==============================================================================================================================
#==============================================================================================================================
ns_ui_prod_metrics = Namespace('ns_ui_prod_metrics', description='UI - показатели по переработке сырья и производству товарной продукции')
#==============================================================================================================================
#==============================================================================================================================
# container_prod_metrics = reqparse.RequestParser()

#==============================================================================================================================
#==============================================================================================================================
@ns_ui_prod_metrics.route('/get_struct')
class ClsStructDataProdMetrics(Resource):
    @ns_ui_prod_metrics.expect()
    def get(self):
        try:
            uf.clear_loc_log()

            total: dict

            variants = uf.get_pagin_data('var_plan', '', 1, 100)
            version = uf.get_pagin_data('vers_plan', '', 1, 100)

            grouped = {}
            for item in variants:
                v_id = item["tab_vers_plan_ids"]

                if v_id not in grouped:
                    grouped[v_id] = {
                        "version": {
                            'id' : v_id,
                            'name' : next((item for item in version if item.get('id') == v_id), {}).get('name', ''),
                        },
                        "variants": []
                    }

                grouped[v_id]["variants"].append({
                    "tab_vers_plan_ids": item["id"],
                    "name": item["name"],
                    "utv" : 0
                })

            total = {
                'factories': uf.get_pagin_data('factories', '', 1, 100),
                'data_type': uf.get_pagin_data('data_type', '', 1, 100),
                'versions': list(grouped.values()),
                'years' : uf.get_pagin_data('view_year', '', 1, 100),
                'product' : uf.get_pagin_data('view_product', '', 1, 1000),
                'sobstv' : uf.get_pagin_data('sobstv', '', 1, 100),
                'mest' : uf.get_pagin_data('mest', '', 1, 100),
                'post_zuv' : uf.get_pagin_data('post_zuv', '', 1, 1000),
            }

            return total, 200

        except Exception as e:
            ns_ui_prod_metrics.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================

variant_column_model = ns_ui_prod_metrics.model('VariantColumn', {
    'typeData': fields.String(description='Тип данных', required=True),
    'versionPlaning': fields.String(description='Версия планирования', required=True),
    'variantPlaning': fields.String(description='Вариант планирования', required=True),
    'year': fields.List(
        fields.String,
        description='Год',
        required=True
    )
})

main_container_get_prod_metrics_model = ns_ui_prod_metrics.model('ContainerGetProdMetrics', {
    'selectedVariantCompare': fields.List(
        fields.String,
        description='Вариант сравнения',
        required=True,
        example=["1","2"]
        # example=["1", "2", "3"]
    ),
    'selectedFactories': fields.List(
        fields.String,
        description='Список выбранных заводов',
        required=True,
        example=["1"]
        # example=["1", "2", "3"]
    ),
    'VariantColumns': fields.List(
        fields.Nested(variant_column_model),
        description='Список колонок с параметрами',
        required=True,
        example=[
            {
                "typeData": "1",   #1 - План
                "versionPlaning": "22600",
                "variantPlaning" : "2260099",
                "year": "2026"
            },
            {
                "typeData": "1",   #1 - План
                "versionPlaning": "22600",
                "variantPlaning" : "2260010",
                "year": "2026"
            },
            {
                "typeData": "15",   #1 - факт
                "year": "2026"
            }
        ]
    )
})

@ns_ui_prod_metrics.route('/get_prod_metrics_flt')
class ClsGetColumnData(Resource):
    @ns_ui_prod_metrics.expect(main_container_get_prod_metrics_model)
    def post(self):
        try:

            uf.clear_loc_log()

            v_selected_variant_compare = ns_ui_prod_metrics.payload.get('selectedVariantCompare')
            v_selected_factories = ns_ui_prod_metrics.payload.get('selectedFactories')
            v_variant_columns = ns_ui_prod_metrics.payload.get('VariantColumns')

            if v_variant_columns:
                v_selected_factories = [factory_id for factory_id in v_selected_factories]
                return funcs_prod_metrics.get_calculated_values(
                    v_selected_variant_compare,
                    v_selected_factories,
                    v_variant_columns
                ), 200
                # return funcs_mirror.main_download_report(v_selected_type_download, v_selected_factories,v_selected_reports,v_columns)
                # return uf.download_report2(v_selected_type_download, v_selected_factories,v_selected_reports,v_columns)
            else:
                return uf.get_msg_struct(uf.EnumMsg.NO_SELECTED_COLUMNS)

        except Exception as e:
            ns_ui_prod_metrics.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================