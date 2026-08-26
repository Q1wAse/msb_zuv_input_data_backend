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
ns_ui_prod_metrics = Namespace('ns_ui_prod_metrics', description='UI(Statistics) - показатели по переработке сырья и производству товарной продукции')
#==============================================================================================================================
#==============================================================================================================================
# container_prod_metrics = reqparse.RequestParser()

#==============================================================================================================================
#==============================================================================================================================
# Получение структуры справчоников и фильтров
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

                if v_id == 0:
                    continue

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

            cat_product = funcs_prod_metrics.get_product_categories()
            default_category_frame1 = next(
                (item for item in cat_product if item.get('id') == 7),
                None
            )
            default_category_frame2 = next(
                (item for item in cat_product if item.get('id') == 2),
                None
            )

            total = {
                'factories': funcs_prod_metrics.get_exist_factories('factories_kao'),
                'data_type': uf.get_pagin_data('data_type_kao', '', 1, 100),
                'versions': list(grouped.values()),
                'years' : uf.get_pagin_data('view_year', '', 1, 100),
                'cat_product': cat_product,
                # Значения по умолчанию для фильтра 1
                'filter_middle_volume_frame1': {
                    'cat_product': default_category_frame1,
                    'product': (
                        funcs_prod_metrics.get_products_by_category(
                            default_category_frame1['id']
                        )
                        if default_category_frame1 else []
                    )
                },
                # Значения по умолчанию для фильтра 2
                'filter_middle_volume_frame2': {
                    'cat_product': default_category_frame2,
                    'product': (
                        funcs_prod_metrics.get_products_by_category(
                            default_category_frame2['id']
                        )
                        if default_category_frame2 else []
                    )
                },
                'last_update': uf.get_last_update(),
            }

            return total, 200

        except Exception as e:
            ns_ui_prod_metrics.abort(*errorhandler(e))
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
#==============================================================================================================================
flt_middle_volume_model = ns_ui_prod_metrics.model('FltMiddleVolume', {
    'product': fields.List(fields.Integer,description='Продукт', required=False),
    'sobstv': fields.List(fields.Integer,description='Собственник', required=False),
    'mest': fields.List(fields.Integer,description='Месторождение', required=False),
    'post_zuv': fields.List(fields.Integer, description='Поставщик ЖУВ', required=False),
    'ei': fields.List(fields.Integer, description='Единицы измерения', required=False),
    'cat_product': fields.List(fields.Integer,description='Категория продукта', required=True),
})

flt_container_get_prod_metrics_model = ns_ui_prod_metrics.model('ContainerGetProdMetricsFlt', {
    'selectedVariantCompare': fields.List(
        fields.String,
        description='Вариант сравнения',
        required=True,
        example=["1","2"]
    ),
    'selectedFactories': fields.List(
        fields.String,
        description='Список выбранных заводов',
        required=True,
        example=["7"]
    ),
    'filtertMiddleVolumeFrame1': fields.Nested(
        flt_middle_volume_model,
        description='Фильтр для центрального левого графика',
        required=True,
        example={
            'product': [31],
            'sobstv': [1],
            'mest': [32],
            'post_zuv': [10],
            'ei': [1],
            'cat_product': [7],
        }
    ),
    'filtertMiddleVolumeFrame2': fields.Nested(
        flt_middle_volume_model,
        description='Фильтр для правых графиков',
        required=True,
        example={
            'product': [7],
            'sobstv': [1],
            'mest': [35],
            'post_zuv': [0],
            'ei': [1],
            'cat_product': [9],
        }
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
                "typeData": "2",   #2 - факт
                "year": "2026"
            }
        ]
    )
})

#==============================================================================================================================
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
    'filtertMiddleVolumeFrame2': fields.Nested(
        flt_middle_volume_model,
        description='Фильтр для правых графиков',
        required=True,
        example={
            'product': [7],
            'sobstv': [1],
            'mest': [35],
            'post_zuv': [0],
            'ei': [1],
            'cat_product': [9],
        }
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
                "typeData": "2",   #2 - факт
                "year": "2026"
            }
        ]
    )
})
#=======================================================================================================================
@ns_ui_prod_metrics.route('/get_prod_metrics_main')
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
                return funcs_prod_metrics.get_calculated_dataset(
                    v_selected_variant_compare,
                    v_selected_factories,
                    {},
                    {},
                    v_variant_columns
                ), 200
            else:
                return uf.get_msg_struct(uf.EnumMsg.NO_SELECTED_COLUMNS)

        except Exception as e:
            ns_ui_prod_metrics.abort(*errorhandler(e))
#=======================================================================================================================
@ns_ui_prod_metrics.route('/get_prod_metrics_flt')
class ClsGetColumnDataFlt(Resource):
    @ns_ui_prod_metrics.expect(flt_container_get_prod_metrics_model)
    def post(self):
        try:
            uf.clear_loc_log()

            v_selected_variant_compare = ns_ui_prod_metrics.payload.get('selectedVariantCompare')
            v_selected_factories = ns_ui_prod_metrics.payload.get('selectedFactories')
            v_filters_middle_volume_frame1 = ns_ui_prod_metrics.payload.get('filtertMiddleVolumeFrame1')
            v_filters_middle_volume_frame2 = ns_ui_prod_metrics.payload.get('filtertMiddleVolumeFrame2')
            v_variant_columns = ns_ui_prod_metrics.payload.get('VariantColumns')

            if v_variant_columns:
                v_selected_factories = [factory_id for factory_id in v_selected_factories]
                return funcs_prod_metrics.get_calculated_dataset(
                    v_selected_variant_compare,
                    v_selected_factories,
                    v_filters_middle_volume_frame1,
                    v_filters_middle_volume_frame2,
                    v_variant_columns
                ), 200
            else:
                return uf.get_msg_struct(uf.EnumMsg.NO_SELECTED_COLUMNS)

        except Exception as e:
            ns_ui_prod_metrics.abort(*errorhandler(e))
#==============================================================================================================================
#==============================================================================================================================