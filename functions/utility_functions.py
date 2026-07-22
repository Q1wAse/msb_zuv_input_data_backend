from typing import NamedTuple, Tuple, Any
from enum import Enum
from datetime import date
from pathlib import Path
import sys, os, re, io, copy, openpyxl
from collections import defaultdict

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from urllib.parse import parse_qs
from decimal import Decimal
from flask import session, g, abort, send_file

from sqlalchemy import inspect, text, exc
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import scoped_session, sessionmaker

from msb_zuv_input_data_backend.config import Config
from msb_zuv_input_data_backend.database import engine_py, db_py

try:
    from access_control_center.centrilized_database_pool import get_session
except (ImportError, ModuleNotFoundError):
    get_session = None


# ============================================================================================
# ============================================================================================
class EnumMsg(Enum):
    SUCCESS = 0
    SYSTEM_ERROR = 1
    INCORRECT_PARAM = 2
    INCORRECT_TAB_KEY = 3
    INCORRECT_PATCH_INPUT_DATA = 4
    INCORRECT_TEMPLATE_NAME = 5
    NO_SELECTED_COLUMNS = 6
    SETTINGS_FOR_REPORT_NOT_FOUND = 7
    ERROR_OPEN_TEMPLATE = 8
    ERROR_SAVE_OR_PROC_TEMPLATE = 9
    ERROR_PERMISSION_CREATE_DIR_LINUX = 10
    ERROR_PERMISSION_OVERWRITE_FILE = 11
    ERROR_VALID_NEW_TEMPLATE = 12


# ============================================================================================
# ============================================================================================
msg_list = {
    EnumMsg.SUCCESS: {'code': 200, 'is_err': False, 'msg': 'Успешное выполнение операции'},
    EnumMsg.SYSTEM_ERROR: {'code': 500, 'is_err': True, 'msg': 'Системная ошибка'},
    EnumMsg.INCORRECT_PARAM: {'code': 400, 'is_err': True, 'msg': 'Неверно задано значение для %'},
    EnumMsg.INCORRECT_TAB_KEY: {'code': 400, 'is_err': True, 'msg': 'Некорректное имя ключа таблицы'},
    EnumMsg.INCORRECT_PATCH_INPUT_DATA: {'code': 400, 'is_err': True, 'msg': 'Некорректный формат обновляемых данных'},
    EnumMsg.INCORRECT_TEMPLATE_NAME: {'code': 400, 'is_err': True, 'msg': 'Некорректное наименование шаблона'},
    EnumMsg.NO_SELECTED_COLUMNS: {'code': 400, 'is_err': True, 'msg': 'Должен быть выбран хотя бы один столбец'},
    EnumMsg.SETTINGS_FOR_REPORT_NOT_FOUND: {'code': 400, 'is_err': True,
                                            'msg': 'Не удалось найти настройки для выбранных отчётов из таблицы: "tab_factories_d816_4"'},
    EnumMsg.ERROR_OPEN_TEMPLATE: {'code': 400, 'is_err': True, 'msg': 'Не удалось прочитать, как XLSX, файл: %'},
    EnumMsg.ERROR_SAVE_OR_PROC_TEMPLATE: {'code': 500, 'is_err': True, 'msg': 'Ошибка при обработке или сохранении: %'},
    EnumMsg.ERROR_PERMISSION_CREATE_DIR_LINUX: {'code': 500, 'is_err': True,
                                                'msg': 'Нет прав на создание папки % в Linux'},
    EnumMsg.ERROR_PERMISSION_OVERWRITE_FILE: {'code': 500, 'is_err': True,
                                              'msg': 'Нет прав на перезапись существующего файла шаблона'},
    EnumMsg.ERROR_VALID_NEW_TEMPLATE: {'code': 500, 'is_err': True,
                                       'msg': 'Загружаемый шаблон не подходит для обработки'},
}


TABLES_MAP = {
    'map_bs_product': {
        'tab_name': 'tab_map_bs_product_d816_4',
        'fields': 'id,name,id_product,koef,factory,type_raspr,sobstv,mest',
        'mutable': True
    },
    'products': {
        'tab_name': 'tab_product_d816_4',
        'fields': 'id, name'
    },
    'factory': {
        'tab_name': 'tab_factory_d816_4',
        'fields': 'id, name'
    },
    'type_raspr': {
        'tab_name': 'tab_type_raspr_d816_4',
        'fields': 'id, name'
    },
    'sobstv': {
        'tab_name': 'tab_sobstv_d816_4',
        'fields': 'id, name'
    },
    'mest': {
        'tab_name': 'tab_mest_d816_4',
        'fields': 'id, name'
    },
    'category_product': {
        'tab_name': 'tab_view_category_product_d816_4',
        'fields': 'id, name'
    },
    'ost': {
        'tab_name': 'tab_ost_d816_4',
        'fields': 'id,tab_factory_d816_4_ids,tab_category_product_d816_4_ids,tab_product_d816_4_ids,value,value_korr',
        'mutable': True
    },

    'factories': {
        'tab_name': 'tab_factories_d816_4',
        'fields': 'id,name'
    },
    'type_reports': {
        'tab_name': 'tab_type_reports_d816_4',
        'fields': 'id,name'
    },
    'data_type': {
        'tab_name': 'tab_view_io_bcblm0003_d816_4',
        'fields': 'id,name'
    },
    'vers_plan': {
        'tab_name': 'tab_view_vers_plan_d816_4',
        'fields': 'id,name'
    },
    'var_plan': {
        'tab_name': 'tab_view_var_plan_d816_4',
        'fields': 'tab_vers_plan_ids,name,id'
    },

    'view_year': {
        'tab_name': 'tab_view_year_d816_4',
        'fields': 'year'
    },
    'view_product': {
        'tab_name': 'tab_view_product_d816_4',
        'fields': 'id, name'
    },
    'post_zuv': {
        'tab_name': 'tab_post_zuv_d816_4',
        'fields': 'id, name'
    }
}

main_folder = "/opt/foresight/msb_zuv_input_data_backend" if sys.platform.lower() in 'linux' else os.getcwd()
file_folder = "file"
sql_folder = "sql"

# ============================================================================================
# ============================================================================================
gv_collect_log_status = 0
gv_collect_log = ""


def clear_loc_log():
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = 0
    gv_collect_log = ""  # "\nn\nn"


def loc_log(msg):
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = -1
    # gv_collect_log = gv_collect_log + "\nn" + msg
    gv_collect_log = gv_collect_log + " " + msg


def loc_log_new(func, locs, err):
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = -1
    # gv_collect_log = gv_collect_log + "\nn" + msg

    # gv_collect_log = gv_collect_log + " " + msg
    gv_collect_log = gv_collect_log + " " + "func::" + func + "::locs::" + str(locs) + "::" + str(err)


def get_db_connection():
    if Config.SERVERBASE_MODE == 'PYTHON':
        return db_py
    elif Config.SERVERBASE_MODE == 'WSGI':
        try:
            return get_session()
        except Exception as e:
            loc_log(str(e))
            abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'),
                  description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])


def is_valid_date(date_string):
    try:
        date.fromisoformat(date_string)
        return True
    except ValueError:
        return False


def get_validate_param(param, field_name):
    value = param.get(field_name)
    if field_name == "tab_id":
        is_valid = isinstance(value, str) and len(value) > 0
    elif field_name == "filter" and value is not None:
        is_valid = isinstance(value, str)
    elif field_name == "page":
        is_valid = isinstance(value, int) and value >= 1
    elif field_name == "limit":
        is_valid = isinstance(value, int) and (1 <= value <= 100)
    elif field_name == "year":
        is_valid = isinstance(value, int) and (0 <= value <= 9999)
    elif field_name == "file":
        is_valid = not ((value is None) or (value.filename == ''))
    else:
        is_valid = True  # (value is not None)

    if not is_valid:
        err_msg = get_msg_struct(EnumMsg.INCORRECT_PARAM, field_name)[0]['message']
        loc_log(err_msg)
        abort(msg_list[EnumMsg.INCORRECT_PARAM].get('code'), description=err_msg)
    return value


def is_msg_id_valid(msg_id):
    try:
        EnumMsg(msg_id)
        return True
    except ValueError:
        return False


def get_msg_struct(msg_id, value=""):
    msg = 'неизвестная ошибка'
    enum_msg_local = EnumMsg(msg_id) if type(msg_id) is int else msg_id
    if is_msg_id_valid(msg_id):
        msg = msg_list[enum_msg_local].get('msg')
    type_msg = 'ошибки' if msg_list[enum_msg_local].get('is_err') else 'сообщения'
    msg = 'Код ' + type_msg + ': ' + str(enum_msg_local.value) + '. ' + msg + gv_collect_log
    msg = msg.replace('%', value)
    return {'message': msg}, msg_list[enum_msg_local].get('code')


def exec_sql_from_file(file_name, params={}):
    db = get_db_connection()
    if len(file_name) > 0:
        full = Path(main_folder) / Path(sql_folder) / file_name
        if full.is_file():
            try:
                with open(full, 'r', encoding='utf-8') as file:
                    sql_exec = file.read()
                    query = db.execute(text(sql_exec), params)
                    db.commit()
                    return query
            except Exception as e:
                loc_log(str(e))
                abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'),
                      description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])
        return '-2'
    return '-1'


def get_param_connect():
    db = get_db_connection()
    return db.execute(
        text("SELECT connection_params FROM tab_params_email_d314 WHERE system_code = 'IS_KAO_DATA'")).first()


# ============================================================================================
def convert_row(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            result[key] = float(value)  # str(value)
        else:
            result[key] = value
    return result


# ============================================================================================
def get_pagin_data_old(v_tab_id, v_filter, v_page, v_limit):
    db = get_db_connection()
    offset = max(0, (v_page - 1) * v_limit)

    cond = "WHERE name ILIKE :filter" if v_filter else ""
    params = {
        "filter": f"%{v_filter}%" if v_filter else "%",
        "limit": v_limit,
        "offset": offset
    }

    # print(str(v_tab_id) + " " + str(dict(TABLES_MAP).values()))

    v_tabname = TABLES_MAP[v_tab_id].get('tab_name')
    field_list = TABLES_MAP[v_tab_id].get('fields')

    try:
        count_sql = text(f"SELECT count(*) FROM {v_tabname} {cond}")
        total = db.execute(count_sql, params).scalar()

        sql_text = text(f"""
            SELECT {field_list} FROM {v_tabname} 
            {cond} 
            LIMIT :limit OFFSET :offset
        """)
        rows = db.execute(sql_text, params).mappings()  # fetchall()
        if rows:
            rows = [convert_row(row) for row in rows]
        # return [{'count' : total }, {'rows' : rows}]
        return {'count': total, 'rows': rows}

    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        return [0, []]


# ============================================================================================
#############################################################################################
#############################################################################################
#############################################################################################
# ============================================================================================
def get_pagin_data(v_tab_id, v_filter, v_page, v_limit):
    db = get_db_connection()
    offset = max(0, (v_page - 1) * v_limit)

    cond = "WHERE name ILIKE :filter" if v_filter else ""
    params = {
        "filter": f"%{v_filter}%" if v_filter else "%",
        "limit": v_limit,
        "offset": offset
    }

    # print(str(v_tab_id) + " " + str(dict(TABLES_MAP).values()))

    v_tabname = TABLES_MAP[v_tab_id].get('tab_name')
    field_list = TABLES_MAP[v_tab_id].get('fields')

    try:
        count_sql = text(f"SELECT count(*) FROM {v_tabname} {cond}")
        total = db.execute(count_sql, params).scalar()

        sql_text = text(f"""
            SELECT {field_list} FROM {v_tabname} 
            {cond} 
            LIMIT :limit OFFSET :offset
        """)
        rows = db.execute(sql_text, params).mappings()  # fetchall()
        if rows:
            rows = [convert_row(row) for row in rows]
        # return [{'count' : total }, {'rows' : rows}]
        return rows

    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        return []


# ============================================================================================
def patch_data(resource_key, data_list):
    db = get_db_connection()
    config = TABLES_MAP.get(resource_key)

    if not config or not isinstance(data_list, list):
        abort(msg_list[EnumMsg.INCORRECT_PATCH_INPUT_DATA].get('code'),
              description=get_msg_struct(EnumMsg.INCORRECT_PATCH_INPUT_DATA)[0]['message'])

    results = {
        "success": [],
        "errors": []
    }

    try:
        with db.begin():
            for item in data_list:
                id_record = item.get('id')
                if not id_record:
                    results["errors"].append(
                        {
                            "id": "unknown",
                            "message": "ID not found"
                        }
                    )
                    continue

                update_dict = {k: v for k, v in item.items() if v is not None and k != 'id'}

                if update_dict:
                    set_fields = ", ".join([f"{col} = :{col}" for col in update_dict.keys()])

                    sql = text(
                        f"""
                            UPDATE {config.get('tab_name')} 
                            SET {set_fields} 
                            WHERE id = :id
                        """
                    )

                    params = {
                        **update_dict,
                        "id": id_record
                    }
                    res = db.execute(sql, params)

                    if res.rowcount > 0:
                        results["success"].append(id_record)
                    else:
                        results["errors"].append(
                            {
                                "id": id_record,
                                "message": "Not found"
                            }
                        )

        return {
            "status": "completed",
            "details": results
        }
    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'),
              description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])


# ============================================================================================
def map_pg_to_frontend(pg_type):
    mapping = {
        'integer': 'number',
        'numeric': 'number',
        'real': 'number',
        'double precision': 'number',
        'character varying': 'string',
        'text': 'string',
        'boolean': 'boolean',
        'timestamp without time zone': 'datetime',
        'date': 'date'
    }
    return mapping.get(pg_type, 'string')

# ============================================================================================
def get_data_from_query(sql_text, param=None):
    db = get_db_connection()
    if db:
        if param:
            return db.execute(text(sql_text), param).fetchall()
        else:
            return db.execute(text(sql_text)).fetchall()
    return []
# ============================================================================================
def get_struct_table(key_tab):
    db = get_db_connection()
    config = TABLES_MAP.get(key_tab)
    if not config:
        abort(msg_list[EnumMsg.INCORRECT_TAB_KEY].get('code'),
              description=get_msg_struct(EnumMsg.INCORRECT_TAB_KEY)[0]['message'])

    field_list = {col.strip() for col in TABLES_MAP.get(key_tab).get('fields').split(',')}

    col_sql = text("""
        SELECT
            column_name,
            data_type,
            numeric_precision, 
            numeric_scale,
            character_maximum_length as max_len 
        FROM information_schema.columns WHERE
            table_name = :t_name AND
            column_name IN :fields
    """)

    try:
        res = db.execute(col_sql,
                         {
                             't_name': config.get('tab_name'),
                             'fields': tuple(field_list)
                         }
                         ).fetchall()

        res_dict = []
        for row in res:
            type_data = map_pg_to_frontend(row.data_type)
            if type_data == 'number':
                res_dict.append(
                    {
                        row.column_name: {
                            'type': type_data,
                            'precision': row.numeric_precision,
                            'scale': row.numeric_scale
                        }
                    }
                )
            else:
                res_dict.append(
                    {
                        row.column_name: {
                            'type': type_data,
                            'len': row.max_len
                        }
                    }
                )

        return res_dict

    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'),
              description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])