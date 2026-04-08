from typing import NamedTuple, Tuple, Any
from enum import Enum
from datetime import date
from pathlib import Path
import sys, os, io, openpyxl

from urllib.parse import parse_qs
from decimal import Decimal
from flask import session, g, abort

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
#============================================================================================
#============================================================================================
class EnumMsg(Enum):
    SUCCESS         			= 0
    SYSTEM_ERROR			    = 1
    INCORRECT_PARAM             = 2
    INCORRECT_PATCH_INPUT_DATA  = 3

#============================================================================================
#============================================================================================
msg_list = {
    EnumMsg.SUCCESS		                    : { 'code' : 200, 'is_err': False,	'msg' : 'Успешное выполнение операции' },
    EnumMsg.SYSTEM_ERROR			        : { 'code' : 500, 'is_err': True,	'msg' : 'Системная ошибка' },
    EnumMsg.INCORRECT_PARAM			        : { 'code' : 400, 'is_err': True,	'msg' : 'Неверно задано значение для %' },
    EnumMsg.INCORRECT_PATCH_INPUT_DATA		: { 'code' : 400, 'is_err': True,	'msg' : 'Некорректный формат обновляемых данных' },
}

TABLES_MAP = {
    'map_bs_product': {
        'tab_name': 'tab_map_bs_product_d816_4',
        'fields': 'id, name, id_product, koef, factory'
    },
    'products': {
        'tab_name': 'tab_product_d816_4',
        'fields': 'id, name'
    },
    'factory': {
        'tab_name': 'tab_factory_d816_4',
        'fields': 'id, name'
    }
}

sql_folder = "sql"
main_folder = "/opt/foresight/msb_zuv_input_data_backend"

#============================================================================================
#============================================================================================
gv_collect_log_status = 0
gv_collect_log = ""

def clear_loc_log():
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = 0
    gv_collect_log = ""#"\nn\nn"

def loc_log(msg):
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = -1
    #gv_collect_log = gv_collect_log + "\nn" + msg
    gv_collect_log = gv_collect_log + " " + msg

def loc_log_new(func, locs, err):
    global gv_collect_log_status
    global gv_collect_log

    gv_collect_log_status = -1
    #gv_collect_log = gv_collect_log + "\nn" + msg

    # gv_collect_log = gv_collect_log + " " + msg
    gv_collect_log = gv_collect_log + " " +  "func::" + func + "::locs::" + str(locs) + "::" + str(err)

def get_db_connection():
    if Config.SERVERBASE_MODE == 'PYTHON':
        return db_py
    elif Config.SERVERBASE_MODE == 'WSGI':
        try:
            return get_session()
        except Exception as e:
            loc_log(str(e))
            abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'), description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])

def is_valid_date(date_string):
    try:
        date.fromisoformat(date_string)
        return True
    except ValueError:
        return False

def validate_param(param, field_name):
    value = param.get(field_name)
    if field_name == "tab_id":
        is_valid = isinstance(value, str)
    elif field_name == "filter" and value is not None:
        is_valid = isinstance(value, str)
    elif field_name == "page":
        is_valid = isinstance(value, int) and value >= 1
    elif field_name == "limit":
        is_valid = isinstance(value, int) and (1 <= value <= 100)
    else:
        is_valid = (value is None)

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

def get_msg_struct(msg_id, value = ""):
    msg = 'неизвестная ошибка'
    enum_msg_local = EnumMsg(msg_id) if type(msg_id) is int else msg_id
    if is_msg_id_valid(msg_id):
        msg = msg_list[enum_msg_local].get('msg')
    type_msg = 'ошибки' if msg_list[enum_msg_local].get('is_err') else 'сообщения'
    msg = 'Код ' + type_msg + ': ' + str(enum_msg_local.value) + '. ' + msg + gv_collect_log
    msg = msg.replace('%',value)
    return {'message' : msg }, msg_list[enum_msg_local].get('code')

def exec_sql_from_file(file_name, params = {}):
    db = get_db_connection()
    if len(file_name) > 0:
        full = Path(main_folder) / Path(sql_folder) /  file_name
        if full.is_file():
            try:
                with open(full, 'r', encoding='utf-8') as file:
                    sql_exec = file.read()
                    query = db.execute(text(sql_exec), params)
                    db.commit()
                    return query
            except Exception as e:
                loc_log(str(e))
                abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'), description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])
        return '-2'
    return '-1'

def get_date(service):
    res = exec_sql_from_file(service.UNI_PROP.get("SQL_GET_DATE_FROM_SRC"))
    if res and not type(res) is str:
        return res.first()[0]
    return  "None"

def get_param_connect():
    db = get_db_connection()
    return  db.execute(text("SELECT connection_params FROM tab_params_email_d314 WHERE system_code = 'IS_KAO_DATA'")).first()
#============================================================================================
def get_pagin_data(v_tab_id, v_filter, v_page, v_limit):
    db = get_db_connection()
    offset = max(0, (v_page - 1) * v_limit)

    cond = "WHERE name ILIKE :filter" if v_filter else ""
    params = {
        "filter": f"%{v_filter}%" if v_filter else "%",
        "limit": v_limit,
        "offset": offset
    }

    print(str(v_tab_id) + " " + str(dict(TABLES_MAP).values()))

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
        rows = db.execute(sql_text, params).fetchall()

        return [total, rows]

    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        return [0, []]
#============================================================================================
def patch_batch_products(resource_key, data_list):
    db = get_db_connection()
    config = TABLES_MAP.get(resource_key)

    if not config or not isinstance(data_list, list):
        abort(msg_list[EnumMsg.INCORRECT_PATCH_INPUT_DATA].get('code'), description=get_msg_struct(EnumMsg.INCORRECT_PATCH_INPUT_DATA)[0]['message'])

    results = {
        "success"   : [],
        "errors"    : []
    }

    try:
        with db.begin():
            for item in data_list:
                id_record = item.get('id')
                if not id_record:
                    results["errors"].append(
                        {
                            "id"        : "unknown",
                            "message"   : "ID not found"
                        }
                    )
                    continue

                update_dict = {k: v for k, v in item.items() if v is not None and k != 'id'}

                if update_dict:
                    set_fields = ", ".join([f"{col} = :{col}" for col in update_dict.keys()])

                    sql = text(
                        f"""
                            UPDATE {config['tab_name']} 
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
                                "id"        : id_record,
                                "message"   : "Not found"
                            }
                        )

        return {
                "status"    : "completed",
                "details"   : results
        }
    except Exception as e:
        loc_log_new(sys._getframe(0).f_code.co_name, locals(), e)
        abort(msg_list[EnumMsg.SYSTEM_ERROR].get('code'), description=get_msg_struct(EnumMsg.SYSTEM_ERROR)[0]['message'])
#============================================================================================