import sys, os, re, io, copy, openpyxl

from flask import session, g, abort, send_file
from sqlalchemy import text
from werkzeug.datastructures import FileStorage
from collections import defaultdict
from enum import Enum
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.cell import Cell
from openpyxl.utils.cell import range_boundaries, coordinate_to_tuple
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.cell_range import MultiCellRange
from openpyxl.styles import Border, Side, NamedStyle
# from openpyxl.styles import Font, Alignment, numbers, Border, Side, PatternFill
# from openpyxl.formatting.rule import CellIsRule

import msb_zuv_input_data_backend.functions.utility_functions as uf
import msb_zuv_input_data_backend.functions.formula_translator as formula_translator

# ============================================================================================
class EnumCellType(Enum):
    TITLE_LVL0 = 0
    TITLE_LVL1 = 1
    TITLE_GROUP1_LVL1 = 2
    TITLE_GROUP1_LVL2 = 3
    INPUT = 4
    SIMPLE_FORMULA = 5
    FONT_CHECKER1 = 6
    PERCENT = 7
    POSITIVE_NEGATIVE = 8
    SECOND_MINUS_FIRST = 9
    PERCENT_OF_COMPLETE = 10
    PERCENT_OF_OUTPUT = 11


class EnumColumnSettings(str, Enum):
    KEY_BS = 'key_bs'
    KEY_SPEC = 'key_spec'
    KEY_INPUT = 'key_input'
    FORMULA_MONTH = 'formula_month'
    FORMULA_POO = 'formula_PercentOfOutput'
    ROW_CHECK = 'row_check'
# ============================================================================================

# g_report_template_name = "Астрахань.xlxs"
g_report_template_name = "мсб свод (общий).xlsx"

# FONT_SIMPLE = Font(name="Times New Roman", size=14, bold=True, color="000000")
# FONT_FORMULA = Font(name="Times New Roman", size=14, bold=True, color="0000FF")
# FONT_TITLE_LVL0 = Font(name="Times New Roman", size=10, bold=True)
# FONT_TITLE_LVL1 = Font(name="Times New Roman", size=14, bold=True)
# FONT_GREEN = Font(name="Times New Roman", size=14, color='00B050')
# FONT_RED = Font(name="Times New Roman", size=14, color='C00000')
#
# ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
#
# RULE_POSITIVE = CellIsRule(operator='greaterThan', formula=['0'], font=FONT_GREEN)
# RULE_NEGATIVE = CellIsRule(operator='lessThan', formula=['0'], font=FONT_RED)
#
# THIN_LINE = Side(border_style="thin", color="000000")
# FULL_BORDER = Border(left=THIN_LINE, right=THIN_LINE, top=THIN_LINE, bottom=THIN_LINE)

G_STYLE_FONT_SIMPLE = None
G_STYLE_FONT_FORMULA = None
G_STYLE_FONT_TITLE_LVL0 = None
G_STYLE_FONT_TITLE_LVL1 = None
G_STYLE_RULE_GREEN_RED_DASH = None
G_STYLE_FULL_BORDER = None
G_STYLE_FULL_BORDER2 = None
G_STYLE_FORMAT_PERCENTAGE = None
G_STYLE_FONT_TITLE_LVL1_GROUP1 = None
G_STYLE_FONT_TITLE_LVL2_GROUP1 = None
G_STYLE_FONT_CHECKER1 = None
G_STYLE_RULE_DASH_FOR_ZERO = None

template_list = [
    'Астрахань',
    'Сосногорск'
]
template_setups = [
    {  # Астраханский ГПЗ
        'index': 1,
        'template_name': 'Астрахань',
        'do': 38,
        'pj': 7
    },
    {  # Сосногорский ГПЗ
        'index': 2,
        'template_name': 'Сосногорск',
        'do': 38,
        'pj': 1
    }
]

# ============================================================================================
def get_row_list_msb_zuv_d816_4(year: int, ver_plan: int, var_plan: int, bs: list, do: int, pj: int, data_type: int):
    db = uf.get_db_connection()
    col_sql = text("""
                SELECT
                    SUM(SUM),
                    BS,
                    CALYEAR, 
                    CALQUART,
                    CALMONTH 
                FROM tab_integ_get_preu_mirror_d816_4 WHERE
                    CALYEAR::INT = :year AND            -- Год планирования
                    BCBLM0001::INT = :ver_plan AND      -- Версия планирования
                    BCBLM0002::INT = :var_plan AND      -- Вариант планирования              
                    BS = ANY((:bs)::int[]) AND          -- Бюджетные статьи
                    BCBIM0002::INT = :do AND            -- Завод (Дочернее общество)
                    pj = :pj AND                        -- Перерабатывающий комплекс (Поставщики ЖУВ)
                    DATA_TYPE::INT = :data_type AND     -- Тип данных
                    CALMONTH <> 0 AND
                    DBS = 0
                GROUP BY BS, CALYEAR, CALQUART, CALMONTH
                ORDER by CALMONTH
            """)
    result = db.execute(col_sql,
                        {
                            'year': year,
                            'ver_plan': ver_plan,
                            'var_plan': var_plan,
                            'bs': f"{{{','.join(map(str, bs))}}}",
                            'do': do,
                            'pj': pj,
                            'data_type': data_type
                        }
                        ).fetchall()
    return result


# ============================================================================================
def download_report(year, template_name):
    settings = [item for item in template_setups if item['template_name'] == template_name]
    if not settings:
        return

    if settings[0].get('all', False):
        settings = [item for item in template_setups if item['template_name'] != template_name]
    if not settings:
        return

    template_name += '.xlsx'
    path_template = str(Path(uf.main_folder) / Path(uf.file_folder) / Path(template_name))

    # Получаем рабочую книгу из шаблона
    wb = openpyxl.load_workbook(path_template)

    # Создаём буфер для наполнения
    buffer = io.BytesIO()

    named_value = wb.defined_names['_YEAR']
    if named_value:
        named_value.value = year

    # for index, item in enumerate(settings, start=1):
    for item in settings:
        do = item.get('do')
        pj = item.get('pj')
        index = item.get('index')

        def_range_bs = wb.defined_names[f'_BS{index}']
        if not def_range_bs:
            continue

        bs_col_index = -1
        dict_month_col_index = []
        sheet_name = ''
        for sheet_name_rng_bs, cell_coordinates in def_range_bs.destinations:
            bs_col_index, _, _, _ = range_boundaries(cell_coordinates)
            sheet_name = sheet_name_rng_bs
        for i in range(1, 13):
            def_range_month = wb.defined_names[f'_MONTH{index}_{i}']
            for sheet_name_rng_month, cell_coordinates in def_range_month.destinations:
                min_col, _, _, _ = range_boundaries(cell_coordinates)
                dict_month_col_index.append(min_col)

        if bs_col_index != -1 and sheet_name and len(dict_month_col_index) == 12:
            dict_bs = []
            sheet = wb[sheet_name]
            if not sheet:
                continue

            last_row = sheet.max_row
            for i in range(1, last_row + 1):
                cell = sheet.cell(row=i, column=bs_col_index)
                if cell.value is not None and str(cell.value).isdigit():
                    dict_bs.append(cell.value)
            if not dict_bs:
                continue

            query_plan_99_res = get_row_list_msb_zuv_d816_4(year, 22600, 2260099, dict_bs, do, pj, 1)
            query_plan_10_res = get_row_list_msb_zuv_d816_4(year, 22600, 2260010, dict_bs, do, pj, 1)
            query_fact_res = get_row_list_msb_zuv_d816_4(year, 0, 0, dict_bs, do, pj, 15)

            for i in range(1, last_row + 1):
                cell_bs = sheet.cell(row=i, column=bs_col_index)
                if cell_bs and cell_bs.value is not None and str(cell_bs.value).isdigit():
                    # for month_col in dict_month_col_index:
                    for ind, it in enumerate(dict_month_col_index, start=1):
                        for row in query_plan_99_res:
                            cell_val = sheet.cell(row=i, column=it)
                            if cell_val and cell_val.data_type == 'f':
                                continue
                            if int(row.bs) == int(cell_bs.value) and ind == row.calmonth:
                                cell_val.value = row.sum
                        for row in query_plan_10_res:
                            cell_val = sheet.cell(row=i, column=it + 1)
                            if cell_val and cell_val.data_type == 'f':
                                continue
                            if int(row.bs) == int(cell_bs.value) and ind == row.calmonth:
                                cell_val.value = row.sum
                        for row in query_fact_res:
                            cell_val = sheet.cell(row=i, column=it + 2)
                            if cell_val and cell_val.data_type == 'f':
                                continue
                            if int(row.bs) == int(cell_bs.value) and ind == row.calmonth:
                                cell_val.value = row.sum

    # Сохраняем подготовленные данные из шаблона
    wb.save(buffer)
    # Откатываем курсор в самое начало
    buffer.seek(0)
    # имя файла
    filename = template_name  # "test_file.xlsx"

    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

# ============================================================================================
# ============================================================================================
def set_value_cell(cell, value, ColumnType: EnumCellType = EnumCellType.INPUT):
    global G_STYLE_FONT_SIMPLE, \
        G_STYLE_FONT_FORMULA, \
        G_STYLE_FONT_TITLE_LVL0, \
        G_STYLE_FONT_TITLE_LVL1, \
        G_STYLE_FONT_GREEN, \
        G_STYLE_FONT_RED, \
        G_STYLE_RULE_GREEN_RED_DASH, \
        G_STYLE_FULL_BORDER, \
        G_STYLE_FULL_BORDER2, \
        G_STYLE_FORMAT_PERCENTAGE, \
        G_STYLE_FONT_TITLE_LVL1_GROUP1, \
        G_STYLE_FONT_TITLE_LVL2_GROUP1, \
        G_STYLE_FONT_CHECKER1, \
        G_STYLE_RULE_DASH_FOR_ZERO

    cell.value = value
    # cell.style = "_USER_CUSTOM_BORDER"

    if ColumnType in (EnumCellType.TITLE_LVL0, EnumCellType.TITLE_LVL1):
        if ColumnType == EnumCellType.TITLE_LVL0:
            chosen_style = {**G_STYLE_FONT_TITLE_LVL0}
        else:
            chosen_style = {**G_STYLE_FONT_TITLE_LVL1}

    elif ColumnType == EnumCellType.INPUT:
        if cell.data_type == 'f':
            chosen_style = {**G_STYLE_FONT_FORMULA}
        else:
            chosen_style = {**G_STYLE_FONT_SIMPLE}

    elif ColumnType == EnumCellType.SIMPLE_FORMULA:
        chosen_style = {**G_STYLE_FONT_FORMULA}

    elif ColumnType in (EnumCellType.PERCENT, EnumCellType.POSITIVE_NEGATIVE):
        chosen_style = {**G_STYLE_FONT_FORMULA}

        # cell.parent.conditional_formatting.add(cell.coordinate, RULE_POSITIVE)
        # cell.parent.conditional_formatting.add(cell.coordinate, RULE_NEGATIVE)

        if ColumnType == EnumCellType.PERCENT:
            chosen_style['number_format'] = G_STYLE_FORMAT_PERCENTAGE  # тип данных строка
    elif ColumnType == EnumCellType.TITLE_GROUP1_LVL1:
        chosen_style = {**G_STYLE_FONT_TITLE_LVL1_GROUP1}
    elif ColumnType == EnumCellType.TITLE_GROUP1_LVL2:
        chosen_style = {**G_STYLE_FONT_TITLE_LVL2_GROUP1}
    elif ColumnType == EnumCellType.FONT_CHECKER1:
        chosen_style = {**G_STYLE_FONT_CHECKER1}
    else:
        chosen_style = {**G_STYLE_FONT_SIMPLE}

    cell.font = chosen_style['font']
    cell.alignment = chosen_style['alignment']
    cell.fill = chosen_style['fill']
    cell.number_format = chosen_style['number_format']


# #============================================================================================
def extract_cell_styles(src_cell):
    return {
        'font': copy.copy(src_cell.font),
        'alignment': copy.copy(src_cell.alignment),
        'fill': copy.copy(src_cell.fill),
        'number_format': src_cell.number_format
    }


# def extract_optimal_border(source_cell):
#     b = source_cell.border
#
#     def clean_side(side):
#         if side and side.style:
#             return Side(style=side.style, color=side.color)
#         return None
#
#     return Border(
#         left=clean_side(b.left),
#         right=clean_side(b.right),
#         top=clean_side(b.top),
#         bottom=clean_side(b.bottom),
#         diagonal=clean_side(b.diagonal),
#         diagonal_direction=b.diagonal_direction,
#         outline=b.outline,
#         vertical=clean_side(b.vertical),
#         horizontal=clean_side(b.horizontal)
#     )
def extract_fast_border(source_cell):
    b = source_cell.border

    def clean_side(side):
        if side and side.style:
            return Side(style=side.style, color=side.color)
        return None

    return Border(
        left=clean_side(b.left),
        right=clean_side(b.right),
        top=clean_side(b.top),
        bottom=clean_side(b.bottom),
        diagonal=clean_side(b.diagonal),
        diagonal_direction=b.diagonal_direction,
        outline=b.outline,
        vertical=clean_side(b.vertical),
        horizontal=clean_side(b.horizontal)
    )


# def download_report2(download_type, selected_factories, selected_reports, src_columns):
def main_download_report(download_type, selected_factories, selected_reports, src_columns):
    global G_STYLE_FONT_SIMPLE, \
        G_STYLE_FONT_FORMULA, \
        G_STYLE_FONT_TITLE_LVL0, \
        G_STYLE_FONT_TITLE_LVL1, \
        G_STYLE_FONT_GREEN, \
        G_STYLE_FONT_RED, \
        G_STYLE_RULE_GREEN_RED_DASH, \
        G_STYLE_FULL_BORDER, \
        G_STYLE_FULL_BORDER2, \
        G_STYLE_FORMAT_PERCENTAGE, \
        G_STYLE_FONT_TITLE_LVL1_GROUP1, \
        G_STYLE_FONT_TITLE_LVL2_GROUP1, \
        G_STYLE_FONT_CHECKER1, \
        G_STYLE_RULE_DASH_FOR_ZERO

    storage_sheet = defaultdict(lambda: defaultdict(dict))

    month = {
        1: 'январь',
        2: 'февраль',
        3: 'март',
        4: 'апрель',
        5: 'май',
        6: 'июнь',
        7: 'июль',
        8: 'август',
        9: 'сентябрь',
        10: 'октябрь',
        11: 'ноябрь',
        12: 'декабрь',
    }
    # ==================================================================================================================
    len_src_columns = len(src_columns)
    columns = []

    for i, column in enumerate(src_columns):
        loc_column = copy.deepcopy(column)
        loc_column['ColumnType'] = 'Selected'
        loc_column['SrcKey'] = i
        columns.append(loc_column)
        columns.append({
            'ColumnType': 'PercentOfOutput',
            'ColumnName': '% выхода',
            'IsStartGroup': True,
            'MergeCount': 1,
            'GroupName': '% выхода',
            'FormulaLink': {
                'Formula': '=ОКРУГЛ({}/{}*100;6)',
                'total': 2,
                'col1': {
                    'Letter': '',
                    'index': 0
                },
                'col2': {
                    'Letter': '',
                    'index': 0
                }
            }
            # 'ptr': columns[1]
        })
    if len_src_columns > 1:
        columns.append({
            'ColumnType': 'SecondMinusFirst',
            'ColumnName': '+ / -',
            'IsStartGroup': True,
            'GroupName': 'Отклонение',
            'FormulaLink': {
                'Formula': '=ЕСЛИОШИБКА({}-{};"-")',
                'total': 2,
                'col1': {
                    'Letter': '',
                    'index': 1
                },
                'col2': {
                    'Letter': '',
                    'index': 0
                }
            }
            # 'ptr': columns[0]
        })
        columns.append({
            'ColumnType': 'PercentOfComplete',
            'ColumnName': '% вып.',
            'IsStartGroup': False,
            'GroupName': 'Отклонение',
            'FormulaLink': {
                'Formula': '=ЕСЛИОШИБКА({}/{};"-")',
                'total': 2,
                'col1': {
                    'Letter': '',
                    'index': 1
                },
                'col2': {
                    'Letter': '',
                    'index': 0
                }
            }
            # 'ptr': columns[0]
        })
    for column in columns:
        if column.get('ColumnType', '') != 'Selected':
            column['SrcKey'] = -1
    # ==================================================================================================================
    offset_ind_col = 1
    factories_all = [str(row.id) for row in uf.get_data_from_query("SELECT id FROM tab_factories_d816_4")]
    if not selected_factories:
        selected_factories = factories_all
    reports_all = [str(row.id) for row in uf.get_data_from_query("SELECT id FROM tab_type_reports_d816_4")]
    # if not selected_reports:
    #     selected_reports = [1]

    settings = uf.get_data_from_query(
        'SELECT id, "DO", pj FROM tab_factories_d816_4 WHERE id IN :factory_ids',
        {"factory_ids": tuple(selected_factories)})
    if not settings:
        return uf.get_msg_struct(uf.EnumMsg.SETTINGS_FOR_REPORT_NOT_FOUND)

    composite_keys_do_pj = [(row.DO, row.pj) for row in settings]

    # bs_calc_mapping = get_data_from_query(
    #     """
    #                 SELECT "DO", pj, bs, calc FROM tab_bs_calc_map_d816_4 WHERE ("DO",pj) IN :composite_keys
    #             """,
    #     {"composite_keys": tuple(composite_keys_do_pj)})

    temp_data = {}
    for col in columns:
        if 'typeData' in col:
            type_id = int(col['typeData'])

            if type_id == 1:
                if type_id not in temp_data:
                    temp_data[type_id] = []

                temp_data[type_id].append({'variant_planing': col.get('variantPlaning')})

            else:
                temp_data[type_id] = {'simple': True}

    columns_collect = [{key: val} for key, val in temp_data.items()]

    columns_text = []
    # for collect in columns_collect:
    for item in columns_collect:
        for type_id, collect in item.items():
            if type_id == 1:
                variants = [item['variant_planing'] for item in collect]

                query_res = uf.get_data_from_query(
                    'SELECT id, name FROM tab_view_var_plan_d816_4 WHERE id IN :variant_ids',
                    {'variant_ids': tuple(variants)})
                for row in query_res:
                    columns_text.append({'typeData': type_id, 'id': row.id, 'name': row.name})

            elif type_id == 15:
                query_res = uf.get_data_from_query(
                    'SELECT id, name FROM tab_view_io_bcblm0003_d816_4 WHERE id = :type_id',
                    {'type_id': type_id})
                for row in query_res:
                    columns_text.append({'typeData': type_id, 'id': row.id, 'name': row.name})

    def get_txt_col(column):
        if 'ColumnType' in column:
            if column.get('ColumnType') == 'Selected':
                if 'typeData' in column:
                    typeData = int(column.get('typeData'))
                    if typeData == 1 and 'variantPlaning' in column:
                        matched_item = next(
                            (item for item in columns_text if int(item['id']) == int(column.get('variantPlaning'))), '')
                        return matched_item.get('name', '')
                    elif typeData == 15:
                        matched_item = next((item for item in columns_text if int(item['id']) == typeData), '')
                        return matched_item.get('name', '')
            else:
                if 'ColumnName' in column:
                    return column.get('ColumnName')
        return ''

    def get_internal_key(column, type_name):
        if 'ColumnType' in column:
            ColumnType = column.get('ColumnType')
            if ColumnType == 'Selected':
                return f"{type_name}:{column.get('typeData')}:{column.get('variantPlaning')}"
        return ''

    def get_index_column_layout(columns_layout, simple_key, idx):
        col_index = 0
        for i, col in enumerate(columns_layout):
            if col['type'] == simple_key:
                if idx == col_index:
                    return i
                else:
                    col_index += 1
        return 0

    # def get_calced_formula_link_index_column_layout_old(columns_layout, simple_key, formula_link):
    #     FormulaLink = formula_link
    #     if 'total' in FormulaLink and FormulaLink.get('total') > 0:
    #         col_index = [0] * FormulaLink.get('total')
    #         for j in range(1,FormulaLink.get('total')+1):
    #             if f'col{j}' in FormulaLink:
    #                 FormulaLink[f'col{j}']['index'] = get_index_column_layout(
    #                     columns_layout,
    #                     simple_key,
    #                     FormulaLink.get(f'col{j}').get('index'))
    #     return FormulaLink
    def get_calced_formula_link_index_column_layout(formula_link):
        FormulaLink = formula_link
        if 'total' in FormulaLink and FormulaLink.get('total') > 0:
            col_index = [0] * FormulaLink.get('total')
            for j in range(1, FormulaLink.get('total') + 1):
                if f'col{j}' in FormulaLink:
                    FormulaLink[f'col{j}']['index'] = 0
        return FormulaLink

    def get_count_merge_group(columns, column):
        count = 0
        if column.get('IsStartGroup', False):
            MergeCount = column.get('MergeCount', 0)
            if MergeCount:
                return MergeCount
            else:
                NeedGroupName = column.get('GroupName')
                if NeedGroupName:
                    for col in columns:
                        if 'GroupName' in col and col.get('GroupName') == NeedGroupName:
                            count += 1
        return count

    template_name = g_report_template_name
    path_template = str(Path(uf.main_folder) / Path(uf.file_folder) / Path(template_name))

    # Получаем рабочую книгу из шаблона
    wb = openpyxl.load_workbook(path_template)

    # Создаём буфер для наполнения
    buffer = io.BytesIO()

    # ===================================================================================================================
    ws_tech = wb['tech']
    if ws_tech:
        G_STYLE_FONT_SIMPLE = extract_cell_styles(ws_tech['B2'])
        G_STYLE_FONT_FORMULA = extract_cell_styles(ws_tech['B3'])
        G_STYLE_FONT_TITLE_LVL0 = extract_cell_styles(ws_tech['B4'])
        G_STYLE_FONT_TITLE_LVL1 = extract_cell_styles(ws_tech['B5'])
        # ===========================================================
        G_STYLE_FULL_BORDER = extract_fast_border(ws_tech['B7'])
        G_STYLE_FULL_BORDER2 = NamedStyle(name="user_custom_border")
        G_STYLE_FULL_BORDER2.border = G_STYLE_FULL_BORDER
        G_STYLE_FULL_BORDER = True
        if "user_custom_border" not in wb.named_styles:
            wb.add_named_style(G_STYLE_FULL_BORDER2)
        # ===========================================================
        G_STYLE_FORMAT_PERCENTAGE = ws_tech['B8'].number_format
        G_STYLE_FONT_TITLE_LVL1_GROUP1 = extract_cell_styles(ws_tech['B9'])
        G_STYLE_FONT_TITLE_LVL2_GROUP1 = extract_cell_styles(ws_tech['B10'])
        G_STYLE_FONT_CHECKER1 = extract_cell_styles(ws_tech['B11'])

        G_STYLE_RULE_GREEN_RED_DASH = []
        for cf in ws_tech.conditional_formatting:
            # RULES: GREEN, RED, DASH
            if 'B6' in cf.cells:
                for rule in cf.rules:
                    G_STYLE_RULE_GREEN_RED_DASH.append(rule)
            # RULE: DASH
            if 'B12' in cf.cells:
                for rule in cf.rules:
                    G_STYLE_RULE_DASH_FOR_ZERO = rule
    # ===================================================================================================================
    count_columns = len(columns)

    columns_layout = []
    for index_column, column in enumerate(columns):
        ColumnType = column.get('ColumnType')
        if ColumnType == 'Selected':
            simple_key = f"year{column.get('dateRange')[0][-4:]}"
            columns_layout.append({
                'Letter': '',
                'ColumnType': ColumnType,
                'SrcKey': column.get('SrcKey'),
                'type': simple_key,
                'data_type_col': index_column,
                'IsNeedMerge': True if index_column == 0 else False,
                'MergeCount': count_columns,
                'col_name': get_txt_col(column),
                'internal_key': get_internal_key(column, simple_key),
                'period': column.get('dateRange')[0][-4:]
            })
        elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete', 'PercentOfOutput'):
            # if 'ptr' in column:
            #     col = column.get('ptr')
            #     simple_key = f"year{col.get('dateRange')[0][-4:]}"
            FormulaLink = copy.deepcopy(column.get('FormulaLink'))
            columns_layout.append({
                'Letter': '',
                'ColumnType': ColumnType,
                'SrcKey': column.get('SrcKey'),
                'type': 'year',
                'data_type_col': index_column,
                'IsNeedMerge': column.get('IsStartGroup', False),
                'MergeCount': get_count_merge_group(columns, column),
                'col_name': get_txt_col(column),
                'GroupName': column.get('GroupName', ''),
                'internal_key': '',  # get_internal_key(column, simple_key),
                'FormulaLink': FormulaLink,
                'period': ''
            })

    for q in range(1, 5):
        for index_column, column in enumerate(columns):
            ColumnType = column.get('ColumnType')
            if ColumnType == 'Selected':
                simple_key = f"Q{q}"
                columns_layout.append({
                    'Letter': '',
                    'ColumnType': ColumnType,
                    'SrcKey': column.get('SrcKey'),
                    'type': simple_key,
                    'data_type_col': index_column,
                    'IsNeedMerge': True if index_column == 0 else False,
                    'MergeCount': count_columns,
                    'col_name': get_txt_col(column),
                    'internal_key': get_internal_key(column, f"year{column.get('dateRange')[0][-4:]}:{simple_key}"),
                    'period': column.get('dateRange')[0][-4:]
                })
            elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete', 'PercentOfOutput'):
                # if 'ptr' in column:
                #     col = column.get('ptr')
                simple_key = f"Q{q}"
                FormulaLink = copy.deepcopy(column.get('FormulaLink'))
                columns_layout.append({
                    'Letter': '',
                    'ColumnType': ColumnType,
                    'SrcKey': column.get('SrcKey'),
                    'type': simple_key,
                    'data_type_col': index_column,
                    'IsNeedMerge': column.get('IsStartGroup', False),
                    'MergeCount': get_count_merge_group(columns, column),
                    'col_name': get_txt_col(column),
                    'GroupName': column.get('GroupName', ''),
                    'internal_key': '',  # get_internal_key(column, simple_key),
                    'FormulaLink': FormulaLink,
                    'period': ''
                })
        for m in range(1, 4):
            for index_column, column in enumerate(columns):
                ColumnType = column.get('ColumnType')
                if ColumnType == 'Selected':
                    simple_key = f"M{q}_{m}"
                    columns_layout.append({
                        'Letter': '',
                        'ColumnType': ColumnType,
                        'SrcKey': column.get('SrcKey'),
                        'type': simple_key,
                        'data_type_col': index_column,
                        'IsNeedMerge': True if index_column == 0 else False,
                        'MergeCount': count_columns,
                        'col_name': get_txt_col(column),
                        'calmonth': (q - 1) * 3 + m,
                        'internal_key': get_internal_key(column, f"year{column.get('dateRange')[0][-4:]}:{simple_key}"),
                        'period': column.get('dateRange')[0][-4:]
                    })
                elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete', 'PercentOfOutput'):
                    # if 'ptr' in column:
                    #     col = column.get('ptr')
                    simple_key = f"M{q}_{m}"
                    FormulaLink = copy.deepcopy(column.get('FormulaLink'))
                    columns_layout.append({
                        'Letter': '',
                        'ColumnType': ColumnType,
                        'SrcKey': column.get('SrcKey'),
                        'type': simple_key,
                        'data_type_col': index_column,
                        'IsNeedMerge': column.get('IsStartGroup', False),
                        'MergeCount': get_count_merge_group(columns, column),
                        'col_name': get_txt_col(column),
                        'GroupName': column.get('GroupName', ''),
                        'internal_key': '',  # get_internal_key(column, simple_key),
                        'FormulaLink': FormulaLink,
                        'period': ''
                    })

    # ===================================================================================================================
    # Удаление неиспользуемых листов
    # if download_type == 'simple':
    #     sheets_to_keep = set()
    #
    #     for factory_id in selected_factories:
    #         range_name = f"_BS{factory_id}"
    #         defined_name = wb.defined_names.get(range_name)
    #
    #         if not defined_name:
    #             continue
    #
    #         try:
    #             for sheet_title, cell_range in defined_name.destinations:
    #                 if sheet_title in wb.sheetnames:
    #                     sheets_to_keep.add(sheet_title)
    #                 break
    #         except Exception as e:
    #             pass
    #
    #     if not sheets_to_keep:
    #         return False
    #
    #     for sheet_name in wb.sheetnames:
    #         if sheet_name not in sheets_to_keep and sheet_name != 'tech':
    #             wb.remove(wb[sheet_name])
    # ===================================================================================================================

    def init_some_data(type_sheet, sheet_ids, sheet_all, named_rng_names):
        # _SET_ROW:
        #
        nonlocal storage_sheet

        # for sheet_id in sheet_ids:
        for sheet_id in sheet_all:
            try:
                def_range_set_row = wb.defined_names[f'{named_rng_names[0]}{sheet_id}']
                def_range_ik = wb.defined_names[f'{named_rng_names[1]}{sheet_id}']
            except Exception as e:
                continue

            sheet_name_set_row = ''
            set_row_left_col_index = -1
            set_row_right_col_index = -1
            FirstRowData = -1
            LastRowData = -1
            first_row = -1

            for sheet_name_rng_bs, cell_coordinates_rng_set_row in def_range_set_row.destinations:
                set_row_left_col_index, _, set_row_right_col_index, _ = range_boundaries(cell_coordinates_rng_set_row)
                sheet_name_set_row = sheet_name_rng_bs

            if sheet_name_set_row in wb.sheetnames:
                sheet = wb[sheet_name_set_row]
            else:
                continue

            if sheet_id not in sheet_ids:
                sheet.sheet_state = 'veryHidden'
                continue

            if type_sheet == 'type_summary_rep':
                # Для СВОДОВ считать процент выхода нет необходимости
                struct_columns = []
                for col in columns:
                    if col.get('ColumnType') != 'PercentOfOutput':
                        struct_columns.append(copy.deepcopy(col))

                sheet_columns_layout = []
                for col in columns_layout:
                    if col.get('ColumnType') != 'PercentOfOutput':
                        loc_col = copy.deepcopy(col)
                        if loc_col and loc_col.get('IsNeedMerge', False):
                            loc_col['MergeCount'] = get_count_merge_group(struct_columns,
                                                                          columns[col.get('data_type_col')])
                        sheet_columns_layout.append(loc_col)
            else:
                sheet_columns_layout = copy.deepcopy(columns_layout)
                struct_columns = copy.deepcopy(columns)

            len_columns = len(struct_columns)

            storage_sheet[sheet_name_set_row]['columns_layout'] = sheet_columns_layout
            storage_sheet[sheet_name_set_row]['struct_columns'] = struct_columns

            storage_sheet[sheet_name_set_row]['columns_settings'] = {
                EnumColumnSettings.KEY_BS: set_row_left_col_index + 0,
                EnumColumnSettings.KEY_SPEC: set_row_left_col_index + 1,
                EnumColumnSettings.KEY_INPUT: set_row_left_col_index + 2,
                EnumColumnSettings.FORMULA_MONTH: set_row_left_col_index + 3,
                EnumColumnSettings.FORMULA_POO: set_row_left_col_index + 4,
                EnumColumnSettings.ROW_CHECK: set_row_left_col_index + 5
            }

            storage_sheet[sheet_name_set_row]['SET_ROW_LIST'] = {}
            storage_sheet[sheet_name_set_row]['SET_ROW_IDX'] = {}
            storage_sheet[sheet_name_set_row]['SET_ROW_KEY'] = {}

            for key, idx_col in storage_sheet[sheet_name_set_row]['columns_settings'].items():
                if key not in storage_sheet[sheet_name_set_row]['SET_ROW_LIST']:
                    storage_sheet[sheet_name_set_row]['SET_ROW_LIST'][key] = []
                if key not in storage_sheet[sheet_name_set_row]['SET_ROW_KEY']:
                    storage_sheet[sheet_name_set_row]['SET_ROW_KEY'][key] = {}

            for sheet_name_rng_ik, cell_coordinates_rng_ik in def_range_ik.destinations:
                _, FirstRowData, _, _ = range_boundaries(cell_coordinates_rng_ik)

            if FirstRowData == -1:
                continue
            else:
                first_row = FirstRowData - 3  # всего строк для заголовков 3
                FirstRowData += 1

            if sheet and set_row_right_col_index != -1:
                dict_bs = []
                loc_index_offset = set_row_right_col_index + 1 + offset_ind_col
                loc_bs_calc_mapping = []
                last_row = sheet.max_row

                for i in range(1, last_row + 1):
                    # Сбор настроек в виде мэпинга по строкам с каждого листа
                    # из именованного диапазона, содержащего, *_SET_ROW*
                    if i >= FirstRowData:
                        for key, idx_col in storage_sheet[sheet_name_set_row]['columns_settings'].items():
                            cell_value = sheet.cell(row=i, column=idx_col).value
                            if cell_value is not None:
                                cell_value = str(cell_value)
                                storage_sheet[sheet_name_set_row]['SET_ROW_LIST'][key].append({
                                    i: cell_value
                                })

                                if i not in storage_sheet[sheet_name_set_row]['SET_ROW_IDX']:
                                    storage_sheet[sheet_name_set_row]['SET_ROW_IDX'][i] = {}
                                storage_sheet[sheet_name_set_row]['SET_ROW_IDX'][i][key] = {
                                    'column': idx_col,
                                    'value': cell_value
                                }

                                storage_sheet[sheet_name_set_row]['SET_ROW_KEY'][key][cell_value] = i

                                # Нет необходимости проверять на большее значение, т.к. основной цикл идёт по строкам сверху вниз
                                storage_sheet[sheet_name_set_row]['LastRowData'] = i
                                if storage_sheet[sheet_name_set_row].get('FirstRowWithBS', '') == '':
                                    storage_sheet[sheet_name_set_row]['FirstRowWithBS'] = i

                # Способ хранения литералов и индексов для столбцов макета листа:
                # {номер группы}:{индекс в группе}
                #
                # Пример:
                # {0:0} {0:1} {0:2} {0:3} {0:4}
                # {1:0} {1:1} {1:2} {1:3} {1:4}
                # {2:0} {2:1} {2:2} {2:3} {2:4}
                # ...
                #
                # Итоговый объём может выглядеть так: {0-16:0-4} 17 групп из 5 элементов
                for idx, col in enumerate(sheet_columns_layout):
                    col_num = idx + loc_index_offset
                    col_letter = get_column_letter(col_num)

                    unique_key_column = f'{idx // len_columns}:{col.get("data_type_col")}'
                    storage_sheet[sheet_name_set_row]['unique_key_column'][unique_key_column] = {
                        'index': col_num,
                        'Letter': col_letter
                    }

                    src_key_column = f'{idx // len_columns}:{col.get("SrcKey")}'
                    storage_sheet[sheet_name_set_row]['src_key_column'][src_key_column] = {
                        'index': col_num,
                        'Letter': col_letter
                    }

                    storage_sheet[sheet_name_set_row]['col'][idx] = {
                        'index': col_num,
                        'Letter': col_letter
                    }

                for idx, col in enumerate(sheet_columns_layout):
                    FormulaLink = col.get('FormulaLink', '')
                    if FormulaLink:
                        total = FormulaLink.get('total')
                        for i in range(1, total + 1):
                            formula_col = FormulaLink.get(f'col{i}', '')
                            if formula_col:
                                formula_col['Letter'] = storage_sheet[sheet_name_set_row]['src_key_column'] \
                                    [f'{idx // len_columns}:{formula_col["index"]}'].get('Letter')

    def prepare_and_fill_data(type_sheet, sheet_ids, named_rng_names):
        nonlocal storage_sheet

        for sheet_id in sheet_ids:
            try:
                def_range_bs = wb.defined_names[f'{named_rng_names[0]}{sheet_id}']
                def_range_ik = wb.defined_names[f'{named_rng_names[1]}{sheet_id}']
            except Exception as e:
                continue

            sheet_name_set_row = ''
            set_row_left_col_index = -1
            set_row_right_col_index = -1
            FirstRowData = -1
            LastRowData = -1
            first_row = -1

            for sheet_name_rng_bs, cell_coordinates_rng_set_row in def_range_bs.destinations:
                set_row_left_col_index, _, set_row_right_col_index, _ = range_boundaries(cell_coordinates_rng_set_row)
                sheet_name_set_row = sheet_name_rng_bs

            if sheet_name_set_row in wb.sheetnames:
                sheet = wb[sheet_name_set_row]
            else:
                continue

            for sheet_name_rng_ik, cell_coordinates_rng_ik in def_range_ik.destinations:
                _, FirstRowData, _, _ = range_boundaries(cell_coordinates_rng_ik)

            if FirstRowData == -1:
                continue
            else:
                first_row = FirstRowData - 3  # всего строк для заголовков 3
                FirstRowData += 1

            if sheet:
                sheet.sheet_state = 'visible'

            if sheet and set_row_right_col_index != -1:

                sheet_columns_layout = storage_sheet[sheet_name_set_row]['columns_layout']
                struct_columns = storage_sheet[sheet_name_set_row]['struct_columns']
                key_bs = storage_sheet[sheet_name_set_row]['SET_ROW_LIST'][EnumColumnSettings.KEY_BS]
                LastRowData = storage_sheet[sheet_name_set_row]['LastRowData']
                SetRowIdx = storage_sheet[sheet_name_set_row]['SET_ROW_IDX']

                dict_bs = []
                loc_index_offset = set_row_right_col_index + 1 + offset_ind_col
                loc_bs_calc_mapping = []
                last_row = sheet.max_row

                for item_dict in key_bs:
                    value = list(item_dict.values())[0]
                    if value is not None and str(value).isdigit():
                        dict_bs.append(value)

                # for i in range(1, last_row + 1):
                #     cell = sheet.cell(row=i, column=set_row_right_col_index)
                #     cell_value = cell.value
                #     if cell_value is not None:
                #         if i >= FirstRowData:
                #             con1 = str(cell_value).isdigit()
                #             if con1:
                #                 dict_bs.append(cell_value)

                if G_STYLE_FULL_BORDER:
                    _min_col = set_row_right_col_index + 1 + offset_ind_col
                    _max_col = set_row_right_col_index + 1 + offset_ind_col + len(sheet_columns_layout) - 1

                    cells_dict = sheet._cells

                    for row_idx in range(first_row, LastRowData + 1):
                        for col_idx in range(_min_col, _max_col + 1):
                            coords = (row_idx, col_idx)

                            if coords not in cells_dict:
                                cells_dict[coords] = Cell(sheet, row=row_idx, column=col_idx)

                            cells_dict[coords].style = "user_custom_border"

                sheet.row_dimensions[first_row + 0].height = 20  # column title
                sheet.row_dimensions[first_row + 1].height = 40  # column name
                sheet.row_dimensions[first_row + 2].height = 20  # column helper description
                sheet.row_dimensions[first_row + 3].height = 20  # column internal_key

                multi_range_rules = MultiCellRange()
                multi_range_rule_dash_for_zero = MultiCellRange()
                for idx, col in enumerate(sheet_columns_layout):
                    col_num = idx + loc_index_offset
                    col_letter = get_column_letter(col_num)
                    col['Letter'] = col_letter
                    ColumnType = col.get('ColumnType')

                    if ColumnType == 'Selected':
                        # Заголовок уровня 0
                        cell = sheet.cell(row=first_row + 1, column=col_num)
                        sheet.merge_cells(start_row=first_row + 1, start_column=col_num, end_row=first_row + 2,
                                          end_column=col_num)
                        set_value_cell(cell, col.get('col_name'), EnumCellType.TITLE_LVL0)

                        sheet.column_dimensions[col_letter].width = 25  # 22 #16.29
                    else:
                        # Заголовок уровня 1
                        cell = sheet.cell(row=first_row + 2, column=col_num)
                        if G_STYLE_RULE_GREEN_RED_DASH and ColumnType in ('SecondMinusFirst', 'PercentOfComplete'):
                            current_range = f'${col_letter}${FirstRowData}:${col_letter}${LastRowData}'
                            multi_range_rules.add(current_range)
                        if G_STYLE_RULE_DASH_FOR_ZERO and ColumnType == 'PercentOfOutput':
                            current_range = f'${col_letter}${FirstRowData}:${col_letter}${LastRowData}'
                            multi_range_rule_dash_for_zero.add(current_range)

                        if ColumnType != 'Selected':
                            set_value_cell(cell, col.get('col_name'), EnumCellType.TITLE_GROUP1_LVL2)
                        else:
                            set_value_cell(cell, col.get('col_name'), EnumCellType.TITLE_LVL0)

                        sheet.column_dimensions[col_letter].width = 15

                    # Заголовок уровня 2 (внутренние ключи)
                    set_value_cell(sheet.cell(row=first_row + 3, column=col_num), col.get('internal_key'))

                    if 'IsNeedMerge' in col and col['IsNeedMerge']:
                        MergeCount = col.get('MergeCount', 0)
                        if MergeCount > 1:
                            if ColumnType == 'Selected':
                                q = 0
                                # Заголовок уровня 0
                                ### sheet.merge_cells(start_row=first_row, start_column=col_num, end_row=first_row,
                                ###                   end_column=col_num + col['MergeCount'] - 1)
                            else:
                                # Заголовок уровня 1 (на строку ниже)
                                sheet.merge_cells(start_row=first_row + 1, start_column=col_num, end_row=first_row + 1,
                                                  end_column=col_num + MergeCount - 1)
                        elif MergeCount == 1:
                            sheet.merge_cells(start_row=first_row + 1, start_column=col_num,
                                              end_row=first_row + 2,
                                              end_column=col_num + MergeCount - 1)

                        if ColumnType == 'Selected':
                            q = 0
                            # cell = sheet.cell(row=first_row, column=col_num)
                            # if 'year' in col['type']:
                            #     set_value_cell(cell,col.get("period"), EnumColumnType.TITLE_LVL1)
                            # elif 'Q' in col['type']:
                            #     q_number = col['type'][1]
                            #     set_value_cell(cell,f'{col.get("period")} год {q_number} квартал', EnumColumnType.TITLE_LVL1)
                            # elif 'M' in col['type']:
                            #     set_value_cell(cell,f'{col.get("period")} год {month[col.get("calmonth")]}', EnumColumnType.TITLE_LVL1)
                        else:
                            GroupName = col.get('GroupName', '')
                            if GroupName:
                                cell = sheet.cell(row=first_row + 1, column=col_num)
                                set_value_cell(cell, GroupName, EnumCellType.TITLE_GROUP1_LVL1)

                    if ColumnType == 'Selected':
                        cell = sheet.cell(row=first_row, column=col_num)
                        if 'year' in col['type']:
                            set_value_cell(cell, col.get("period"), EnumCellType.TITLE_LVL1)
                        elif 'Q' in col['type']:
                            q_number = col['type'][1]
                            set_value_cell(cell, f'{col.get("period")} год {q_number} квартал',
                                           EnumCellType.TITLE_LVL1)
                        elif 'M' in col['type']:
                            set_value_cell(cell, f'{col.get("period")} год {month[col.get("calmonth")]}',
                                           EnumCellType.TITLE_LVL1)

                if G_STYLE_RULE_GREEN_RED_DASH:
                    for rule in G_STYLE_RULE_GREEN_RED_DASH:
                        sheet.conditional_formatting.add(str(multi_range_rules), rule)
                if G_STYLE_RULE_DASH_FOR_ZERO:
                    sheet.conditional_formatting.add(str(multi_range_rule_dash_for_zero), G_STYLE_RULE_DASH_FOR_ZERO)

                query_res_for_column = []
                if type_sheet == 'type_factory':
                    loc_settings = next((row for row in settings if str(row.id) == sheet_id), None)
                    for index_column, column in enumerate(struct_columns, start=1):
                        if column.get('ColumnType') == 'Selected':
                            year = column.get('dateRange')[0][-4:]
                            ver_plan = column.get('versionPlaning', 0)
                            var_plan = column.get('variantPlaning', 0)
                            do = loc_settings.DO
                            pj = loc_settings.pj
                            data_type = int(column.get('typeData'))
                            query_res_for_column.append(
                                get_row_list_msb_zuv_d816_4(year, ver_plan, var_plan, dict_bs, do, pj, data_type))
                elif type_sheet == 'type_summary_rep':
                    for index_column, column in enumerate(struct_columns, start=1):
                        if column.get('ColumnType') == 'Selected':
                            query_res_for_column.append(0)

                # основная функция для заполнения таблицы подготовленными данными
                fill_flat_data(type_sheet,
                               offset_ind_col,
                               sheet,
                               set_row_right_col_index,
                               FirstRowData,
                               LastRowData,
                               sheet_columns_layout,
                               query_res_for_column,
                               download_type,
                               struct_columns,
                               storage_sheet)

    init_some_data('type_factory', selected_factories, factories_all, ['_SET_ROW', '_INTERNAL_KEY'])
    init_some_data('type_summary_rep', selected_reports, reports_all, ['_SUM_REP_SET_ROW', '_SUM_REP_INTERNAL_KEY'])

    prepare_and_fill_data('type_factory', selected_factories, ['_SET_ROW', '_INTERNAL_KEY'])
    prepare_and_fill_data('type_summary_rep', selected_reports, ['_SUM_REP_SET_ROW', '_SUM_REP_INTERNAL_KEY'])

    # ===================================================================================================================

    # Сохраняем подготовленные данные из шаблона
    wb.save(buffer)
    # Откатываем курсор в самое начало
    buffer.seek(0)
    # имя файла
    filename = template_name

    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


# ============================================================================================
def fill_flat_data(
        type_sheet,
        offset_ind_col,
        sheet,
        bs_col_index,
        FirstRowData,
        LastRowData,
        sheet_columns_layout,
        query_res_for_column,
        download_type,
        struct_columns,
        loc_storage_sheet):
    len_columns = len(struct_columns)

    def get_col_letter_by_type(col_type, data_type_col):
        for idx, col in enumerate(sheet_columns_layout):
            if col["type"] == col_type and col["data_type_col"] == data_type_col:
                return col.get('Letter')
        return None

    def set_calc_cell_val(cell, col_letter, query_res, col, idx, ColumnSettings=EnumColumnSettings.FORMULA_MONTH,
                          ColumnCellType: EnumCellType = EnumCellType.INPUT):

        def get_sheetname_from_sheetkey(sheet_key):
            sheet_name = ''
            named_rng_ik_name = f'_INTERNAL_KEY{sheet_key}'
            if named_rng_ik_name in wb.defined_names:
                nr_rng = wb.defined_names[named_rng_ik_name]
                destinations = list(nr_rng.destinations)
                if destinations:
                    sheet_name, cell_coord = destinations[0]
            return sheet_name

        # 1_10030001 -> Лист1!AB4 или 1_10030001 -> 4 (эта логика нереализована[OnlyRow=True])
        def get_cell_addr_by_linked_bs(p, OnlyRow=False):
            sheet_key, bs = p.split('_', 1)
            if sheet_key and bs:
                sheet_name = get_sheetname_from_sheetkey(sheet_key)
                need_col_letter = ''
                need_row = -1

                if sheet_name:
                    check_rec_formula_sheet_name = loc_storage_sheet.get(sheet_name)
                    if check_rec_formula_sheet_name:
                        check_rec_unique_key_column = check_rec_formula_sheet_name.get('unique_key_column')
                        if check_rec_unique_key_column:
                            unique_key_column = f'{idx // len_columns}:{col.get("data_type_col")}'
                            check_rec_this_key = check_rec_unique_key_column.get(unique_key_column, None)
                            if check_rec_this_key:
                                need_col_letter = check_rec_this_key.get('Letter')
                        check_rec_set_row_key = check_rec_formula_sheet_name.get('SET_ROW_KEY')
                        if check_rec_set_row_key:
                            check_rec_key_bs = check_rec_set_row_key.get(EnumColumnSettings.KEY_BS)
                            if check_rec_key_bs:
                                check_rec_finded_row = check_rec_key_bs.get(bs)
                                if check_rec_finded_row:
                                    need_row = check_rec_finded_row
                                    if OnlyRow:
                                        return need_row
                if isinstance(need_col_letter, str) and isinstance(need_row, int):
                    if need_col_letter and need_row != -1:
                        return f"'{sheet_name}'!{need_col_letter}{need_row}"
                return None

        # 10030001 -> AB4 или 10030001 -> 4
        def get_cell_addr_by_bs(p, OnlyRow=False):
            # check_rec_sheet_name.get('SET_ROW_KEY').get('key_bs').get('100331977')
            check_rec_set_row_key = check_rec_sheet_name.get('SET_ROW_KEY')
            if check_rec_set_row_key:
                check_rec_key_bs = check_rec_set_row_key.get(EnumColumnSettings.KEY_BS)
                if check_rec_key_bs:
                    need_row = check_rec_key_bs.get(p)
                    if need_row:
                        return need_row if OnlyRow else f'{col_letter}{need_row}'
            return None

        # трансформировать формулу в которой бюджетные статьи имеют ссылки на соответствующие листы, пример:
        # 1_100333000+2_100333001+3_100333002
        # где "1_" - означает ключ листа, а остальное это номер бюджетной статьи
        # Если в формуле не встречается "1_", то текущее значение отличное от нуля считается статьёй
        # с ссылкой на текущий лист из которого и была передана формула для трансформации
        def get_transformed_formula_linked_old(src_formula):
            formula = src_formula
            # исключаем ошибочные формулы в виде ссылок на другие листы (Excel-формулы)
            if '!' in formula:
                src_formula = '0'
            else:
                # сложная регулярка для поиска наших шаблонов вида:
                # 1) 100330001              или 1_100330001
                # 2) (100330001:100330003)  или (1_100330001:1_100330001)
                # 3) {100330001:-1}         или {1_100330001:-1}
                # 4) F6 (Обычная ссылка на ячейку в формулах)

                pattern = r'(\{-?[\d_]+:-?[\d_]+\}|[А-Яа-яA-Za-z_]+\(|\$?[A-Za-z0-9_]+|[^0-9_${}])'
                parts = re.findall(pattern, formula)
                parts = [p for p in parts if p]

                # разделённую на части формулу обработаем, чтобы подменить ключи статей на ссылки ячеек для формул
                src_formula = ''
                for p in parts:
                    FindState = 0
                    if p.startswith('='):
                        # пропускаем обработку такого символа, игнорируя его добавление
                        continue
                    elif p.startswith('$'):
                        p = p.replace('$', '')
                    elif '_' in p:
                        FindState = 1
                        p = get_cell_addr_by_linked_bs(p)
                        if p != None:
                            FindState = 2
                    elif p.isdigit() and int(p) != 0:
                        FindState = 1
                        p = get_cell_addr_by_bs(p)
                        if p != None:
                            FindState = 2
                    elif re.match(r'^\{(-?[\d_]+):(-?[\d_]+)\}$', p):
                        FindState = 1
                        match = re.match(r'^\{(-?[\d_]+):(-?[\d_]+)\}$', p)

                        if match:
                            col = None

                            row_numb = match.group(1)
                            col_offset = match.group(2)
                            try:
                                col_offset = int(col_offset)
                            except ValueError:
                                p = 0
                                pass

                            if '_' in row_numb:
                                # Нет логики обработки ссылочных статей(ссылка ячейки на другой лист)
                                p = 0
                                # loc_sheet_name = ''
                                # row_numb = get_cell_addr_by_linked_bs(row_numb, True)
                                # sheet_key, bs = p.split('_', 1)
                                # if sheet_key and bs:
                                #     loc_sheet_name = get_sheetname_from_sheetkey(sheet_key)
                                #
                                # if loc_sheet_name:
                                #     loc_storage_sheet[loc_sheet_name]['columns_layout'][ + col_offset]
                                #     FindState = 2
                            else:
                                row_numb = get_cell_addr_by_bs(row_numb, True)
                                if row_numb == None:
                                    p = 0
                                else:
                                    col = sheet_columns_layout[idx + col_offset]
                                    p = f'{col.get("Letter")}{row_numb}'
                                    FindState = 2
                    # elif any(re.match(r'^\$?[A-Za-z]{1,2}\$?\d+$', p) for p in parts):
                    elif re.match(r'^\$?[A-Za-z]{1,3}\$?\d+$', p):
                        # удаляем значение столбца далее заменив его на текущий столбец
                        loc_row = re.sub(r'[A-Za-z$]', '', p)
                        p = f'{col_letter}{loc_row}'
                        FindState = 2
                    if FindState == 1:
                        p = 0
                    src_formula = f'{src_formula}{p}'
                pattern = re.compile(r'\b0:0\b|\b0:|:0\b')
                src_formula = pattern.sub('0', src_formula)
                return src_formula
            return '0'

        def get_transformed_formula_linked(src_formula):
            formula = src_formula
            src_formula = ''
            # исключаем ошибочные формулы в виде ссылок на другие листы (Excel-формулы)
            if '!' in formula:
                return '0'
            pattern = re.compile(
                r'(?P<CELL_OFFSET>\{[^}]+\})'
                r'|(?P<ESCAPED>\$[0-9]+)'
                r'|(?P<FUNC_OR_VAR>[a-zA-Zа-яА-ЯёЁ0-9_]+)'
                r'|(?P<OPERATOR>[;(),+\-*/:])'
                r'|(?P<NUMBER>[0-9]+)'
            )

            for match in pattern.finditer(formula):
                token_type = match.lastgroup
                value = match.group()
                new_value = '0'

                # {A1:2}, {1000330001:0} - обработка шаблона
                if token_type == 'CELL_OFFSET':
                    inner_content = value[1:-1]
                    row_numb, col_offset = inner_content.split(':')
                    loc_col = None

                    try:
                        col_offset = int(col_offset)
                        if re.match(r'^\$?[A-Za-z]{1,3}\$?\d+$', row_numb):
                            loc_row = re.sub(r'[A-Za-z$]', '', row_numb)
                            loc_col = sheet_columns_layout[idx + col_offset]
                            new_value = f'{loc_col.get("Letter")}{loc_row}'
                        elif '_' not in row_numb:
                            row_numb = get_cell_addr_by_bs(row_numb, True)
                            if row_numb is not None:
                                loc_col = sheet_columns_layout[idx + col_offset]
                                new_value = f'{loc_col.get("Letter")}{row_numb}'
                        else:
                            # логики для обработки ссылочной статьи нет
                            pass

                    except Exception as e:
                        pass

                # Округл - Имя функции формулы-xls
                # A1 - ссылка на ячейку
                # 1000330001 - ключ статьи
                # 1_1000330001 - ключ статьи со ссылкой на лист
                elif token_type == 'FUNC_OR_VAR':
                    end_index = match.end()
                    next_char = formula[end_index] if end_index < len(formula) else ''
                    # Проверяем, если попало имя функции, то просто передаём его дальше без обработки
                    if next_char == '(':
                        new_value = f'{value}'
                    elif re.match(r'^\$?[A-Za-z]{1,3}\$?\d+$', value):
                        # удаляем значение столбца далее заменив его на текущий столбец
                        loc_row = re.sub(r'[A-Za-z$]', '', value)
                        new_value = f'{col_letter}{loc_row}'
                    else:
                        if '_' in value:
                            new_value = get_cell_addr_by_linked_bs(value)
                            if new_value is None:
                                new_value = '0'
                        else:
                            new_value = get_cell_addr_by_bs(value)
                            if new_value is None:
                                new_value = value

                # экранирование для $
                elif token_type == 'ESCAPED':
                    new_value = value.replace('$', '')

                # обычный элемент
                else:
                    new_value = value

                src_formula = f'{src_formula}{new_value}'
            return src_formula if src_formula != '' else '0'

        finded_map = None
        wb = cell.parent.parent

        check_rec_sheet_name = loc_storage_sheet.get(cell.parent.title)
        if check_rec_sheet_name:
            check_rec_set_row_idx = check_rec_sheet_name.get('SET_ROW_IDX')
            if check_rec_set_row_idx:
                check_rec_index = check_rec_set_row_idx.get(cell.row)
                if check_rec_index:
                    check_rec_some_key = check_rec_index.get(ColumnSettings)
                    if check_rec_some_key:
                        check_rec_value = check_rec_some_key.get('value')
                        if check_rec_value:
                            finded_map = check_rec_value
        if finded_map:
            src_formula = get_transformed_formula_linked(finded_map)
            # openpyxl не умеет в русские формулы поэтому конвертируем из русской формулы в английскую
            # чтобы на выходе в Exel или Р7-офис формула автоматически пересчитывались (работали)
            set_value_cell(cell, formula_translator.convert_russian_formula(src_formula), ColumnCellType)
            return
            # if finded_map.get('bs') == cell_bs.value and finded_map.get('calc') != None and finded_map.get('calc') != '':
            #     src_formula = get_transformed_formula_linked(finded_map.get('calc'))
            #     # openpyxl не умеет в русские формулы поэтому конвертируем из русской формулы в английскую
            #     # чтобы на выходе в Exel или Р7-офис формула автоматически пересчитывались (работали)
            #     set_value_cell(cell, ft.convert_russian_formula(src_formula), ColumnType)
            #     return
            # elif finded_map.get('formula', ''):
            #     src_formula = get_transformed_formula_linked(finded_map.get('formula'))
            #
            #     # openpyxl не умеет в русские формулы поэтому конвертируем из русской формулы в английскую
            #     # чтобы на выходе в Exel или Р7-офис формула автоматически пересчитывались (работали)
            #     set_value_cell(cell, ft.convert_russian_formula(src_formula), ColumnType)
            #     return
        if query_res:
            if query_res[0] == 0:
                set_value_cell(cell, 0)
                return
            else:
                for row in query_res:
                    try:
                        if int(row.bs) == int(cell_bs.value) and col["calmonth"] == row.calmonth:
                            set_value_cell(cell, row.sum, ColumnCellType)
                            return
                    except Exception as e:
                        pass
        return

    def get_filled_formula_by_col(col, l_col: list):
        if 'FormulaLink' in col and 'total' in col.get('FormulaLink'):
            FormulaLink = col.get('FormulaLink')
            total = FormulaLink.get('total')
            if total == len(l_col):
                # src_formula = f"=ОКРУГЛ({letter_col}{i}/{letter_col}{FirstRowData}*100;6)"
                src_formula = FormulaLink.get('Formula')  # [1:]
                cell_address = []
                for i in range(1, total + 1):
                    Letter = FormulaLink.get(f'col{i}').get('Letter')
                    row = l_col[i - 1]
                    cell_address.append(f'{Letter}{row}')
                src_formula = src_formula.format(*cell_address)
                return src_formula
        return ''

    cell_bs = None
    cell_calc = None

    SetRowIdx = loc_storage_sheet[sheet.title]['SET_ROW_IDX']
    key_bs = loc_storage_sheet[sheet.title]['columns_settings'][EnumColumnSettings.KEY_BS]
    key_spec = loc_storage_sheet[sheet.title]['columns_settings'][EnumColumnSettings.KEY_SPEC]
    formula_month = loc_storage_sheet[sheet.title]['columns_settings'][EnumColumnSettings.FORMULA_MONTH]
    formula_poo = loc_storage_sheet[sheet.title]['columns_settings'][EnumColumnSettings.FORMULA_POO]

    prepared_column_offset_1 = -1
    prepared_column_offset_2 = -1
    if type_sheet == 'type_factory':
        prepared_column_offset_1 = key_bs
        prepared_column_offset_2 = formula_month
    elif type_sheet == 'type_summary_rep':
        prepared_column_offset_1 = formula_month  # key_bs
        prepared_column_offset_2 = formula_month

    FirstRowWithBS = -1
    check_rec_title = loc_storage_sheet.get(sheet.title)
    if check_rec_title:
        check_rec_row_with_bs = check_rec_title.get('FirstRowWithBS')
        if check_rec_row_with_bs:
            FirstRowWithBS = check_rec_row_with_bs

    if FirstRowWithBS == -1:
        FirstRowWithBS = FirstRowData

    # for i in range(FirstRowData, LastRowData + 1):
    for i, v in SetRowIdx.items():
        if i == 9 and sheet.title == 'Баланс ЗС':
            q = 0
        cell_key_spec = sheet.cell(row=i, column=key_spec)
        cell_bs = sheet.cell(row=i, column=prepared_column_offset_1)
        cell_calc = sheet.cell(row=i, column=prepared_column_offset_2)

        if not (isinstance(cell_bs.value, str) or isinstance(cell_bs.value, int) or cell_key_spec.value is not None):
            continue

        # расчёт для специальных строк
        if "T" in str(cell_key_spec.value):
            if cell_calc and cell_calc.value is not None:
                for idx, col in enumerate(sheet_columns_layout):
                    col_letter = col.get('Letter')
                    data_type_col = col["data_type_col"]
                    SrcKey = col["SrcKey"]
                    ColumnType = col["ColumnType"]
                    cell_val = sheet[f"{col_letter}{i}"]

                    if ColumnType == 'Selected':
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx)
                    elif ColumnType == 'PercentOfOutput':
                        cell_val = sheet[f"{col_letter}{i}"]
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx)
                        # ColumnSettings=EnumColumnSettings.FORMULA_POO)
                        # src_formula = get_filled_formula_by_col(col, [i, FirstRowWithBS])
                        # set_value_cell(sheet[f"{col_letter}{i}"],
                        #                formula_translator.convert_russian_formula(src_formula))
                    elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete'):
                        src_formula = get_filled_formula_by_col(col, [i, i])
                        if ColumnType == 'SecondMinusFirst':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.POSITIVE_NEGATIVE)
                        elif ColumnType == 'PercentOfComplete':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.PERCENT)
        # Проверочные строки
        elif "V" in str(cell_key_spec.value):
            for idx, col in enumerate(sheet_columns_layout):
                col_letter = col.get('Letter')
                data_type_col = col["data_type_col"]
                SrcKey = col["SrcKey"]
                ColumnType = col["ColumnType"]
                if ColumnType == 'Selected':
                    cell_val = sheet[f"{col_letter}{i}"]
                    set_calc_cell_val(cell_val, col_letter,
                                      query_res_for_column[SrcKey], col, idx, ColumnCellType=EnumCellType.FONT_CHECKER1)
        # Проверочные строки #2
        elif "S1" in str(cell_key_spec.value):
            for idx, col in enumerate(sheet_columns_layout):
                col_letter = col.get('Letter')
                data_type_col = col["data_type_col"]
                SrcKey = col["SrcKey"]
                ColumnType = col["ColumnType"]
                if ColumnType == 'Selected' and SrcKey == 0 and "M" in col["type"]:
                    cell_val = sheet[f"{col_letter}{i}"]
                    set_calc_cell_val(cell_val, col_letter,
                                      query_res_for_column[SrcKey], col, idx,
                                      ColumnCellType=EnumCellType.FONT_CHECKER1)
        # Подсчёт строк для столбцов отклонение #2
        elif "S2" in str(cell_key_spec.value):
            for idx, col in enumerate(sheet_columns_layout):
                col_letter = col.get('Letter')
                data_type_col = col["data_type_col"]
                SrcKey = col["SrcKey"]
                ColumnType = col["ColumnType"]
                if ColumnType == 'SecondMinusFirst':
                    cell_val = sheet[f"{col_letter}{i}"]
                    set_calc_cell_val(cell_val, col_letter,
                                      query_res_for_column[SrcKey], col, idx,
                                      ColumnCellType=EnumCellType.FONT_CHECKER1)
        # Не справочные строки
        # забираем только не пустые
        elif cell_bs and cell_bs.value is not None and cell_bs.value != 0:
            # (str(cell_bs.value).isdigit() or '_' in str(cell_bs.value) or \
            #  (str(cell_bs.value).startswith('='))): # логика для формул на листах СВОД
            for idx, col in enumerate(sheet_columns_layout):
                col_letter = col.get('Letter')
                data_type_col = col["data_type_col"]
                SrcKey = col["SrcKey"]
                ColumnType = col["ColumnType"]

                if "year" in col["type"]:
                    src_formula = ''
                    if ColumnType == 'Selected':
                        q1 = get_col_letter_by_type("Q1", data_type_col)
                        q2 = get_col_letter_by_type("Q2", data_type_col)
                        q3 = get_col_letter_by_type("Q3", data_type_col)
                        q4 = get_col_letter_by_type("Q4", data_type_col)
                        set_value_cell(sheet[f"{col_letter}{i}"], f"={q1}{i}+{q2}{i}+{q3}{i}+{q4}{i}")
                    elif ColumnType == 'PercentOfOutput':
                        cell_val = sheet[f"{col_letter}{i}"]
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx,
                                          ColumnSettings=EnumColumnSettings.FORMULA_POO)

                        # src_formula = get_filled_formula_by_col(col,[i,FirstRowWithBS])
                        #
                        # set_value_cell(sheet[f"{col_letter}{i}"],
                        #                formula_translator.convert_russian_formula(src_formula))
                    elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete'):
                        src_formula = get_filled_formula_by_col(col, [i, i])
                        if ColumnType == 'SecondMinusFirst':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.POSITIVE_NEGATIVE)
                        elif ColumnType == 'PercentOfComplete':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.PERCENT)
                elif "Q" in col["type"]:
                    q_num = col["type"][1]

                    if ColumnType == 'Selected':
                        m1 = get_col_letter_by_type(f"M{q_num}_1", data_type_col)
                        m2 = get_col_letter_by_type(f"M{q_num}_2", data_type_col)
                        m3 = get_col_letter_by_type(f"M{q_num}_3", data_type_col)
                        set_value_cell(sheet[f"{col_letter}{i}"], f"={m1}{i}+{m2}{i}+{m3}{i}")
                    elif ColumnType == 'PercentOfOutput':
                        cell_val = sheet[f"{col_letter}{i}"]
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx,
                                          ColumnSettings=EnumColumnSettings.FORMULA_POO)
                        # src_formula = get_filled_formula_by_col(col, [i, FirstRowWithBS])
                        # set_value_cell(sheet[f"{col_letter}{i}"],
                        #                formula_translator.convert_russian_formula(src_formula),)
                    elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete'):
                        src_formula = get_filled_formula_by_col(col, [i, i])
                        if ColumnType == 'SecondMinusFirst':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.POSITIVE_NEGATIVE)
                        elif ColumnType == 'PercentOfComplete':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.PERCENT)
                elif "M" in col["type"]:
                    if ColumnType == 'Selected':
                        cell_val = sheet[f"{col_letter}{i}"]
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx)
                    elif ColumnType == 'PercentOfOutput':
                        cell_val = sheet[f"{col_letter}{i}"]
                        set_calc_cell_val(cell_val, col_letter,
                                          query_res_for_column[SrcKey], col, idx,
                                          ColumnSettings=EnumColumnSettings.FORMULA_POO)
                        # src_formula = get_filled_formula_by_col(col, [i, FirstRowWithBS])
                        # set_value_cell(sheet[f"{col_letter}{i}"],
                        #                formula_translator.convert_russian_formula(src_formula))
                    elif ColumnType in ('SecondMinusFirst', 'PercentOfComplete'):
                        src_formula = get_filled_formula_by_col(col, [i, i])
                        if ColumnType == 'SecondMinusFirst':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.POSITIVE_NEGATIVE)
                        elif ColumnType == 'PercentOfComplete':
                            set_value_cell(sheet[f"{col_letter}{i}"],
                                           formula_translator.convert_russian_formula(src_formula),
                                           EnumCellType.PERCENT)


# ============================================================================================
def get_report_template(id):
    template_name = g_report_template_name
    path_template = str(Path(uf.main_folder) / Path(uf.file_folder) / Path(template_name))

    # Получаем рабочую книгу из шаблона
    wb = openpyxl.load_workbook(path_template)

    factories_all = [row.id for row in uf.get_data_from_query("SELECT id FROM tab_factories_d816_4")]
    for factory_id in factories_all:
        try:
            def_range_bs = wb.defined_names[f'_SET_ROW{factory_id}']
        except Exception as e:
            continue
        for sheet_name_rng_bs, cell_coordinates_rng_bs in def_range_bs.destinations:
            sheet = wb[sheet_name_rng_bs]
            if id != str(factory_id):
                sheet.sheet_state = 'veryHidden'
                continue

            sheet.sheet_state = 'visible'

    # Создаём буфер для наполнения
    buffer = io.BytesIO()

    wb.save(buffer)
    # Откатываем курсор в самое начало
    buffer.seek(0)
    # имя файла
    filename = template_name

    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


# ============================================================================================
def upload_report_template(factory_id: str, file_storage: FileStorage):
    # Проверка, замена левых символов в имени файла
    # safe_filename = secure_filename(file_storage.filename)

    try:
        file_bytes = file_storage.stream.read()
        file_stream = io.BytesIO(file_bytes)
        wb = load_workbook(filename=file_stream, read_only=False)

    except Exception as e:
        return uf.get_msg_struct(uf.EnumMsg.ERROR_OPEN_TEMPLATE, str(e))

    try:
        def prepare_sheets(sheet_list, name_rng):
            def_range_bs_list = []
            def_range_ik_list = []
            for sheet_id in sheet_list:
                try:
                    def_range_bs_list.append(wb.defined_names[f'{name_rng[0]}{sheet_id}'])
                    def_range_ik_list.append(wb.defined_names[f'{name_rng[1]}{sheet_id}'])
                except Exception as e:
                    return uf.get_msg_struct(uf.EnumMsg.ERROR_VALID_NEW_TEMPLATE)
            # ===============================================================================
            for idx, def_range_bs in enumerate(def_range_bs_list):
                def_range_ik = def_range_ik_list[idx]
                sheet = None
                set_row_right_col_index = None
                start_row = None
                start_col = None
                end_col = None
                amount_col = None

                for sheet_name_rng_ik, cell_coordinates_rng_ik in def_range_ik.destinations:
                    _, start_row, _, _ = range_boundaries(cell_coordinates_rng_ik)

                for sheet_name_rng_bs, cell_coordinates_rng_bs in def_range_bs.destinations:
                    _, _, set_row_right_col_index, _ = range_boundaries(cell_coordinates_rng_bs)
                    sheet = wb[sheet_name_rng_bs]

                if sheet is not None and set_row_right_col_index is not None and start_row is not None:
                    start_col = set_row_right_col_index + 2
                    end_col = sheet.max_column + 1
                    amount_col = end_col - start_col

                    # Сброс ширины столбцов (можно удалять для ускорения процесса)
                    # так как кроме визуала ни на что не влияет
                    # и при формировании отчёта всё равно будет выставлена нужная ширина для всех столбцов
                    # если правильно обнаружил, то 8.43 является значением по умолчанию
                    for i in range(start_col, end_col):
                        sheet.column_dimensions[get_column_letter(i)].width = 8.43

                    # Очистка столбцов
                    sheet.delete_cols(idx=start_col, amount=amount_col)

                    # Из-за особенностей openpyxl приходится дополнительно делать очистку по merged ячейкам
                    merged_range = list(sheet.merged_cells.ranges)
                    for m_range in merged_range:
                        merged_start_col, _, merged_end_col, _ = m_range.bounds
                        if merged_start_col <= end_col and merged_end_col >= start_col:
                            try:
                                sheet.merged_cells.remove(m_range)
                            except ValueError:
                                pass
            # ===============================================================================

        factories_all = [row.id for row in uf.get_data_from_query("SELECT id FROM tab_factories_d816_4")]
        reports_all = [1,2]#[row.id for row in get_data_from_query("SELECT id FROM tab_type_reports_d816_4")]

        if factories_all and reports_all:
            prepare_sheets(factories_all, ['_SET_ROW', '_INTERNAL_KEY'])
            prepare_sheets(reports_all, ['_SUM_REP_SET_ROW', '_SUM_REP_INTERNAL_KEY'])
        else:
            return uf.get_msg_struct(uf.EnumMsg.SETTINGS_FOR_REPORT_NOT_FOUND)


        path_template = str(Path(uf.main_folder) / Path(uf.file_folder))

        base_template_dir = os.environ.get("TEMPLATE_DIR", path_template)

        target_filename = f"{g_report_template_name}".lower()
        target_path = os.path.join(base_template_dir, target_filename)

        dir_to_create = os.path.dirname(target_path)
        if not os.path.exists(dir_to_create):
            try:
                os.makedirs(dir_to_create, mode=0o755, exist_ok=True)
            except PermissionError:
                wb.close()
                return uf.get_msg_struct(uf.EnumMsg.ERROR_PERMISSION_CREATE_DIR_LINUX, dir_to_create)

        # Перезапись существующего файла шаблона
        if os.path.exists(target_path) and not os.access(target_path, os.W_OK):
            wb.close()
            return uf.get_msg_struct(uf.EnumMsg.ERROR_PERMISSION_OVERWRITE_FILE)

        # Сохранение файла
        wb.save(target_path)
        wb.close()

        return uf.get_msg_struct(uf.EnumMsg.SUCCESS)

    except Exception as e:
        if 'wb' in locals():
            wb.close()
        return uf.get_msg_struct(uf.EnumMsg.ERROR_SAVE_OR_PROC_TEMPLATE, str(e))
# ============================================================================================
# ============================================================================================