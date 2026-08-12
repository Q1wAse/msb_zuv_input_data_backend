from collections import defaultdict
import datetime

from sqlalchemy import text, inspect

import msb_zuv_input_data_backend.functions.utility_functions as uf

#=======================================================================================================================
# Рассчитать для графика объёмы для года
def get_calc_volume_old(data_slice, product, type_raspr, ei, selected_variant_compare, selected_factories, variant_columns):
    db = uf.get_db_connection()
    period_str = ""
    data_slice_str = ""
    if data_slice == 'year':
        # period_str = "main.month = 0 AND main.quarter = 0 AND"
        period_str = "main.month <> 0 AND"
        data_slice_str = f"main.{data_slice},"
    elif data_slice == 'month':
        period_str = "main.month <> 0 AND"
        data_slice_str = f"main.{data_slice},"
    elif data_slice == 'tab_product_d816_4_ids':
        period_str = "main.month <> 0 AND"
        data_slice_str = f"main.{data_slice},"
    params = {}

    product_list = [int(item) for item in product]
    product_str = ""
    if product_list:
        product_str = "main.tab_product_d816_4_ids = ANY(:product) AND"
        params["product"] = product_list

    type_raspr_list = [int(item) for item in type_raspr]

    factory_list = [int(item) for item in selected_factories]
    var_plans_list = [
        int(item.get("variantPlaning", 0))
        for idx, item in enumerate(variant_columns, start=1)
        if str(idx) in selected_variant_compare
    ]
    years_list = [
        int(item.get("year", 0))
        for idx, item in enumerate(variant_columns, start=1)
        if str(idx) in selected_variant_compare
    ]

    params['type_raspr'] = type_raspr_list
    params['ei'] = ei
    params['factory'] = factory_list
    params['var_plans'] = var_plans_list
    params['years'] = years_list

    col_sql = text(f"""
                        SELECT
                            params.idx as variantColumns,
                            {data_slice_str}
                            sum(main.value)
                            
                        FROM tab_pererabotka_d816_4 main
                        JOIN LATERAL unnest(CAST(:var_plans AS INTEGER[]), CAST(:years AS INTEGER[])) WITH ORDINALITY AS params(var_plan, year, idx)
                          ON main.tab_var_plan_d816_4_ids = params.var_plan 
                          AND main.year = params.year
                        WHERE
                            {product_str}
                            main.tab_type_raspr_d816_4_ids = ANY(:type_raspr) AND
                            {period_str}
                            main.tab_factory_d816_4_ids = ANY(:factory) AND
                            main.tab_ei_d816_4_ids = :ei
                        GROUP BY {data_slice_str}params.idx
                        ORDER BY params.idx;
                    """)
    if var_plans_list and years_list:
        result = db.execute(col_sql, params).fetchall()
        res = []

        for row in result:
            mapping = row._mapping

            new_row = {
                'variantColumns' : row.variantcolumns
            }
            if data_slice in mapping:
                new_row[data_slice] = mapping[data_slice]
            new_row['value'] = float(row.sum)

            res.append(new_row)
        return res
    else:
        return []
#=======================================================================================================================
def removeprefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        result = text[len(prefix):]
    else:
        result = text
    return result
def removesuffix(text: str, suffix: str) -> str:
    if suffix and text.endswith(suffix):
        return text[:-len(suffix)]
    return text

def get_tab_name_check(name):
    db = uf.get_db_connection()
    name_list = []
    if db:
        inspector = inspect(db.get_bind())
        if inspector:
            name_list.append(removesuffix(name,'_ids'))
            name_list.append(f"tab_view_{removeprefix(removesuffix(name,'_ids'),'tab_')}")
            for tab_name in name_list:
                if inspector.has_table(tab_name):
                    return tab_name
    return ''
#=======================================================================================================================
def convert_data_to_tab_front_old(result, key_name, reverse_diff=False):
    db = uf.get_db_connection()
    final_res = []
    key_list = []
    dict_key_name = []
    res_list = []
    for res in result:
        key = res.get(key_name, None)
        if key:
            if key not in key_list:
                key_list.append(key)
    tab_name = get_tab_name_check(key_name)
    if key_list and tab_name:
        col_sql = text(f"""
            SELECT
                id,
                name
            FROM {tab_name}
            WHERE
                id = ANY(:key)
        """)
        query_result = db.execute(col_sql,
          {
              'key' : key_list
          }
        ).fetchall()
        for row in query_result:
            dict_key_name.append(
                {
                    'id' : row.id,
                    'name' : row.name
                }
            )
        grouped = {}
        for item in result:
            key_name_value = item.get(key_name, "all")
            # if key_name_value is None:
            #     continue
            if key_name_value not in grouped:
                grouped[key_name_value] = {}
                if key_name in item:
                    grouped[key_name_value][key_name] = key_name_value

            var_num = item.get("variantColumns")

            if var_num == 0:
                new_add_param = "deviation"
            elif var_num == -1:
                new_add_param = "percents"
            else:
                new_add_param = f"variant{var_num}"

            grouped[key_name_value][new_add_param] = item["value"]

        processed_list = list(grouped.values())

        for slice_id, values in grouped.items():
            # Сначала всегда идет ключ среза (например, 'month' или 'tab_product_d816_4_ids')
            ordered_row = {}
            if slice_id != "all":
                ordered_row['name'] = next((item['name'] for item in dict_key_name if item.get('id') == slice_id), None)
                # ordered_row[key_name] = slice_id

            ordered_row['variant1'] = values.get('variant1', 0.0)
            ordered_row['variant2'] = values.get('variant2', 0.0)
            ordered_row['deviation'] = values.get('deviation', 0.0)
            ordered_row['percents'] = values.get('percents', 0.0)

            final_res.append(ordered_row)
        dict_total = {
            'sum' : True,
            'variant1' : 0.0,
            'variant2' : 0.0,
            'deviation' : 0.0,
            'percents' : 0.0,
        }
        for res in final_res:
            dict_total['variant1'] += res.get('variant1', 0.0)
            dict_total['variant2'] += res.get('variant2', 0.0)
        #==============================================================
        v1_val = dict_total['variant1']
        v2_val = dict_total['variant2']

        if reverse_diff:
            diff_value = round(v2_val - v1_val, 2)
            base_val = v2_val
        else:
            diff_value = round(v1_val - v2_val, 2)
            base_val = v1_val

        if base_val != 0:
            pct_value = round((diff_value / base_val) * 100, 2)
        else:
            pct_value = 0.0

        dict_total['deviation'] = diff_value
        dict_total['percents'] = pct_value
        final_res.append(dict_total)
        #==============================================================
    return final_res
#=======================================================================================================================
def get_list_percent_from_lists(list_base,list_slice):
    result = []
    if list_base and list_slice and len(list_base) == len(list_slice):
        for i in range(0, len(list_base)):
            con1 = (list_base[i].get('month', None) != None and
                    list_base[i].get('variant1', None) != None and
                    list_base[i].get('variant2', None) != None )
            con2 = (list_slice[i].get('month', None) != None and
                    list_slice[i].get('variant1', None) != None and
                    list_slice[i].get('variant2', None) != None)
            if con1 and con2:
                v1_val1 = list_base[i].get('variant1', 0.0)
                v1_val2 = list_slice[i].get('variant1', 0.0)
                v2_val1 = list_base[i].get('variant2', 0.0)
                v2_val2 = list_slice[i].get('variant2', 0.0)
                result.append({
                    'month' : list_base[i].get('month', 0),
                    'variant1' : round( v1_val2 / v1_val1 * 100, 1)  if v1_val1 != 0 else 0.0,
                    'variant2' : round( v2_val2 / v2_val1 * 100, 1)  if v2_val1 != 0 else 0.0
                })
    return result
#=======================================================================================================================
def convert_data_to_tab_front(result, key_name, reverse_diff=False):
    db = uf.get_db_connection()
    final_res = []
    key_list = []
    key_name_list = []
    res_list = []
    for res in result:
        key = res.get(key_name, None)
        if key:
            if key not in key_list:
                key_list.append(key)
    tab_name = get_tab_name_check(key_name)
    if key_list and tab_name:
        col_sql = text(f"""
                SELECT
                    id,
                    name
                FROM {tab_name}
                WHERE
                    id = ANY(:key)
            """)
        query_result = db.execute(col_sql,
                                  {
                                      'key': key_list
                                  }
                                  ).fetchall()
        for row in query_result:
            key_name_list.append(
                {
                    'id': row.id,
                    'name': row.name
                }
            )
        for res in result:
            final_res.append({
                'name' : next((item for item in key_name_list if item.get('id') == res.get(key_name, '')), {}).get('name', ''),
                'variant1' : res.get('variant1', 0.0),
                'variant2' : res.get('variant2', 0.0),
                'deviation' : res.get('deviation', 0.0),
                'percents' : res.get('percents', 0.0),
            })

        dict_total = {
            'sum': True,
            'variant1': 0.0,
            'variant2': 0.0,
            'deviation': 0.0,
            'percents': 0.0,
        }
        for res in final_res:
            dict_total['variant1'] += res.get('variant1', 0.0)
            dict_total['variant2'] += res.get('variant2', 0.0)
        # ==============================================================
        v1_val = dict_total['variant1']
        v2_val = dict_total['variant2']

        if reverse_diff:
            diff_value = round(v2_val - v1_val, 1)
            base_val = v2_val
        else:
            diff_value = round(v1_val - v2_val, 1)
            base_val = v1_val

        if base_val != 0:
            pct_value = round((diff_value / base_val) * 100, 1)
        else:
            pct_value = 0.0

        dict_total['variant1'] = round(dict_total['variant1'], 1)
        dict_total['variant2'] = round(dict_total['variant2'], 1)
        dict_total['deviation'] = diff_value
        dict_total['percents'] = pct_value
        final_res.append(dict_total)
        # ==============================================================
    final_res.sort(key=lambda row: (1 if 'name' not in row else 0, row.get('name', '')))
    return final_res
#=======================================================================================================================

def get_calc_volume(
        data_slice,
        product,
        type_raspr,
        filters,
        selected_variant_compare,
        selected_factories,
        variant_columns,
        ei=None,
        reverse_diff=False
):
    mapping_col = {
        'product': 'tab_product_d816_4_ids',
        'sobstv': 'tab_sobstv_d816_4_ids',
        'mest': 'tab_mest_d816_4_ids',
        'post_zuv': 'tab_post_zuv_d816_4_ids',
        'ei': 'tab_ei_d816_4_ids'
    }

    db = uf.get_db_connection()
    query_params = {}
    period_str = ""
    data_slice_str = ""
    group_by_str = ""
    ei_str = ""
    filter_str = ""

    if data_slice == 'year':
        period_str = "main.month <> 0 AND"
        data_slice_str = f"string_agg(DISTINCT params.year::text, ',' ORDER BY params.year::text) AS {data_slice},"
    elif data_slice == 'month':
        period_str = "main.month <> 0 AND"
        data_slice_str = f"main.{data_slice}::text,"
        group_by_str = f"GROUP BY {data_slice_str.replace(',','')}"
    elif data_slice == 'tab_product_d816_4_ids':
        period_str = "main.month <> 0 AND"
        data_slice_str = f"main.{data_slice},"
        group_by_str = f"GROUP BY {data_slice_str.replace(',', '')}"

    if ei != None:
        ei_str = "main.tab_ei_d816_4_ids = :ei AND"

    if filters:
        for idx, (key, value) in enumerate(filters.items()):
            if value:
                need_key = mapping_col.get(key, None)
                if need_key == None:
                    continue
                filter_str = f'{filter_str}main.{need_key} = ANY(:flt{idx}) AND '
                filter_list = []
                for flt in value:
                    filter_list.append(flt)
                query_params[f'flt{idx}'] = filter_list

    product_list = [int(item) for item in product]
    product_str = ""
    if product_list:
        product_str = "main.tab_product_d816_4_ids = ANY(:product) AND"
        query_params["product"] = product_list

    type_raspr_list = [int(item) for item in type_raspr]

    factory_list = [int(item) for item in selected_factories]
    var_plans_list = [
        int(item.get("variantPlaning", 0))
        for idx, item in enumerate(variant_columns, start=1)
        if str(idx) in selected_variant_compare
    ]
    years_list = [
        int(item.get("year", 0))
        for idx, item in enumerate(variant_columns, start=1)
        if str(idx) in selected_variant_compare
    ]

    query_params['type_raspr'] = type_raspr_list
    if ei != None:
        query_params['ei'] = ei
    query_params['factory'] = factory_list
    query_params['var_plans'] = var_plans_list
    query_params['years'] = years_list

    # Запрос с распределением в столбец
    # col_sql = text(f"""
    #     SELECT
    #         params.idx as variantColumns,
    #         {data_slice_str}
    #         sum(main.value)
    #
    #     FROM tab_pererabotka_d816_4 main
    #     JOIN LATERAL unnest(CAST(:var_plans AS INTEGER[]), CAST(:years AS INTEGER[])) WITH ORDINALITY AS params(var_plan, year, idx)
    #       ON main.tab_var_plan_d816_4_ids = params.var_plan
    #       AND main.year = params.year
    #     WHERE
    #         {filter_str}
    #         {product_str}
    #         main.tab_type_raspr_d816_4_ids = ANY(:type_raspr) AND
    #         {period_str}
    #         main.tab_factory_d816_4_ids = ANY(:factory) AND
    #         main.tab_ei_d816_4_ids = :ei
    #     GROUP BY {data_slice_str}params.idx
    #     ORDER BY params.idx
    # """)

    # Запрос с распределением в строку
    col_sql = text(f"""
        SELECT
            {data_slice_str}
            COALESCE(SUM(main.value) FILTER (WHERE params.idx = 1), 0.0) AS variant1,
            COALESCE(SUM(main.value) FILTER (WHERE params.idx = 2), 0.0) AS variant2

        FROM tab_pererabotka_d816_4 as main
            JOIN LATERAL unnest(CAST(:var_plans AS INTEGER[]), CAST(:years AS INTEGER[])) WITH ORDINALITY AS params(var_plan, year, idx)
            ON main.tab_var_plan_d816_4_ids = params.var_plan 
            AND main.year = params.year
        WHERE
            {filter_str}
            {product_str}
            main.tab_type_raspr_d816_4_ids = ANY(:type_raspr) AND
            {period_str}
            {ei_str}
            main.tab_factory_d816_4_ids = ANY(:factory)
        {group_by_str}
    """)
    if var_plans_list and years_list:
        result = db.execute(col_sql, query_params).fetchall()
        res = []

        if result:
            for row in result:
                mapping = row._mapping
                v1_val = float(row.variant1)
                v2_val = float(row.variant2)
                if reverse_diff:
                    diff_value = round(v2_val - v1_val, 1)
                    base_val = v2_val
                else:
                    diff_value = round(v1_val - v2_val, 1)
                    base_val = v1_val
                res.append({
                    data_slice : mapping.get(data_slice, None),
                    'variant1' : round(v1_val, 1),
                    'variant2' : round(v2_val, 1),
                    'deviation' : round(diff_value,1),
                    'percents' : round((diff_value / base_val) * 100, 1) if base_val != 0 else 0.0,
                })
            # Насыщаем коллекцию недостающими месяцами, если такие есть
            if data_slice == 'month':
                for i in range(0,12):
                    try:
                        cur_month = res[i].get(data_slice)
                    except IndexError:
                        res.append({
                            data_slice: str(i+1),
                            'variant1': 0.0,
                            'variant2': 0.0,
                            'deviation': 0.0,
                            'percents': 0.0,
                        })
        return res
    else:
        return []
#=======================================================================================================================
def get_exist_factories(tab_id):
    db = uf.get_db_connection()
    res = []
    tab_name = ''
    fields_list = []
    as_name_factory = 'factory'
    if tab_id:
        tab_map = uf.TABLES_MAP.get(tab_id, None)
        if tab_map:
            tab_name = tab_map.get('tab_name', '')
            tab_fields = tab_map.get('fields', '')
            fields_list = [f.strip() for f in tab_fields.split(',')]
            fields = ', '.join([f'{as_name_factory}.{f.strip()}' for f in tab_fields.split(',')])
    if tab_name and fields_list and len(fields_list) == 2:
        sql_text = text(f"""
            SELECT
                {fields}
            FROM
                tab_pererabotka_d816_4 as pererab
            JOIN
                {tab_name} as {as_name_factory}
            ON
                pererab.tab_factory_d816_4_ids =
                {as_name_factory}.id
            WHERE
                pererab.tab_type_raspr_d816_4_ids IN (5,7)
            GROUP BY
                {fields}
            ORDER BY 
                {fields}
        """)
        result = db.execute(sql_text).fetchall()
        if result:
            for row in result:
                new_row = {}
                mapping = row._mapping
                for f in fields_list:
                    if mapping.get(f, None):
                        new_row[f] = mapping[f]
                if new_row:
                    res.append(new_row)
    return res
#=======================================================================================================================
def get_def_val_from_list(def_numb,fields_src_list,src,value_list):
    for item in fields_src_list:
        name = item.get('name')
        if name == src:
            value = item.get(def_numb,None)
            if value:
                for val in value_list:
                    id = val.get('id', None)
                    if id == value:
                        return value
    return None
#=======================================================================================================================
def get_exist_factory_collect(factory_id):
    db = uf.get_db_connection()
    res = {}
    fields_src_list =[
        {
            'name': 'product',
            'default1' : 31,
            'default2' : 56,
        },
        {
            'name': 'sobstv',
            'default1' : None,
            'default2' : None,
        },
        {
            'name': 'mest',
            'default1' : None,
            'default2' : None,
        },
        {
            'name': 'post_zuv',
            'default1' : None,
            'default2' : None,
        },
        {
            'name': 'ei',
            'default1' : 1,
            'default2' : 1,
        },
    ]
    fields_list = [
        item
        for field in fields_src_list
        for item in (f"{field.get('name','')}_id", f"{field.get('name','')}_name")
    ]
    fields_list.insert(0,'type_raspr')
    fields_str = """
        pererab.tab_type_raspr_d816_4_ids as {},
        product.id as {},
        product.name as {},
        sobstv.id as {},
        sobstv.name as {},
        mest.id as {},
        mest.name as {},
        post_zuv.id as {},
        post_zuv.name as {},
        ei.id as {},
        ei.name as {}
    """
    fields_clr_str = fields_str.replace(' as {}','')
    fields_str = fields_str.format(*fields_list)

    sql_text = text(f"""
        SELECT
            {fields_str}
        FROM
            tab_pererabotka_d816_4 as pererab
        LEFT JOIN tab_factory_d816_4 as factory ON pererab.tab_factory_d816_4_ids = factory.id
        LEFT JOIN tab_view_product_d816_4 as product ON pererab.tab_product_d816_4_ids = product.id
        LEFT JOIN tab_sobstv_d816_4 as sobstv ON pererab.tab_sobstv_d816_4_ids = sobstv.id
        LEFT JOIN tab_mest_d816_4 as mest ON pererab.tab_mest_d816_4_ids = mest.id
        LEFT JOIN tab_post_zuv_d816_4 as post_zuv ON pererab.tab_post_zuv_d816_4_ids = post_zuv.id
        LEFT JOIN tab_ei_d816_4 as ei ON pererab.tab_ei_d816_4_ids = ei.id
        WHERE
            pererab.tab_type_raspr_d816_4_ids IN (5,7) AND
            pererab.tab_factory_d816_4_ids = {int(factory_id)}
        GROUP BY
            {fields_clr_str}
        ORDER BY 
            {fields_clr_str}
    """)

    # sql_text = text(f"""
    #     SELECT
    #         {fields_str}
    #     FROM
    #         tab_pererabotka_d816_4 as pererab
    #     JOIN
    #         tab_factory_d816_4 as factory
    #     ON
    #         pererab.tab_factory_d816_4_ids =
    #         factory.id
    #     WHERE
    #         pererab.tab_type_raspr_d816_4_ids IN (5,7) AND
    #         pererab.tab_factory_d816_4_ids = {int(factory_id)}
    #     GROUP BY
    #         {fields_clr_str}
    #     ORDER BY
    #         {fields_clr_str}
    # """)
    result = db.execute(sql_text).fetchall()
    if result:
        uniq_dict_frame1 = defaultdict(lambda: defaultdict(int)) # defaultdict(list)
        uniq_dict_frame2 = defaultdict(lambda: defaultdict(int)) # defaultdict(list)
        for value in fields_src_list:
            src = value.get('name','')
            if src:
                uniq_dict_frame1[src]['default'] = None
                uniq_dict_frame1[src]['value'] = []
                uniq_dict_frame2[src]['default'] = None
                uniq_dict_frame2[src]['value'] = []
        for row in result:
            mapping = row._mapping
            type_raspr = mapping.get('type_raspr', None)
            row_data = defaultdict(dict)
            for db_key, db_value in mapping.items():
                if db_key != 'type_raspr':
                    for value in fields_src_list:
                        src = value.get('name','')
                        if src in db_key:
                            suffix = db_key.rsplit('_', 1)[-1]

                            if suffix == 'id':
                                row_data[src]['id'] = db_value
                            elif suffix == 'name':
                                row_data[src]['name'] = db_value
                            break

            for src, item_dict in row_data.items():
                if 'id' in item_dict and 'name' in item_dict and item_dict.get('id', 0) != 0:
                    if type_raspr == 5: # Переработка
                        if item_dict not in uniq_dict_frame1[src]['value']:
                            uniq_dict_frame1[src]['value'].append(item_dict)
                    elif type_raspr == 7: # Производство
                        if item_dict not in uniq_dict_frame2[src]['value']:
                            uniq_dict_frame2[src]['value'].append(item_dict)
        for value in fields_src_list:
            src = value.get('name', '')
            if src:
                uniq_dict_frame1[src]['value'].sort(key=lambda row: row.get('name',''))
                uniq_dict_frame1[src]['default'] = get_def_val_from_list(
                    'default1',
                    fields_src_list,
                    src,
                    uniq_dict_frame1[src]['value']
                )
                uniq_dict_frame2[src]['value'].sort(key=lambda row: row.get('name',''))
                uniq_dict_frame2[src]['default'] = get_def_val_from_list(
                    'default2',
                    fields_src_list,
                    src,
                    uniq_dict_frame2[src]['value']
                )
        res = {
             'panel_middle_month_volume_frame1_filter' : uniq_dict_frame1,
             'panel_middle_month_volume_frame2_filter' : uniq_dict_frame2
        }
    return res
#=======================================================================================================================
def get_calculated_dataset(selected_variant_compare,
                           selected_factories,
                           v_filters_middle_volume_frame1,
                           v_filters_middle_volume_frame2,
                           variant_columns):
    if v_filters_middle_volume_frame1 or v_filters_middle_volume_frame2:
        collection = {
            'panel_middle_month_volume_frame1': get_calc_volume(
                'month',
                [],  # Газ
                [5],  # Переработка
                v_filters_middle_volume_frame1,
                selected_variant_compare,
                selected_factories,
                variant_columns),
            'panel_middle_month_volume_frame2': get_calc_volume(
                'month',
                [],  # Газ
                [7],  # Производство
                v_filters_middle_volume_frame2,
                selected_variant_compare,
                selected_factories,
                variant_columns),
        }
    else:
        collection  = {
            'panel_upper_year_volume_frame1' : get_calc_volume(
                'year',
                [64],  # Газ
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=2, ),  # мл. м3 (Единица измерения)
            'panel_upper_year_volume_frame2': get_calc_volume(
                'year',
                [67],  # Нестабильный конденсат
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei = 1, ),  # тыс тонн (Единица измерения)
            'panel_upper_year_volume_frame3': get_calc_volume(
                'year',
                [],  # пусто
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            'panel_upper_year_volume_frame4': get_calc_volume(
                'year',
                [],  # Пусто
                [7],  # Производство
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            'panel_upper_month_volume_graph1': get_list_percent_from_lists(
                get_calc_volume(
                    'month',
                    [],  #
                    [5],  # Переработка
                    {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=1, ),  # тыс тонн (Единица измерения)
                get_calc_volume(
                    'month',
                    [64],  # Газ
                    [5],  # Переработка
                    {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=1, ),  # тыс тонн (Единица измерения)
            ),
            'panel_middle_month_volume_frame1': get_calc_volume(
                'month',
                [],  #
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            'panel_middle_month_volume_frame2': get_calc_volume(
                'month',
                [],  # Газ
                [7],  # Производство
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            'panel_lower_month_volume_tab1': {
                'ton' :
                    convert_data_to_tab_front(get_calc_volume(
                        'tab_product_d816_4_ids',
                        [],  # Газ
                        [7],  # Производство
                        {},
                        selected_variant_compare,
                        selected_factories,
                        variant_columns,
                        ei=1, ), 'tab_product_d816_4_ids'),
                'cube':
                    convert_data_to_tab_front(get_calc_volume(
                        'tab_product_d816_4_ids',
                        [],  # Газ
                        [7],  # Производство
                        {},
                        selected_variant_compare,
                        selected_factories,
                        variant_columns,
                        ei=2, ), 'tab_product_d816_4_ids'),
            },
            'panel_lower_month_volume_tab2': {
                'ton' : convert_data_to_tab_front(get_calc_volume(
                    'tab_product_d816_4_ids',
                    [],  # Газ
                    [5],  # Переработка
                    {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=1, ), 'tab_product_d816_4_ids'),
                'cube': convert_data_to_tab_front(get_calc_volume(
                    'tab_product_d816_4_ids',
                    [],  # Газ
                    [5],  # Переработка
                    {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=2, ), 'tab_product_d816_4_ids'),
            }
        }
        if len(selected_factories) == 1:
            collection.update(get_exist_factory_collect(selected_factories[0]))
    return collection
#=======================================================================================================================