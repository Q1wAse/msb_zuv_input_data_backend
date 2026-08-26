from collections import defaultdict
import datetime
import re
import ast
import operator

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
#=================== Список категорий продукта ========================================================================================
def get_product_categories():
    db = uf.get_db_connection()

    sql = text("""
        SELECT
            category.id,
            category.name,
            category.ord,
            COALESCE(
                ARRAY_AGG(product.id ORDER BY product.name)
                    FILTER (WHERE product.id IS NOT NULL),
                ARRAY[]::INTEGER[]
            ) AS product
        FROM tab_category_product_d816_4 AS category
        LEFT JOIN tab_view_product_d816_4 AS product
            ON product.group_nom_real = category.id
        GROUP BY
            category.id,
            category.name,
            category.ord
        ORDER BY category.ord
    """)

    result = db.execute(sql).fetchall()

    return [
        {
            'id': row.id,
            'name': row.name,
            'ord': row.ord,
            'product': list(row.product or [])
        }
        for row in result
    ]
#==============Продукты из выбранной категории продукта ================================================================
def get_products_by_category(category_id):
    db = uf.get_db_connection()
    sql = text("""
        SELECT
            id,
            name,
            group_nom_real
        FROM tab_view_product_d816_4
        WHERE group_nom_real = :category_id
        ORDER BY name
    """)
    result = db.execute(
        sql,
        {
            'category_id': int(category_id)
        }
    ).fetchall()
    return [
        {
            'id': row.id,
            'name': row.name,
            'cat_product': row.group_nom_real
        }
        for row in result
    ]
#=======================================================================================================================
def convert_data_to_tab_front_old(result, key_name, reverse_diff=True):
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
# =======================================================================================================================
# РАСЧЁТ КОЭФФИЦИЕНТА ВЫХОДА по формуле, которая хранится в tab_formula_koef_d816_4
# =======================================================================================================================
BS_COLUMN = "tab_bud_st_d816_4_ids" # id статей
#=============== Преобразование value во float ============================
def _safe_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
#=============== Формулы из tab_formula_koef_d816_4 =======================
def _get_formula_rows(
        factory_id,
        category_product_id=None,
        product_id=None,
        ei_id=None
):
    """
    Приоритет:
        1. Формула конкретного продукта.
        2. Формула категории продукта.
    Фильтр по заводу и единице измерения.
    """
    db = uf.get_db_connection()
    if not db:
        return []
    where_parts = [
        "factory_id = :factory_id"
    ]
    params = {
        "factory_id": int(factory_id)
    }
    if ei_id is not None:
        where_parts.append("(ei = :ei_id OR ei IS NULL)")
        params["ei_id"] = int(ei_id)

    product_condition = ""
    if product_id is not None:
        product_condition = """
            (
                product_id = :product_id
                OR
                (
                    product_id IS NULL
                    AND category_product_id = :category_product_id
                )
            )
        """
        params["product_id"] = int(product_id)
        params["category_product_id"] = (
            int(category_product_id)
            if category_product_id is not None
            else -1
        )

    elif category_product_id is not None:
        product_condition = """
            product_id IS NULL
            AND category_product_id = :category_product_id
        """

        params["category_product_id"] = int(category_product_id)

    else:
        return []

    where_parts.append(product_condition)

    sql = text(f"""
        SELECT
            id,
            factory_id,
            product_id,
            category_product_id,
            ei,
            formula
        FROM tab_formula_koef_d816_4
        WHERE
            {' AND '.join(where_parts)}
        ORDER BY
            CASE
                WHEN product_id IS NOT NULL THEN 0
                ELSE 1
            END,
            id
    """)

    return db.execute(sql, params).fetchall()

#============= Получить бюджетные статьи из tab_map_bs_product_d816_4 ============
def _get_mapping_budget_articles(
        factory_id,
        category_product_id=None,
        product_id=None,
        ei_id=None
):
    """
    Обязательно:
        type_raspr = 7
    При наличии product_id сначала ищем соответствие конкретному продукту.
    Если продукт не найден — используем соответствие категории.
    """
    db = uf.get_db_connection()
    if not db:
        return []

    params = {
        "factory_id": int(factory_id),
        "type_raspr": 7
    }

    where_parts = [
        "factory = :factory_id",
        "type_raspr = :type_raspr"
    ]

    if ei_id is not None:
        where_parts.append("ei_id = :ei_id")
        params["ei_id"] = int(ei_id)

    if product_id is not None:
        product_condition = "id_product = :product_id"
        params["product_id"] = int(product_id)

        if category_product_id is not None:
            product_condition = """
                (
                    id_product = :product_id
                    OR
                    (
                        id_product IS NULL
                        AND category_product_id = :category_product_id
                    )
                )
            """
            params["category_product_id"] = int(category_product_id)

        where_parts.append(product_condition)

    elif category_product_id is not None:
        where_parts.append(
            """
            (
                id_product IS NULL
                AND category_product_id = :category_product_id
            )
            """
        )
        params["category_product_id"] = int(category_product_id)

    else:
        return []

    sql = text(f"""
        SELECT DISTINCT
            id,
            id_product,
            category_product_id,
            factory,
            ei_id,
            type_raspr
        FROM tab_map_bs_product_d816_4
        WHERE
            {' AND '.join(where_parts)}
        ORDER BY id
    """)

    return db.execute(sql, params).fetchall()
# ===================== Выбрать формулу согласно правилам: ====================
def _get_formula_for_selection(
        factory_id,
        category_product_id=None,
        product_id=None,
        ei_id=None
):
    """
    Правила:
    1. Категория + все продукты:
       product_id IS NULL + category_product_id
    2. Категория + один продукт:
       если для продукта существует одна бюджетная статья,
       используется формула продукта.
    3. Категория + один продукт + несколько бюджетных статей:
       используется формула категории.
    """

    formula_rows = _get_formula_rows(
        factory_id=factory_id,
        category_product_id=category_product_id,
        product_id=product_id,
        ei_id=ei_id
    )
    if not formula_rows:
        return None
    # --------------------------------------------------------------------------------
    # Если пользователь выбрал категорию продукта, но не выбрал конкретный продукт -> используем формулу категории.
    # --------------------------------------------------------------------------------
    if product_id is None:

        for row in formula_rows:
            if (
                row.product_id is None
                and
                category_product_id is not None
                and
                row.category_product_id == int(category_product_id)
            ):
                return row

        return None

    # --------------------------------------------------------------------------------
    # Выбран конкретный продукт ->определить количество бюджетных статей.
    # --------------------------------------------------------------------------------
    mapping_rows = _get_mapping_budget_articles(
        factory_id=factory_id,
        category_product_id=category_product_id,
        product_id=product_id,
        ei_id=ei_id
    )
    # Оставляем только соответствия выбранному продукту.
    product_mapping_rows = [
        row
        for row in mapping_rows
        if row.id_product is not None
        and int(row.id_product) == int(product_id)
    ]

    # Если для продукта одна статья — формула продукта.
    if len(product_mapping_rows) == 1:
        for row in formula_rows:
            if (
                row.product_id is not None
                and int(row.product_id) == int(product_id)
            ):
                return row
    # Если статей несколько — формула категории.
    for row in formula_rows:
        if (
            row.product_id is None
            and
            category_product_id is not None
            and
            row.category_product_id == int(category_product_id)
        ):
            return row
    return None

#Получить значение конкретной бюджетной статьи из tab_pererabotka_d816_4

def _get_budget_article_value(
        factory_id,
        budget_article_id,
        ei_id,
        variant_plan,
        year,
        month
):
    db = uf.get_db_connection()

    if not db:
        return 0.0

    sql = text(f"""
        SELECT
            COALESCE(SUM(main.value), 0.0) AS value
        FROM tab_pererabotka_d816_4 AS main
        WHERE
            main.{BS_COLUMN} = :budget_article_id
            AND main.tab_type_raspr_d816_4_ids = 7
            AND main.tab_factory_d816_4_ids = :factory_id
            AND main.tab_ei_d816_4_ids = :ei_id
            AND main.tab_var_plan_d816_4_ids = :variant_plan
            AND main.year = :year
            AND main.month = :month
    """)

    result = db.execute(
        sql,
        {
            "budget_article_id": int(budget_article_id),
            "factory_id": int(factory_id),
            "ei_id": int(ei_id),
            "variant_plan": int(variant_plan),
            "year": int(year),
            "month": int(month)
        }
    ).fetchone()

    if not result:
        return 0.0

    return _safe_float(result.value)
#    ============  Заменить ссылки на бюджетные статьи: {100331992:-1} на фактические значения из БД ================
#    $100 = обычное число 100
def _replace_formula_budget_references(
        formula,
        factory_id,
        ei_id,
        variant_plan,
        year,
        month,
        coefficient_cache=None,
        recursion_stack=None
):
    if coefficient_cache is None:
        coefficient_cache = {}
    if not formula:
        return "0"
    formula_text = str(formula).strip()
    # --------------------------------------------------------------------------------
    # {100331992:-1}, а значение бюджетной статьи берём из БД.
    # --------------------------------------------------------------------------------
    def replace_braced(match):
        budget_article_id = int(match.group(1))

        cache_key = (
            "value",
            budget_article_id,
            factory_id,
            ei_id,
            variant_plan,
            year,
            month
        )

        if cache_key in coefficient_cache:
            value = coefficient_cache[cache_key]
        else:
            value = _get_budget_article_value(
                factory_id=factory_id,
                budget_article_id=budget_article_id,
                ei_id=ei_id,
                variant_plan=variant_plan,
                year=year,
                month=month
            )

            coefficient_cache[cache_key] = value

        return str(value)
    formula_text = re.sub(
        r"\{(\d+):-1\}",
        replace_braced,
        formula_text
    )
    # --------------------------------------------------------------------------------
    # $100 -> 100  $6   -> 6
    # --------------------------------------------------------------------------------
    formula_text = re.sub(
        r"\$(\d+(?:\.\d+)?)",
        r"\1",
        formula_text
    )

    return formula_text
# Вычисление арифметического выражения
def _safe_eval_arithmetic(expression):
    """
    Поддерживаются:
        +
        -
        *
        /
        ()
        числа
    """
    expression = expression.replace(",", ".")

    tree = ast.parse(expression, mode="eval")

    allowed_binary_operations = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv
    }

    allowed_unary_operations = {
        ast.UAdd: operator.pos,
        ast.USub: operator.neg
    }

    def evaluate(node):

        if isinstance(node, ast.Expression):
            return evaluate(node.body)

        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return float(node.value)

            raise ValueError(
                f"Недопустимое значение в формуле: {node.value}"
            )

        if isinstance(node, ast.BinOp):
            operation = allowed_binary_operations.get(type(node.op))

            if operation is None:
                raise ValueError(
                    f"Недопустимая операция: {type(node.op).__name__}"
                )

            left = evaluate(node.left)
            right = evaluate(node.right)

            if isinstance(node.op, ast.Div) and right == 0:
                return 0.0

            return operation(left, right)

        if isinstance(node, ast.UnaryOp):
            operation = allowed_unary_operations.get(type(node.op))

            if operation is None:
                raise ValueError(
                    f"Недопустимая унарная операция: "
                    f"{type(node.op).__name__}"
                )

            return operation(evaluate(node.operand))

        raise ValueError(
            f"Недопустимый элемент формулы: "
            f"{type(node).__name__}"
        )

    return evaluate(tree)
# ===============Привести формулу из tab_formula_koef_d816_4 к арифметическому виду Python
def _normalize_formula(formula):
    """
    Например:
    ОКРУГЛ(
        {100331992:-1}/{100335806:-1}*$100;
        $6
    )
    превратить в:
        round(<expression>, 6)
    """
    formula_text = str(formula).strip()

    # Убираем ОКРУГЛ( ... ; ... )
    round_match = re.match(
        r"^\s*ОКРУГЛ\s*\((.*)\)\s*$",
        formula_text,
        flags=re.IGNORECASE
    )

    if round_match:

        inner = round_match.group(1)

        # В формуле разделитель аргументов ;
        parts = inner.rsplit(";", 1)

        if len(parts) == 2:

            expression = parts[0].strip()
            precision_text = parts[1].strip()

            # В формулах используется $6 как количество знаков.
            precision_text = precision_text.replace("$", "")

            try:
                precision = int(precision_text)
            except ValueError:
                precision = 6

            return expression, precision

    return formula_text, None

def _calculate_formula(
        formula,
        factory_id,
        ei_id,
        variant_plan,
        year,
        month,
        coefficient_cache=None
):
# ===========  Рассчитать одну формулу tab_formula_koef_d816_4.
    if coefficient_cache is None:
        coefficient_cache = {}

    expression, precision = _normalize_formula(formula)

    expression = _replace_formula_budget_references(
        formula=expression,
        factory_id=factory_id,
        ei_id=ei_id,
        variant_plan=variant_plan,
        year=year,
        month=month,
        coefficient_cache=coefficient_cache
    )

    try:
        value = _safe_eval_arithmetic(expression)
    except Exception:
        return 0.0

    if precision is not None:
        value = round(value, precision)

    return value

# Рассчитать коэффициент выхода бюджетной статьи. Используется для обработки ссылок вида $100
def _calculate_budget_article_coefficient(
        budget_article_id,
        factory_id,
        ei_id,
        variant_plan,
        year,
        month,
        coefficient_cache,
        recursion_stack=None
):
    if recursion_stack is None:
        recursion_stack = set()
    db = uf.get_db_connection()
    if not db:
        return 0.0
    # --------------------------------------------------------------------------------
    # Ищем формулу для бюджетной статьи в tab_formula_koef_d816_4, где id = бюджетная статья.
    # --------------------------------------------------------------------------------
    sql = text("""
        SELECT
            id,
            factory_id,
            product_id,
            category_product_id,
            ei,
            formula
        FROM tab_formula_koef_d816_4
        WHERE
            id = :budget_article_id
            AND factory_id = :factory_id
            AND (ei = :ei_id OR ei IS NULL)
        ORDER BY
            CASE
                WHEN ei = :ei_id THEN 0
                ELSE 1
            END,
            CASE
                WHEN product_id IS NOT NULL THEN 0
                ELSE 1
            END,
            id
        LIMIT 1
    """)

    row = db.execute(
        sql,
        {
            "budget_article_id": int(budget_article_id),
            "factory_id": int(factory_id),
            "ei_id": int(ei_id)
        }
    ).fetchone()

    # --------------------------------------------------------------------------------
    # Если формулы для этой статьи нет, используем значение статьи.
    # --------------------------------------------------------------------------------
    if not row or not row.formula:
        return _get_budget_article_value(
            factory_id=factory_id,
            budget_article_id=budget_article_id,
            ei_id=ei_id,
            variant_plan=variant_plan,
            year=year,
            month=month
        )

    return _calculate_formula(
        formula=row.formula,
        factory_id=factory_id,
        ei_id=ei_id,
        variant_plan=variant_plan,
        year=year,
        month=month,
        coefficient_cache=coefficient_cache
    )

# Рассчитать коэффициент выхода для конкретного завода, варианта, года и месяца.
def _calculate_output_coefficient(
        formula_row,
        factory_id,
        ei_id,
        variant_plan,
        year,
        month
):
    if not formula_row:
        return 0.0

    coefficient_cache = {}

    return _calculate_formula(
        formula=formula_row.formula,
        factory_id=factory_id,
        ei_id=ei_id,
        variant_plan=variant_plan,
        year=year,
        month=month,
        coefficient_cache=coefficient_cache
    )

# Получить category_product_id для продукта.
def _get_category_for_product(product_id):
    if product_id is None:
        return None
    db = uf.get_db_connection()
    if not db:
        return None
    sql = text("""
        SELECT
            group_nom_real
        FROM tab_view_product_d816_4
        WHERE id = :product_id
        LIMIT 1
    """)
    row = db.execute(
        sql,
        {
            "product_id": int(product_id)
        }
    ).fetchone()
    if not row:
        return None
    return row.group_nom_real

# Главная функция получения коэффициента выхода.
def _get_percent_output_by_formula(
        factory_id,
        category_product_id,
        product_id,
        ei_id,
        variant_plan,
        year,
        month
):
    """
    Правила выбора формулы:
        - все продукты категории -> формула категории;
        - один продукт + одна бюджетная статья -> формула продукта;
        - один продукт + несколько бюджетных статей -> формула категории.
    """

    formula_row = _get_formula_for_selection(
        factory_id=factory_id,
        category_product_id=category_product_id,
        product_id=product_id,
        ei_id=ei_id
    )

    if not formula_row:
        return None

    return _calculate_output_coefficient(
        formula_row=formula_row,
        factory_id=factory_id,
        ei_id=ei_id,
        variant_plan=variant_plan,
        year=year,
        month=month
    )
# =======================================================================================================================
# Расчёт % выхода
# =======================================================================================================================

def get_list_percent_from_lists(
        list_base,
        list_slice,
        filters=None,
        selected_factories=None,
        variant_columns=None,
        ei=1
):
    """
    Расчёт процента выхода.
    Если:
        - выбран один завод;
        - выбрана категория продукта;
        - продукты не выбраны,

    используется формула категории.
    Если:
        - выбран один завод;
        - выбрана категория;
        - выбран один продукт,

    используется:
        - формула продукта, если ему соответствует одна бюджетная статья;
        - формула категории, если бюджетных статей несколько.

    При отсутствии необходимых данных используется старая формула slice / base * 100
    """

    result = []

    if not list_base or not list_slice:
        return result

    if len(list_base) != len(list_slice):
        return result

    filters = filters or {}
    selected_factories = selected_factories or []
    variant_columns = variant_columns or []

    factory_list = [
        int(factory_id)
        for factory_id in selected_factories
        if factory_id is not None
    ]

    # --------------------------------------------------------------------------------
    # Формульный расчёт применяется только для одного выбранного завода.
    # --------------------------------------------------------------------------------
    use_formula = len(factory_list) == 1

    factory_id = factory_list[0] if use_formula else None

    category_list = [
        int(category_id)
        for category_id in filters.get("cat_product", [])
        if category_id is not None
    ]

    product_list = [
        int(product_id)
        for product_id in filters.get("product", [])
        if product_id is not None
    ]

    # --------------------------------------------------------------------------------
    # Если выбрана категория, но продукты не выбраны,
    # это означает "все продукты категории".
    # --------------------------------------------------------------------------------
    category_product_id = (
        category_list[0]
        if len(category_list) == 1
        else None
    )

    product_id = (
        product_list[0]
        if len(product_list) == 1
        else None
    )

    # Если выбрано несколько категорий или продуктов,
    # однозначно выбрать формулу нельзя.
    if len(category_list) != 1:
        use_formula = False

    if len(product_list) > 1:
        use_formula = False

    # --------------------------------------------------------------------------------
    # Получаем variantPlaning и year для двух вариантов.
    # --------------------------------------------------------------------------------
    variant_data = {}

    for idx, item in enumerate(variant_columns, start=1):

        try:
            variant_idx = int(idx)
        except (TypeError, ValueError):
            continue

        if str(variant_idx) not in [
            str(value) for value in filters.get(
                "_selected_variant_compare",
                []
            )
        ]:
            continue

        variant_data[variant_idx] = {
            "variantPlaning": int(
                item.get("variantPlaning", 0)
            ),
            "year": int(
                item.get("year", 0)
            )
        }

    # --------------------------------------------------------------------------------
    # В текущей функции list_base/list_slice уже являются результатами
    # variant1/variant2, поэтому variant_columns может не содержать
    # selected_variant_compare.
    #
    # В этом случае определяем варианты по порядку.
    # --------------------------------------------------------------------------------
    if not variant_data:

        for idx, item in enumerate(variant_columns, start=1):

            if idx > 2:
                break

            variant_data[idx] = {
                "variantPlaning": int(
                    item.get("variantPlaning", 0)
                ),
                "year": int(
                    item.get("year", 0)
                )
            }

    # --------------------------------------------------------------------------------
    # Основной цикл по месяцам.
    # --------------------------------------------------------------------------------
    for i in range(len(list_base)):

        base_item = list_base[i]
        slice_item = list_slice[i]

        month = base_item.get(
            "month",
            slice_item.get("month")
        )

        if month is None:
            continue

        try:
            month_int = int(month)
        except (TypeError, ValueError):
            continue

        v1_base = _safe_float(
            base_item.get("variant1", 0.0)
        )

        v1_slice = _safe_float(
            slice_item.get("variant1", 0.0)
        )

        v2_base = _safe_float(
            base_item.get("variant2", 0.0)
        )

        v2_slice = _safe_float(
            slice_item.get("variant2", 0.0)
        )

        # --------------------------------------------------------------------------------
        # Старый расчёт — fallback.
        # --------------------------------------------------------------------------------
        percent_v1 = (
            round(v1_slice / v1_base * 100, 1)
            if v1_base != 0
            else 0.0
        )

        percent_v2 = (
            round(v2_slice / v2_base * 100, 1)
            if v2_base != 0
            else 0.0
        )

        # --------------------------------------------------------------------------------
        # Новый расчёт по формуле.
        # --------------------------------------------------------------------------------
        if use_formula and factory_id is not None:

            # -----------------------------
            # Вариант 1
            # -----------------------------
            variant1_formula_data = variant_data.get(1)

            if variant1_formula_data:

                percent_formula_v1 = _get_percent_output_by_formula(
                    factory_id=factory_id,
                    category_product_id=category_product_id,
                    product_id=product_id,
                    ei_id=ei,
                    variant_plan=variant1_formula_data[
                        "variantPlaning"
                    ],
                    year=variant1_formula_data["year"],
                    month=month_int
                )

                if percent_formula_v1 is not None:
                    percent_v1 = round(
                        percent_formula_v1,
                        1
                    )

            # -----------------------------
            # Вариант 2
            # -----------------------------
            variant2_formula_data = variant_data.get(2)

            if variant2_formula_data:

                percent_formula_v2 = _get_percent_output_by_formula(
                    factory_id=factory_id,
                    category_product_id=category_product_id,
                    product_id=product_id,
                    ei_id=ei,
                    variant_plan=variant2_formula_data[
                        "variantPlaning"
                    ],
                    year=variant2_formula_data["year"],
                    month=month_int
                )

                if percent_formula_v2 is not None:
                    percent_v2 = round(
                        percent_formula_v2,
                        1
                    )

        result.append({
            "month": month,
            "variant1": percent_v1,
            "variant2": percent_v2
        })

    return result
#=======================================================================================================================
def convert_data_to_tab_front(result, key_name, reverse_diff=True):
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
        reverse_diff=True
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
    category_join_str = ""

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
        query_params["ei"] = ei

    category_list = []

    # Блок обработки фильтров с учетом категории продуктов
    if filters:
        category_list = [int(x) for x in filters.get("cat_product", [])]
        idx = 0

        for key, value in filters.items():

            if key == "cat_product":
                if value:
                    category_join_str = """
                            JOIN tab_view_product_d816_4 AS product
                                ON main.tab_product_d816_4_ids = product.id
                            JOIN tab_category_product_d816_4 AS category
                                ON product.group_nom_real = category.id
                        """

                    filter_str += f"category.id = ANY(:flt{idx}) AND "
                    query_params[f"flt{idx}"] = [int(flt) for flt in value]
                    idx += 1

                continue

            if key == "product":
                continue

            if not value:
                continue

            need_key = mapping_col.get(key)

            if need_key is None:
                continue

            filter_str += f"main.{need_key} = ANY(:flt{idx}) AND "
            query_params[f"flt{idx}"] = [int(v) for v in value]
            idx += 1

    product_list = [int(item) for item in product]

    if category_list:
        sql_products = text("""
                SELECT id
                FROM tab_view_product_d816_4
                WHERE group_nom_real = ANY(:cat_product)
                ORDER BY name
            """)

        result_products = db.execute(
            sql_products,
            {"cat_product": category_list}
        ).fetchall()

        category_product_ids = [row.id for row in result_products]

        # Если пользователь продукты не выбрал — берем все продукты категории
        if not product_list:
            product_list = category_product_ids

        # Если выбрал — оставляем только продукты этой категории
        else:
            product_list = [
                p for p in product_list
                if p in category_product_ids
            ]

    # --------------------------------------------------------------------------------------------------
    # Фильтр по продуктам

    product_str = ""
    if product_list:
        product_str = "main.tab_product_d816_4_ids = ANY(:product) AND"
        query_params["product"] = product_list

    type_raspr_list = [int(item) for item in type_raspr]
    factory_list = [int(item) for item in selected_factories]

    # var_plans_list = []
    # for item in selected_variant_compare:
    #     idx = int(item) - 1
    #     if 0 <= idx < len(variant_columns):
    #         var_plans_list.append(variant_columns[idx].get("variantPlaning", 0))

    # years_list = []
    # for item in selected_variant_compare:
    #     idx = int(item) - 1
    #     if 0 <= idx < len(variant_columns):
    #         years_list.append(variant_columns[idx].get("year", 0))

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
            {category_join_str}
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
    if v_filters_middle_volume_frame1:
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
            # Верхняя левая панель, где 4 карточки
            'panel_upper_year_volume_frame1' : get_calc_volume(
                'year',
                [64],  # Газ
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=2, ),  # млн. м3 (Единица измерения)
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
            # Правый график с коэффициентами выхода
            'panel_upper_month_volume_graph1': get_list_percent_from_lists(
                get_calc_volume(
                    'month',
                    [],  #
                    [7],  # Переработка
                    v_filters_middle_volume_frame2 or {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=1, ),  # тыс тонн (Единица измерения)
                get_calc_volume(
                    'month',
                    [],
                    [7],
                    v_filters_middle_volume_frame2 or {},
                    selected_variant_compare,
                    selected_factories,
                    variant_columns,
                    ei=1,  # тыс тонн (Единица измерения)
                ),
                filters=v_filters_middle_volume_frame2 or {},
                selected_factories=selected_factories,
                variant_columns=variant_columns,
                ei=1
            ),
            # Центральный левый график
            'panel_middle_month_volume_frame1': get_calc_volume(
                'month',
                [],  #
                [5],  # Переработка
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            # Центральный правый график
            'panel_middle_month_volume_frame2': get_calc_volume(
                'month',
                [],  # Газ
                [7],  # Производство
                {},
                selected_variant_compare,
                selected_factories,
                variant_columns,
                ei=1, ),  # тыс тонн (Единица измерения)
            # Левая таблица
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
            # Правая таблица
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