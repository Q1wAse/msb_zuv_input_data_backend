from sqlalchemy import text

import msb_zuv_input_data_backend.functions.utility_functions as uf

def get_struct_prod_metrics():
    return [0,1]
#=======================================================================================================================
def get_calc_frame1():
    return 0
    # db = uf.get_db_connection()
    # col_sql = text("""
    #                 SELECT
    #                     SUM(SUM),
    #                     BS,
    #                     CALYEAR,
    #                     CALQUART,
    #                     CALMONTH
    #                 FROM tab_integ_get_preu_mirror_d816_4 WHERE
    #                     CALYEAR::INT = :year AND            -- Год планирования
    #                     BCBLM0001::INT = :ver_plan AND      -- Версия планирования
    #                     BCBLM0002::INT = :var_plan AND      -- Вариант планирования
    #                     BS = ANY((:bs)::int[]) AND          -- Бюджетные статьи
    #                     BCBIM0002::INT = :do AND            -- Завод (Дочернее общество)
    #                     pj = :pj AND                        -- Перерабатывающий комплекс (Поставщики ЖУВ)
    #                     DATA_TYPE::INT = :data_type AND     -- Тип данных
    #                     CALMONTH <> 0 AND
    #                     DBS = 0
    #                 GROUP BY BS, CALYEAR, CALQUART, CALMONTH
    #                 ORDER by CALMONTH
    #             """)
    # result = db.execute(col_sql,
    #                     {
    #                         'year': year,
    #                         'ver_plan': ver_plan,
    #                         'var_plan': var_plan,
    #                         'bs': f"{{{','.join(map(str, bs))}}}",
    #                         'do': do,
    #                         'pj': pj,
    #                         'data_type': data_type
    #                     }
    #                     ).fetchall()
    # return result
#=======================================================================================================================
def get_calculated_values(selected_variant_compare, selected_factories, variant_columns):


    return 0