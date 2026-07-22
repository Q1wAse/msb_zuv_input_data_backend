from sqlalchemy import text

import msb_zuv_input_data_backend.functions.utility_functions as uf

def get_struct_prod_metrics():
    return [0,1]
#=======================================================================================================================
def get_calc_frame1(selected_variant_compare,selected_factories, variant_columns):
    db = uf.get_db_connection()

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
    col_sql = text("""
                        SELECT
                            sum(main.value),
                            params.idx as variantColumns
                        FROM tab_pererabotka_d816_4 main
                        JOIN LATERAL unnest(CAST(:var_plans AS INTEGER[]), CAST(:years AS INTEGER[])) WITH ORDINALITY AS params(var_plan, year, idx)
                          ON main.tab_var_plan_d816_4_ids = params.var_plan 
                          AND main.year = params.year
                        WHERE
                            main.tab_product_d816_4_ids = 64 AND
                            main.tab_type_raspr_d816_4_ids = 5 AND
                            main.month = 0 AND 
                            main.quarter = 0 AND
                            main.tab_factory_d816_4_ids = ANY(:factory) AND
                            main.tab_ei_d816_4_ids = 2
                        GROUP BY params.idx
                        ORDER BY params.idx;
                    """)
    if var_plans_list and years_list:
        result = db.execute(col_sql,
                            {
                                'factory' : factory_list,
                                'var_plans': var_plans_list,
                                'years': years_list
                            }
                            ).fetchall()
        return result
    else:
        return []
#=======================================================================================================================
def get_calculated_values(selected_variant_compare, selected_factories, variant_columns):
    collection  = {
        'frame1' : get_calc_frame1(selected_variant_compare,selected_factories,variant_columns)
    }
    return collection