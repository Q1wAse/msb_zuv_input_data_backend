from flask import request
from flask_restx import Namespace, Resource
from sqlalchemy import text
from decimal import Decimal
import datetime

from msb_zuv_input_data_backend.database import cache, errorhandler
import msb_zuv_input_data_backend.functions.utility_functions as uf


# ======================================================================================================================
# Namespace
# ======================================================================================================================

ns_mapping = Namespace(
    'mapping',
    description='API для ведения таблицы мэппинга'
)
# ======================================================================================================================
# Вспомогательные функции
# ======================================================================================================================
def _get_db():
    return uf.get_db_connection()
def _to_int(value, field_name):
    if value is None or value == '':
        raise ValueError(f'Поле {field_name} обязательно')
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f'Поле {field_name} должно быть числом')
def _to_float(value, field_name):
    if value is None or value == '':
        raise ValueError(f'Поле {field_name} обязательно')

    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f'Поле {field_name} должно быть числом')
def _to_id(value):
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None
def _generate_id_str(
        mapping_id,
        factory_id,
        type_raspr_id,
        year
):
    """
    id_str формируется по правилу: id + '_' + factory.id + '_' + type_raspr.id + '_' + year (100332254_7_7_2026)
    """
    return (
        f'{int(mapping_id)}_'
        f'{int(factory_id)}_'
        f'{int(type_raspr_id)}_'
        f'{int(year)}'
    )
def _get_dictionary(
        table_name,
        order_by='name'
):
    """
    Получить простой справочник:
        [
            {
                "id": 1,
                "label": "Наименование"
            }
        ]
    """
    db = _get_db()
    sql = text(f"""
        SELECT
            id,
            name
        FROM {table_name}
        ORDER BY {order_by}
    """)
    rows = db.execute(sql).fetchall()
    return [
        {
            'id': row.id,
            'label': row.name
        }
        for row in rows
    ]
def _get_years():
    db = _get_db()

    sql = text("""
        SELECT
            year
        FROM tab_view_year_d816_4
        ORDER BY year
    """)
    rows = db.execute(sql).fetchall()
    return [
        row.year
        for row in rows
    ]
def _get_coefficients():
    """
    Коэффициенты - значения 1 и 0,001.
    """
    return [
        {
            'value': 1,
            'label': '1'
        },
        {
            'value': 0.001,
            'label': '0,001'
        }
    ]
def _get_products():
    db = _get_db()
    sql = text("""
        SELECT
            id,
            name
        FROM tab_view_product_d816_4
        ORDER BY name
    """)
    rows = db.execute(sql).fetchall()
    return [
        {
            'id': row.id,
            'label': row.name
        }
        for row in rows
    ]
def _get_product_categories():
    db = _get_db()

    sql = text("""
        SELECT
            id,
            name
        FROM tab_category_product_d816_4
        ORDER BY ord, name
    """)
    rows = db.execute(sql).fetchall()
    return [
        {
            'id': row.id,
            'label': row.name
        }
        for row in rows
    ]
# ======================================================================================================================
# 1. GET /mapping/structure
# ======================================================================================================================
@ns_mapping.route('/structure')
class MappingStructure(Resource):
    def get(self):
        try:
            db = _get_db()
            # ----------------------------------------------------------------------------------
            # Справочники
            # ----------------------------------------------------------------------------------
            products = _get_products()
            factories = _get_dictionary('tab_factory_d816_4')
            distribution_types = _get_dictionary('tab_type_raspr_d816_4')
            owners = _get_dictionary('tab_sobstv_d816_4')
            fields = _get_dictionary('tab_mest_d816_4')
            suppliers = _get_dictionary('tab_post_zuv_d816_4')
            units = _get_dictionary('tab_ei_d816_4')
            product_categories = _get_product_categories()
            years = _get_years()
            # ----------------------------------------------------------------------------------
            # form_lines
            # Пока возвращаем пустой список.
            # ----------------------------------------------------------------------------------
            form_lines = []
            # ----------------------------------------------------------------------------------
            # last_update
            # ----------------------------------------------------------------------------------
            last_update_sql = text("""
                SELECT
                    MAX(updated_at) AS last_update
                FROM tab_map_bs_product_d816_4
            """)
            try:
                last_update_row = db.execute(
                    last_update_sql
                ).fetchone()
                last_update = (
                    last_update_row.last_update
                    if last_update_row
                    else None
                )

            except Exception:
                # Если в таблице нет updated_at
                last_update = None
            return {
                'last_update': (
                    last_update.isoformat()
                    if isinstance(
                        last_update,
                        (datetime.datetime, datetime.date)
                    )
                    else last_update
                ),
                'references': {
                    'form_lines': form_lines,
                    'products': products,
                    'coefficients': _get_coefficients(),
                    'factories': factories,
                    'distribution_types': distribution_types,
                    'owners': owners,
                    'fields': fields,
                    'product_categories': product_categories,
                    'suppliers': suppliers,
                    'units': units,
                    'years': years
                }
            }, 200

        except Exception as e:
            ns_mapping.abort(*errorhandler(e))
# ======================================================================================================================
# Проверка входных данных строки
# ======================================================================================================================
def _validate_mapping_data(data):
    if not isinstance(data, dict):
        raise ValueError('Тело запроса должно быть JSON-объектом')

    # Обязательные поля
    required_fields = [
        'id',
        'coefficient',
        'factory_id',
        'product_id',
        'product_category_id',
        'unit_id',
        'year'
    ]
    for field_name in required_fields:
        if field_name not in data or data[field_name] in (None, ''):
            raise ValueError(
                f'Поле {field_name} обязательно'
            )
    result = {
        'id': _to_int(data.get('id'), 'id'),
        'koef': _to_float(
            data.get('coefficient'),
            'coefficient'
        ),
        'factory': _to_int(
            data.get('factory_id'),
            'factory_id'
        ),
        'id_product': _to_int(
            data.get('product_id'),
            'product_id'
        ),
        'category_product_id': _to_int(
            data.get('product_category_id'),
            'product_category_id'
        ),
        'ei_id': _to_int(
            data.get('unit_id'),
            'unit_id'
        ),
        'year': _to_int(
            data.get('year'),
            'year'
        ),

        'type_raspr': _to_id(
            data.get('distribution_type_id')
        ),
        'sobstv': _to_id(
            data.get('owner_id')
        ),
        'mest': _to_id(
            data.get('field_id')
        ),
        'post_id': _to_id(
            data.get('supplier_id')
        )
    }
    return result
# ======================================================================================================================
# Проверка существования ссылочных значений
# ======================================================================================================================
def _validate_references(data):
    db = _get_db()

    checks = [
        (
            'factory',
            'tab_factory_d816_4',
            data['factory']
        ),
        (
            'id_product',
            'tab_view_product_d816_4',
            data['id_product']
        ),
        (
            'category_product_id',
            'tab_category_product_d816_4',
            data['category_product_id']
        ),
        (
            'ei_id',
            'tab_ei_d816_4',
            data['ei_id']
        )
    ]

    optional_checks = [
        (
            'type_raspr',
            'tab_type_raspr_d816_4',
            data.get('type_raspr')
        ),
        (
            'sobstv',
            'tab_sobstv_d816_4',
            data.get('sobstv')
        ),
        (
            'mest',
            'tab_mest_d816_4',
            data.get('mest')
        ),
        (
            'post_id',
            'tab_post_zuv_d816_4',
            data.get('post_id')
        )
    ]

    checks.extend(
        item
        for item in optional_checks
        if item[2] is not None
    )

    for field_name, table_name, value in checks:

        sql = text(f"""
            SELECT 1
            FROM {table_name}
            WHERE id = :id
            LIMIT 1
        """)

        row = db.execute(
            sql,
            {'id': value}
        ).fetchone()

        if not row:
            raise ValueError(
                f'Значение {value} для поля {field_name} '
                f'не найдено в справочнике {table_name}'
            )

# ======================================================================================================================
# 2. GET + POST /mapping/rows
# ======================================================================================================================
@ns_mapping.route('/rows')
class MappingRows(Resource):

    def get(self):
        try:
            db = _get_db()

            sql = text("""
                SELECT
                    map.id,
                    map.koef,
                    map.factory,
                    map.type_raspr,
                    map.sobstv,
                    map.mest,
                    map.category_product_id,
                    map.id_product,
                    map.post_id,
                    map.ei_id,
                    map.year,
                    map.id_str,
                    map.name AS article_name,
                    product.name AS product_name,
                    factory.name AS factory_name,
                    type_raspr.name AS type_raspr_name,
                    sobstv.name AS sobstv_name,
                    mest.name AS mest_name,
                    category.name AS category_name,
                    post.name AS post_name,
                    ei.name AS ei_name
                FROM tab_map_bs_product_d816_4 AS map
                LEFT JOIN tab_view_product_d816_4 AS product
                    ON product.id = map.id_product
                LEFT JOIN tab_factory_d816_4 AS factory
                    ON factory.id = map.factory
                LEFT JOIN tab_type_raspr_d816_4 AS type_raspr
                    ON type_raspr.id = map.type_raspr
                LEFT JOIN tab_sobstv_d816_4 AS sobstv
                    ON sobstv.id = map.sobstv
                LEFT JOIN tab_mest_d816_4 AS mest
                    ON mest.id = map.mest
                LEFT JOIN tab_category_product_d816_4 AS category
                    ON category.id = map.category_product_id
                LEFT JOIN tab_post_zuv_d816_4 AS post
                    ON post.id = map.post_id
                LEFT JOIN tab_ei_d816_4 AS ei
                    ON ei.id = map.ei_id
                ORDER BY
                    map.id,
                    map.factory,
                    map.type_raspr,
                    map.year
            """)

            rows = db.execute(sql).fetchall()

            items = []

            for row in rows:
                items.append({
                    'id': row.id,

                    'article': {
                        'id': row.id,
                        'label': row.article_name
                    } if row.article_name is not None else None,

                    'form_line': None,

                    'product': {
                        'id': row.id_product,
                        'label': row.product_name
                    } if row.id_product is not None else None,

                    'coefficient': (
                        float(row.koef)
                        if row.koef is not None
                        else None
                    ),

                    'factory': {
                        'id': row.factory,
                        'label': row.factory_name
                    } if row.factory is not None else None,

                    'distribution_type': {
                        'id': row.type_raspr,
                        'label': row.type_raspr_name
                    } if row.type_raspr is not None else None,

                    'owner': {
                        'id': row.sobstv,
                        'label': row.sobstv_name
                    } if row.sobstv is not None else None,

                    'field': {
                        'id': row.mest,
                        'label': row.mest_name
                    } if row.mest is not None else None,

                    'product_category': {
                        'id': row.category_product_id,
                        'label': row.category_name
                    } if row.category_product_id is not None else None,

                    'supplier': {
                        'id': row.post_id,
                        'label': row.post_name
                    } if row.post_id is not None else None,

                    'unit': {
                        'id': row.ei_id,
                        'label': row.ei_name
                    } if row.ei_id is not None else None,

                    'year': row.year
                })

            return {
                'items': items
            }, 200

        except Exception as e:
            ns_mapping.abort(*errorhandler(e))

    def post(self):
        try:
            data = request.get_json(silent=True)

            mapping = _validate_mapping_data(data)

            _validate_references(mapping)

            db = _get_db()

            id_str = _generate_id_str(
                mapping_id=mapping['id'],
                factory_id=mapping['factory'],
                type_raspr_id=mapping.get('type_raspr') or 0,
                year=mapping['year']
            )

            check_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
                LIMIT 1
            """)

            existing = db.execute(
                check_sql,
                {'id_str': id_str}
            ).fetchone()

            if existing:
                return {
                    'code': 'validation_error',
                    'message': (
                        f'Мэппинг с id_str={id_str} уже существует'
                    )
                }, 400

            insert_sql = text("""
                INSERT INTO tab_map_bs_product_d816_4 (
                    id,
                    koef,
                    factory,
                    id_product,
                    type_raspr,
                    sobstv,
                    mest,
                    category_product_id,
                    post_id,
                    ei_id,
                    year,
                    id_str
                )
                VALUES (
                    :id,
                    :koef,
                    :factory,
                    :id_product,
                    :type_raspr,
                    :sobstv,
                    :mest,
                    :category_product_id,
                    :post_id,
                    :ei_id,
                    :year,
                    :id_str
                )
            """)

            db.execute(
                insert_sql,
                {
                    **mapping,
                    'id_str': id_str
                }
            )

            db.commit()

            return {
                'id': mapping['id'],
                'id_str': id_str
            }, 201

        except ValueError as e:
            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400
# ======================================================================================================================
# 3. PUT + DELETE /mapping/rows/{mapping_id}
# ======================================================================================================================
@ns_mapping.route('/rows/<int:mapping_id>')
class MappingRowsUpdate(Resource):

    def put(self, mapping_id):
        try:
            data = request.get_json(silent=True)

            mapping = _validate_mapping_data(data)

            # В PUT id берём из URL.
            mapping['id'] = mapping_id

            _validate_references(mapping)

            db = _get_db()

            # Проверяем существование строки
            check_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id = :id
                LIMIT 1
            """)

            existing = db.execute(
                check_sql,
                {'id': mapping_id}
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': 'Строка мэппинга не найдена'
                }, 404

            id_str = _generate_id_str(
                mapping_id=mapping['id'],
                factory_id=mapping['factory'],
                type_raspr_id=mapping.get('type_raspr') or 0,
                year=mapping['year']
            )

            # Не допускаем дубль id_str
            duplicate_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
                  AND id <> :id
                LIMIT 1
            """)

            duplicate = db.execute(
                duplicate_sql,
                {
                    'id_str': id_str,
                    'id': mapping_id
                }
            ).fetchone()

            if duplicate:
                return {
                    'code': 'validation_error',
                    'message': (
                        f'Мэппинг с id_str={id_str} уже существует'
                    )
                }, 400

            update_sql = text("""
                UPDATE tab_map_bs_product_d816_4
                SET
                    koef = :koef,
                    factory = :factory,
                    id_product = :id_product,
                    type_raspr = :type_raspr,
                    sobstv = :sobstv,
                    mest = :mest,
                    category_product_id = :category_product_id,
                    post_id = :post_id,
                    ei_id = :ei_id,
                    year = :year,
                    id_str = :id_str
                WHERE id = :id
            """)

            db.execute(
                update_sql,
                {
                    **mapping,
                    'id_str': id_str
                }
            )

            db.commit()

            return {
                'id': mapping_id,
                'id_str': id_str
            }, 200

        except ValueError as e:
            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400
    def delete(self, mapping_id):
        try:
            db = _get_db()

            check_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id = :id
                LIMIT 1
            """)

            existing = db.execute(
                check_sql,
                {'id': mapping_id}
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': 'Строка мэппинга не найдена'
                }, 404

            delete_sql = text("""
                DELETE FROM tab_map_bs_product_d816_4
                WHERE id = :id
            """)

            db.execute(
                delete_sql,
                {'id': mapping_id}
            )

            db.commit()

            return {
                'message': 'Строка удалена'
            }, 200

        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось удалить данные'
            }, 400
# ======================================================================================================================
# 4. PATCH /mapping/dictionaries/products
# ======================================================================================================================
@ns_mapping.route('/dictionaries/products')
class MappingProductsDictionary(Resource):

    def patch(self):
        try:
            data = request.get_json(silent=True)

            if not isinstance(data, dict):
                raise ValueError(
                    'Тело запроса должно быть JSON-объектом'
                )

            create_list = data.get('create', [])
            update_list = data.get('update', [])
            delete_ids = data.get('delete_ids', [])

            if not isinstance(create_list, list):
                raise ValueError('Поле create должно быть массивом')

            if not isinstance(update_list, list):
                raise ValueError('Поле update должно быть массивом')

            if not isinstance(delete_ids, list):
                raise ValueError(
                    'Поле delete_ids должно быть массивом'
                )

            db = _get_db()

            # ----------------------------------------------------------------------------------
            # CREATE
            # ----------------------------------------------------------------------------------
            for item in create_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент create должен быть объектом'
                    )

                name = item.get('name')

                if not name:
                    raise ValueError(
                        'Для создания продукта поле name обязательно'
                    )

                sql = text("""
                    INSERT INTO tab_view_product_d816_4 (
                        name
                    )
                    VALUES (
                        :name
                    )
                """)

                db.execute(
                    sql,
                    {
                        'name': name
                    }
                )

            # ----------------------------------------------------------------------------------
            # UPDATE
            # ----------------------------------------------------------------------------------

            for item in update_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент update должен быть объектом'
                    )

                product_id = item.get('id')
                name = item.get('name')

                if product_id is None:
                    raise ValueError(
                        'Для изменения продукта поле id обязательно'
                    )

                if not name:
                    raise ValueError(
                        'Для изменения продукта поле name обязательно'
                    )

                sql = text("""
                    UPDATE tab_view_product_d816_4
                    SET name = :name
                    WHERE id = :id
                """)

                db.execute(
                    sql,
                    {
                        'id': int(product_id),
                        'name': name
                    }
                )

            # ----------------------------------------------------------------------------------
            # DELETE
            # ----------------------------------------------------------------------------------

            for product_id in delete_ids:

                sql = text("""
                    DELETE FROM tab_product_pererabotka_d816_4
                    WHERE id = :id
                """)

                db.execute(
                    sql,
                    {
                        'id': int(product_id)
                    }
                )

            db.commit()

            return {
                'message': 'Справочник продуктов обновлён'
            }, 200

        except ValueError as e:
            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400