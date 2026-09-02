from flask import request
from flask_restx import Namespace, Resource, fields
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
# Модель JSON ля POST -запроса
mapping_post_model = ns_mapping.model(
    'MappingRowPost',
    {
        'id': fields.Integer(
            required=True,
            default=100330007,
            description='Бюджетная статья'
        ),
        'article_name': fields.String(
            required=False,
            default='',
            description='Наименование статьи'
        ),
        'product_id': fields.Integer(
            required=True,
            default=64
        ),
        'coefficient': fields.Float(
            required=True,
            default=1
        ),
        'factory_id': fields.Integer(
            required=True,
            default=1
        ),
        'distribution_type_id': fields.Integer(
            required=False,
            default=9
        ),
        'owner_id': fields.Integer(
            required=False,
            default=1
        ),
        'field_id': fields.Integer(
            required=False,
            default=32
        ),
        'product_category_id': fields.Integer(
            required=True,
            default=2
        ),
        'supplier_id': fields.Integer(
            required=False,
            default=10
        ),
        'unit_id': fields.Integer(
            required=True,
            default=1
        ),
        'year': fields.Integer(
            required=True,
            default=2099
        )
    }
)
# Модель JSON ля PUT -запроса
mapping_put_model = ns_mapping.model(
    'MappingRowPut',
    {
        'id': fields.Integer(
            required=True,
            description='ID бюджетной статьи'
        ),
        'article_name': fields.String(
            required=False,
            description='Наименование статьи'
        ),
        'product_id': fields.Integer(
            required=True,
            description='Продукт'
        ),
        'coefficient': fields.Float(
            required=True,
            description='Коэффициент'
        ),
        'factory_id': fields.Integer(
            required=True,
            description='Предприятие'
        ),
        'distribution_type_id': fields.Integer(
            required=False,
            description='Тип распределения'
        ),
        'owner_id': fields.Integer(
            required=False,
            description='Собственник'
        ),
        'field_id': fields.Integer(
            required=False,
            description='Месторождение / поле'
        ),
        'product_category_id': fields.Integer(
            required=True,
            description='Категория продукта'
        ),
        'supplier_id': fields.Integer(
            required=False,
            description='Поставщик'
        ),
        'unit_id': fields.Integer(
            required=True,
            description='Единица измерения'
        ),
        'year': fields.Integer(
            required=True,
            description='Год'
        )
    }
)

# Модель JSON для PATCH справочника продуктов
mapping_products_patch_model = ns_mapping.model(
    'MappingProductsPatch',
    {
        'create': fields.List(
            fields.Nested(
                ns_mapping.model(
                    'MappingProductCreate',
                    {
                        'name': fields.String(
                            required=True,
                            description='Наименование нового продукта'
                        ),
                        'category_id': fields.Integer(
                            required=True,
                            description='Категория продукта из tab_category_product_d816_4'
                        )
                    }
                )
            ),
            required=False,
            default=[
                {
                    'name': 'Новый продукт',
                    'category_id': 2
                }
            ],
            description='Продукты для создания'
        ),

        'update': fields.List(
            fields.Nested(
                ns_mapping.model(
                    'MappingProductUpdate',
                    {
                        'id': fields.Integer(
                            required=True,
                            description='ID продукта'
                        ),
                        'name': fields.String(
                            required=True,
                            description='Новое наименование продукта'
                        ),
                        'category_id': fields.Integer(
                            required=True,
                            description='Категория продукта из tab_category_product_d816_4'
                        )
                    }
                )
            ),
            required=False,
            default=[
                {
                    'id': 100000001,
                    'name': 'Изменённый продукт',
                    'category_id': 2
                }
            ],
            description='Продукты для изменения'
        ),

        'delete_ids': fields.List(
            fields.Integer,
            required=False,
            default=[100000001],
            description='ID продуктов для удаления'
        )
    }
)
# Модель JSON для PATCH справочников Собственник, Месторождение, Поставщик
mapping_dictionary_patch_model = ns_mapping.model(
    'MappingDictionaryPatch',
    {
        'create': fields.List(
            fields.Nested(
                ns_mapping.model(
                    'MappingDictionaryCreate',
                    {
                        'name': fields.String(
                            required=True,
                            description='Наименование нового элемента справочника'
                        )
                    }
                )
            ),
            required=False,
            description='Элементы для создания'
        ),

        'update': fields.List(
            fields.Nested(
                ns_mapping.model(
                    'MappingDictionaryUpdate',
                    {
                        'id': fields.Integer(
                            required=True,
                            description='ID элемента справочника'
                        ),
                        'name': fields.String(
                            required=True,
                            description='Новое наименование'
                        )
                    }
                )
            ),
            required=False,
            description='Элементы для изменения'
        ),

        'delete_ids': fields.List(
            fields.Integer,
            required=False,
            description='ID элементов для удаления'
        )
    }
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
    Получить простой справочник со структурой:
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
            # last_update - пока в таблице поле пустое
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
        raise ValueError('Тело запроса должно быть JSON')
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
        'article_name': (
            data.get('article_name')
            if data.get('article_name') is not None
            else ''
        ),
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
                WHERE map.factory > 0
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

                    'article_name': row.article_name,

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

    @ns_mapping.expect(mapping_post_model, validate=True)
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
                    name,
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
                    :article_name,
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
@ns_mapping.route('/rows/<string:id_str>')
class MappingRowsUpdate(Resource):

    @ns_mapping.expect(mapping_put_model, validate=True)
    def put(self, id_str):
        try:
            data = request.get_json(force=True, silent=False)

            if not isinstance(data, dict):
                raise ValueError(
                    'Тело запроса должно быть JSON'
                )
            # 1. Находим существующую строку по СТАРОМУ id_str
            db = _get_db()

            select_sql = text("""
                SELECT
                    id,
                    name,
                    koef,
                    factory,
                    type_raspr,
                    sobstv,
                    mest,
                    category_product_id,
                    id_product,
                    post_id,
                    ei_id,
                    year,
                    id_str
                FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
                LIMIT 1
            """)

            existing = db.execute(
                select_sql,
                {
                    'id_str': str(id_str)
                }
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': (
                        f'Строка мэппинга с id_str={id_str} не найдена'
                    )
                }, 404

            # 2. Проверяем обязательные поля
            required_fields = [
                'id',
                'product_id',
                'coefficient',
                'factory_id',
                'product_category_id',
                'unit_id',
                'year'
            ]

            for field_name in required_fields:
                if (
                        field_name not in data
                        or data[field_name] is None
                        or data[field_name] == ''
                ):
                    raise ValueError(
                        f'Поле {field_name} обязательно'
                    )

            # 3. Получаем ВСЕ новые значения
            mapping_id = _to_int(
                data.get('id'),
                'id'
            )

            article_name = data.get('article_name')

            if article_name is None:
                article_name = ''

            article_name = str(article_name)

            coefficient = _to_float(
                data.get('coefficient'),
                'coefficient'
            )

            factory_id = _to_int(
                data.get('factory_id'),
                'factory_id'
            )

            product_id = _to_int(
                data.get('product_id'),
                'product_id'
            )

            product_category_id = _to_int(
                data.get('product_category_id'),
                'product_category_id'
            )

            unit_id = _to_int(
                data.get('unit_id'),
                'unit_id'
            )

            year = _to_int(
                data.get('year'),
                'year'
            )

            distribution_type_id = _to_id(
                data.get('distribution_type_id')
            )

            owner_id = _to_id(
                data.get('owner_id')
            )

            field_id = _to_id(
                data.get('field_id')
            )

            supplier_id = _to_id(
                data.get('supplier_id')
            )

            # 4. Проверяем существование справочных значений
            reference_checks = [
                (
                    'factory_id',
                    'tab_factory_d816_4',
                    factory_id
                ),
                (
                    'product_id',
                    'tab_view_product_d816_4',
                    product_id
                ),
                (
                    'product_category_id',
                    'tab_category_product_d816_4',
                    product_category_id
                ),
                (
                    'unit_id',
                    'tab_ei_d816_4',
                    unit_id
                )
            ]

            optional_checks = [
                (
                    'distribution_type_id',
                    'tab_type_raspr_d816_4',
                    distribution_type_id
                ),
                (
                    'owner_id',
                    'tab_sobstv_d816_4',
                    owner_id
                ),
                (
                    'field_id',
                    'tab_mest_d816_4',
                    field_id
                ),
                (
                    'supplier_id',
                    'tab_post_zuv_d816_4',
                    supplier_id
                )
            ]

            reference_checks.extend(
                item
                for item in optional_checks
                if item[2] is not None
            )

            for field_name, table_name, value in reference_checks:

                reference_sql = text(f"""
                    SELECT 1
                    FROM {table_name}
                    WHERE id = :id
                    LIMIT 1
                """)

                reference = db.execute(
                    reference_sql,
                    {
                        'id': value
                    }
                ).fetchone()

                if not reference:
                    raise ValueError(
                        f'Значение {value} для поля {field_name} '
                        f'не найдено в справочнике {table_name}'
                    )
            # 5. Формируем новый id_str = id + '_' + factory.id + '_' + type_raspr.id + '_' + year
            new_id_str = _generate_id_str(
                mapping_id=mapping_id,
                factory_id=factory_id,
                type_raspr_id=distribution_type_id or 0,
                year=year
            )

            # 6. Проверяем, что новый id_str не занят другой строкой
            duplicate_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
                  AND id_str <> :old_id_str
                LIMIT 1
            """)

            duplicate = db.execute(
                duplicate_sql,
                {
                    'id_str': new_id_str,
                    'old_id_str': str(id_str)
                }
            ).fetchone()

            if duplicate:
                raise ValueError(
                    f'Мэппинг с id_str={new_id_str} уже существует'
                )

            # 7. UPDATE #id_str рассчитывается автоматически.
            update_sql = text("""
                UPDATE tab_map_bs_product_d816_4
                SET
                    id = :id,
                    name = :article_name,
                    koef = :koef,
                    factory = :factory,
                    type_raspr = :type_raspr,
                    sobstv = :sobstv,
                    mest = :mest,
                    category_product_id = :category_product_id,
                    id_product = :id_product,
                    post_id = :post_id,
                    ei_id = :ei_id,
                    year = :year,
                    id_str = :new_id_str
                WHERE id_str = :old_id_str
            """)

            result = db.execute(
                update_sql,
                {
                    'id': mapping_id,
                    'article_name': article_name,
                    'koef': coefficient,
                    'factory': factory_id,
                    'type_raspr': distribution_type_id,
                    'sobstv': owner_id,
                    'mest': field_id,
                    'category_product_id': product_category_id,
                    'id_product': product_id,
                    'post_id': supplier_id,
                    'ei_id': unit_id,
                    'year': year,
                    'new_id_str': new_id_str,
                    'old_id_str': str(id_str)
                }
            )

            if result.rowcount == 0:
                raise ValueError(
                    'Строка мэппинга не была изменена'
                )

            db.commit()

            return {
                'message': 'Строка мэппинга обновлена',
                'id': mapping_id,
                'id_str': new_id_str,
                'article_name': article_name,
                'product_id': product_id,
                'coefficient': coefficient,
                'factory_id': factory_id,
                'distribution_type_id': distribution_type_id,
                'owner_id': owner_id,
                'field_id': field_id,
                'product_category_id': product_category_id,
                'supplier_id': supplier_id,
                'unit_id': unit_id,
                'year': year
            }, 200

        except ValueError as e:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400

    def delete(self, id_str):
        try:
            db = _get_db()

            # Проверяем существование строки по id_str
            check_sql = text("""
                SELECT 1
                FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
                LIMIT 1
            """)

            existing = db.execute(
                check_sql,
                {
                    'id_str': id_str
                }
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': 'Строка мэппинга не найдена'
                }, 404

            # DELETE по id_str
            delete_sql = text("""
                DELETE FROM tab_map_bs_product_d816_4
                WHERE id_str = :id_str
            """)

            db.execute(
                delete_sql,
                {
                    'id_str': id_str
                }
            )

            db.commit()

            return {
                'message': 'Строка удалена',
                'id_str': id_str
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
    @ns_mapping.expect(mapping_products_patch_model, validate=True)
    def patch(self):
        """
        Создание, изменение и удаление продуктов.
        Изменять можно ТОЛЬКО записи таблицы tab_product_pererabotka_d816_4
        Записи table_st_nom_zuv являются неизменяемыми.
        tab_view_product_d816_4 используется только для отображения полного справочника продуктов.
        """

        db = None
        try:
            data = request.get_json(force=True, silent=False)

            if not isinstance(data, dict):
                raise ValueError(
                    'Тело запроса должно быть JSON'
                )

            create_list = data.get('create', [])
            update_list = data.get('update', [])
            delete_ids = data.get('delete_ids', [])

            # Проверка структуры PATCH
            if not isinstance(create_list, list):
                raise ValueError(
                    'Поле create должно быть массивом'
                )

            if not isinstance(update_list, list):
                raise ValueError(
                    'Поле update должно быть массивом'
                )

            if not isinstance(delete_ids, list):
                raise ValueError(
                    'Поле delete_ids должно быть массивом'
                )

            db = _get_db()
            # CREATE
            created = []

            for item in create_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент create должен быть JSON-объектом'
                    )

                name = item.get('name')

                if name is None or str(name).strip() == '':
                    raise ValueError(
                        'Для создания продукта поле name обязательно'
                    )

                category_id = item.get('category_id')

                if category_id is None or category_id == '':
                    raise ValueError(
                        'Для создания продукта поле category_id обязательно'
                    )

                try:
                    category_id = int(category_id)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле category_id должно быть числом'
                    )
                # Проверяем категорию
                category_sql = text("""
                    SELECT
                        id,
                        name
                    FROM tab_category_product_d816_4
                    WHERE id = :id
                    LIMIT 1
                """)

                category = db.execute(
                    category_sql,
                    {
                        'id': category_id
                    }
                ).fetchone()

                if not category:
                    raise ValueError(
                        f'Категория продукта с id={category_id} '
                        f'не найдена в tab_category_product_d816_4'
                    )

                aggr_level = item.get('aggr_level', 0)

                if aggr_level is None or aggr_level == '':
                    aggr_level = 0

                try:
                    aggr_level = int(aggr_level)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле aggr_level должно быть числом'
                    )

                # ID должен быть следующим номером после максимального ID из tab_view_product_d816_4.
                next_id_sql = text("""
                    SELECT COALESCE(MAX(id), 0) + 1 AS next_id
                    FROM tab_view_product_d816_4
                """)

                next_id_row = db.execute(next_id_sql).fetchone()
                product_id = int(next_id_row.next_id)

                exists_sql = text("""
                    SELECT 1
                    FROM tab_view_product_d816_4
                    WHERE id = :id
                    LIMIT 1
                """)

                exists = db.execute(
                    exists_sql,
                    {
                        'id': product_id
                    }
                ).fetchone()

                if exists:
                    raise ValueError(
                        f'ID продукта {product_id} уже существует'
                    )
                # INSERT только в tab_product_pererabotka_d816_4

                insert_sql = text("""
                    INSERT INTO tab_product_pererabotka_d816_4 (
                        id,
                        name,
                        category_id,
                        aggr_level
                    )
                    VALUES (
                        :id,
                        :name,
                        :category_id,
                        NULL
                    )
                """)

                db.execute(
                    insert_sql,
                    {
                        'id': product_id,
                        'name': str(name).strip(),
                        'category_id': category_id,
                        'aggr_level': aggr_level
                    }
                )

                created.append({
                    'id': product_id,
                    'name': str(name).strip(),
                    'category_id': category_id,
                    'category_name': category.name,
                    'aggr_level': aggr_level
                })
            # UPDATE
            updated = []
            for item in update_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент update должен быть JSON'
                    )

                product_id = item.get('id')

                if product_id is None or product_id == '':
                    raise ValueError(
                        'Для изменения продукта поле id обязательно'
                    )

                try:
                    product_id = int(product_id)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле id должно быть числом'
                    )

                name = item.get('name')

                if name is None or str(name).strip() == '':
                    raise ValueError(
                        'Для изменения продукта поле name обязательно'
                    )

                category_id = item.get('category_id')

                if category_id is None or category_id == '':
                    raise ValueError(
                        'Для изменения продукта поле category_id обязательно'
                    )

                try:
                    category_id = int(category_id)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле category_id должно быть числом'
                    )

                # Проверяем, что продукт существует именно в tab_product_pererabotka_d816_4, а не в table_st_nom_zuv
                product_sql = text("""
                    SELECT
                        id,
                        name,
                        category_id,
                        aggr_level
                    FROM tab_product_pererabotka_d816_4
                    WHERE id = :id
                    LIMIT 1
                """)

                product = db.execute(
                    product_sql,
                    {
                        'id': product_id
                    }
                ).fetchone()

                if not product:
                    # Проверяем, существует ли он вообще в полном справочнике.
                    check_view_sql = text("""
                        SELECT 1
                        FROM tab_view_product_d816_4
                        WHERE id = :id
                        LIMIT 1
                    """)

                    exists_in_view = db.execute(
                        check_view_sql,
                        {
                            'id': product_id
                        }
                    ).fetchone()

                    if exists_in_view:
                        raise ValueError(
                            f'Продукт с id={product_id} является '
                            f'неизменяемым и не может быть изменён'
                        )

                    raise ValueError(
                        f'Продукт с id={product_id} не найден'
                    )
                # Проверяем категорию
                category_sql = text("""
                    SELECT
                        id,
                        name
                    FROM tab_category_product_d816_4
                    WHERE id = :id
                    LIMIT 1
                """)

                category = db.execute(
                    category_sql,
                    {
                        'id': category_id
                    }
                ).fetchone()

                if not category:
                    raise ValueError(
                        f'Категория продукта с id={category_id} '
                        f'не найдена в tab_category_product_d816_4'
                    )

                aggr_level = item.get(
                    'aggr_level',
                    product.aggr_level
                )

                if aggr_level is None or aggr_level == '':
                    aggr_level = 0

                try:
                    aggr_level = int(aggr_level)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле aggr_level должно быть числом'
                    )
                # UPDATE
                update_sql = text("""
                    UPDATE tab_product_pererabotka_d816_4
                    SET
                        name = :name,
                        category_id = :category_id,
                        aggr_level = NULL
                    WHERE id = :id
                """)

                db.execute(
                    update_sql,
                    {
                        'id': product_id,
                        'name': str(name).strip(),
                        'category_id': category_id,
                    }
                )

                updated.append({
                    'id': product_id,
                    'name': str(name).strip(),
                    'category_id': category_id,
                    'category_name': category.name,
                })
            # DELETE
            deleted = []

            for product_id in delete_ids:

                if product_id is None or product_id == '':
                    raise ValueError(
                        'ID продукта для удаления не может быть пустым'
                    )

                try:
                    product_id = int(product_id)
                except (TypeError, ValueError):
                    raise ValueError(
                        'ID продукта для удаления должен быть числом'
                    )
                # Удаляем ТОЛЬКО из изменяемой таблицы.
                product_sql = text("""
                    SELECT id
                    FROM tab_product_pererabotka_d816_4
                    WHERE id = :id
                    LIMIT 1
                """)

                product = db.execute(
                    product_sql,
                    {
                        'id': product_id
                    }
                ).fetchone()

                if not product:

                    check_view_sql = text("""
                        SELECT 1
                        FROM tab_view_product_d816_4
                        WHERE id = :id
                        LIMIT 1
                    """)

                    exists_in_view = db.execute(
                        check_view_sql,
                        {
                            'id': product_id
                        }
                    ).fetchone()

                    if exists_in_view:
                        raise ValueError(
                            f'Продукт с id={product_id} является '
                            f'неизменяемым и не может быть удалён'
                        )

                    raise ValueError(
                        f'Продукт с id={product_id} не найден'
                    )

                delete_sql = text("""
                    DELETE FROM tab_product_pererabotka_d816_4
                    WHERE id = :id
                """)

                db.execute(
                    delete_sql,
                    {
                        'id': product_id
                    }
                )

                deleted.append(product_id)
            # COMMIT
            db.commit()
            # RESPONSE
            return {
                'message': 'Справочник продуктов обновлён',
                'created': created,
                'updated': updated,
                'deleted': deleted
            }, 200

        except ValueError as e:
            try:
                if db:
                    db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception as e:
            try:
                if db:
                    db.rollback()
            except Exception:
                pass
            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400

# ======================================================================================================================
# 5. PATCH /mapping/dictionaries/{dictionary} -  Для Собственник, Месторождение и Поставщик
# ======================================================================================================================
@ns_mapping.route('/dictionaries/<string:dictionary>')
class MappingDictionary(Resource):

    @ns_mapping.expect(mapping_dictionary_patch_model, validate=True)
    def patch(self, dictionary):
        """
        Создание, изменение и удаление элементов справочников:
        owners    -> tab_sobstv_d816_4
        fields    -> tab_mest_d816_4
        suppliers -> tab_post_zuv_d816_4
        """
        dictionary_config = {
            'owners': {
                'table': 'tab_sobstv_d816_4',
                'name': 'Собственник'
            },
            'fields': {
                'table': 'tab_mest_d816_4',
                'name': 'Месторождение'
            },
            'suppliers': {
                'table': 'tab_post_zuv_d816_4',
                'name': 'Поставщик'
            }
        }

        if dictionary not in dictionary_config:
            return {
                'code': 'validation_error',
                'message': (
                    f'Неизвестный справочник "{dictionary}". '
                    f'Допустимые значения: owners, fields, suppliers'
                )
            }, 400

        config = dictionary_config[dictionary]
        table_name = config['table']
        dictionary_name = config['name']

        db = None
        try:
            data = request.get_json(force=True, silent=False)

            if not isinstance(data, dict):
                raise ValueError(
                    'Тело запроса должно быть JSON'
                )
            create_list = data.get('create', [])
            update_list = data.get('update', [])
            delete_ids = data.get('delete_ids', [])
            # Проверка структуры PATCH
            if not isinstance(create_list, list):
                raise ValueError(
                    'Поле create должно быть массивом'
                )

            if not isinstance(update_list, list):
                raise ValueError(
                    'Поле update должно быть массивом'
                )

            if not isinstance(delete_ids, list):
                raise ValueError(
                    'Поле delete_ids должно быть массивом'
                )

            db = _get_db()
            # CREATE
            created = []

            for item in create_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент create должен быть JSON-объектом'
                    )

                name = item.get('name')

                if name is None or str(name).strip() == '':
                    raise ValueError(
                        f'Для создания элемента справочника "{dictionary_name}" '
                        f'поле name обязательно'
                    )

                name = str(name).strip()

                # Проверяем, что такого названия ещё нет
                exists_sql = text(f"""
                    SELECT
                        id,
                        name
                    FROM {table_name}
                    WHERE name = :name
                    LIMIT 1
                """)

                exists = db.execute(
                    exists_sql,
                    {
                        'name': name
                    }
                ).fetchone()

                if exists:
                    raise ValueError(
                        f'{dictionary_name} с наименованием "{name}" '
                        f'уже существует, id={exists.id}'
                    )
                # Новый id
                next_id_sql = text(f"""
                    SELECT
                        COALESCE(MAX(id), 0) + 1 AS next_id
                    FROM {table_name}
                """)

                next_id_row = db.execute(next_id_sql).fetchone()

                new_id = int(next_id_row.next_id)

                # INSERT
                insert_sql = text(f"""
                    INSERT INTO {table_name} (
                        id,
                        name
                    )
                    VALUES (
                        :id,
                        :name
                    )
                """)

                db.execute(
                    insert_sql,
                    {
                        'id': new_id,
                        'name': name
                    }
                )

                created.append({
                    'id': new_id,
                    'name': name
                })
            # UPDATE
            updated = []

            for item in update_list:

                if not isinstance(item, dict):
                    raise ValueError(
                        'Элемент update должен быть JSON'
                    )

                product_id = item.get('id')

                if product_id is None or product_id == '':
                    raise ValueError(
                        f'Для изменения элемента справочника "{dictionary_name}" '
                        f'поле id обязательно'
                    )

                try:
                    item_id = int(product_id)
                except (TypeError, ValueError):
                    raise ValueError(
                        'Поле id должно быть числом'
                    )

                name = item.get('name')

                if name is None or str(name).strip() == '':
                    raise ValueError(
                        f'Для изменения элемента справочника "{dictionary_name}" '
                        f'поле name обязательно'
                    )

                name = str(name).strip()

                select_sql = text(f"""
                    SELECT
                        id,
                        name
                    FROM {table_name}
                    WHERE id = :id
                    LIMIT 1
                """)

                existing = db.execute(
                    select_sql,
                    {
                        'id': item_id
                    }
                ).fetchone()

                if not existing:
                    raise ValueError(
                        f'{dictionary_name} с id={item_id} не найден'
                    )
                # Проверяем дубликат имени
                duplicate_sql = text(f"""
                    SELECT
                        id
                    FROM {table_name}
                    WHERE name = :name
                      AND id <> :id
                    LIMIT 1
                """)

                duplicate = db.execute(
                    duplicate_sql,
                    {
                        'name': name,
                        'id': item_id
                    }
                ).fetchone()

                if duplicate:
                    raise ValueError(
                        f'{dictionary_name} с наименованием "{name}" '
                        f'уже существует, id={duplicate.id}'
                    )
                # UPDATE
                update_sql = text(f"""
                    UPDATE {table_name}
                    SET
                        name = :name
                    WHERE id = :id
                """)

                db.execute(
                    update_sql,
                    {
                        'id': item_id,
                        'name': name
                    }
                )

                updated.append({
                    'id': item_id,
                    'name': name
                })
            # DELETE
            deleted = []

            for value in delete_ids:

                if value is None or value == '':
                    raise ValueError(
                        f'ID элемента справочника "{dictionary_name}" '
                        f'для удаления не может быть пустым'
                    )

                try:
                    item_id = int(value)
                except (TypeError, ValueError):
                    raise ValueError(
                        'ID элемента для удаления должен быть числом'
                    )

                select_sql = text(f"""
                    SELECT
                        id,
                        name
                    FROM {table_name}
                    WHERE id = :id
                    LIMIT 1
                """)

                existing = db.execute(
                    select_sql,
                    {
                        'id': item_id
                    }
                ).fetchone()

                if not existing:
                    raise ValueError(
                        f'{dictionary_name} с id={item_id} не найден'
                    )
                # DELETE
                delete_sql = text(f"""
                    DELETE FROM {table_name}
                    WHERE id = :id
                """)

                db.execute(
                    delete_sql,
                    {
                        'id': item_id
                    }
                )

                deleted.append({
                    'id': item_id,
                    'name': existing.name
                })
            # COMMIT
            db.commit()
            return {
                'message': f'Справочник "{dictionary_name}" обновлён',
                'dictionary': dictionary,
                'created': created,
                'updated': updated,
                'deleted': deleted
            }, 200

        except ValueError as e:

            try:
                if db:
                    db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception as e:

            try:
                if db:
                    db.rollback()
            except Exception:
                pass

            return {
                'code': 'validation_error',
                'message': 'Не удалось сохранить данные'
            }, 400