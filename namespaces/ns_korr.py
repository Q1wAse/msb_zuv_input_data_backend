from flask import request
from flask_restx import Namespace, Resource, fields
from sqlalchemy import text

from msb_zuv_input_data_backend.database import cache, errorhandler
import msb_zuv_input_data_backend.functions.utility_functions as uf

ns_korr = Namespace(
    'korr',
    description='API для ведения правил корректировки tab_korr_bs_d816_4'
)
# Модель POST
korr_post_model = ns_korr.model(
    'KorrRowPost',
    {
        'from_bs_id': fields.Integer(
            required=True,
            default=100330007,
            description='Какую бюджетную статью вычесть'
        ),

        'to_bs_id': fields.Integer(
            required=True,
            default=100330008,
            description='Из какой бюджетной статьи вычесть'
        ),

        'new_factory': fields.Integer(
            required=False,
            default=1,
            description='Новый завод'
        ),

        'new_sobstv': fields.Integer(
            required=False,
            default=1,
            description='Новый собственник'
        ),

        'new_mest': fields.Integer(
            required=False,
            default=1,
            description='Новое месторождение'
        ),

        'new_post': fields.Integer(
            required=False,
            default=1,
            description='Новый поставщик'
        ),

        'type_raspr': fields.Integer(
            required=False,
            default=1,
            description='Тип распределения'
        )
    }
)
# Модель PUT
korr_put_model = ns_korr.model(
    'KorrRowPut',
    {
        'from_bs_id': fields.Integer(
            required=True,
            description='Какую бюджетную статью вычесть'
        ),

        'to_bs_id': fields.Integer(
            required=True,
            description='Из какой бюджетной статьи вычесть'
        ),

        'new_factory': fields.Integer(
            required=False,
            description='Новый завод'
        ),

        'new_sobstv': fields.Integer(
            required=False,
            description='Новый собственник'
        ),

        'new_mest': fields.Integer(
            required=False,
            description='Новое месторождение'
        ),

        'new_post': fields.Integer(
            required=False,
            description='Новый поставщик'
        ),

        'type_raspr': fields.Integer(
            required=False,
            description='Тип распределения'
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
        raise ValueError(
            f'Поле {field_name} обязательно'
        )
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f'Поле {field_name} должно быть числом'
        )
def _to_optional_int(value, field_name):
    """
    Для необязательных полей.
    None / '' превращаются в None.
    """
    if value is None or value == '':
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f'Поле {field_name} должно быть числом'
        )
# ======================================================================================================================
# Проверка существования бюджетной статьи
# ======================================================================================================================
def _validate_budget_article(article_id, field_name):
    db = _get_db()
    sql = text("""
        SELECT
            id,
            name
        FROM tab_bud_st_d816_4
        WHERE id = :id
        LIMIT 1
    """)
    row = db.execute(
        sql,
        {
            'id': article_id
        }
    ).fetchone()
    if not row:
        raise ValueError(
            f'Бюджетная статья с id={article_id} '
            f'для поля {field_name} '
            f'не существует в tab_bud_st_d816_4'
        )
    return row
# ======================================================================================================================
# Проверка справочника
# ======================================================================================================================
def _validate_reference(
        value,
        field_name,
        table_name
    ):

    if value is None:
        return

    db = _get_db()

    sql = text(f"""
        SELECT
            id,
            name
        FROM {table_name}
        WHERE id = :id
        LIMIT 1
    """)

    row = db.execute(
        sql,
        {
            'id': value
        }
    ).fetchone()

    if not row:
        raise ValueError(
            f'Значение {value} для поля {field_name} '
            f'не найдено в справочнике {table_name}'
        )

    return row
# ======================================================================================================================
# Проверка всех ссылочных значений
# ======================================================================================================================
def _validate_korr_references(data):
    # Бюджетные статьи
    from_article = _validate_budget_article(
        data['from_bs_id'],
        'from_bs_id'
    )

    to_article = _validate_budget_article(
        data['to_bs_id'],
        'to_bs_id'
    )
    # Справочники
    _validate_reference(
        data.get('new_factory'),
        'new_factory',
        'tab_factory_d816_4'
    )

    _validate_reference(
        data.get('new_sobstv'),
        'new_sobstv',
        'tab_sobstv_d816_4'
    )

    _validate_reference(
        data.get('new_mest'),
        'new_mest',
        'tab_mest_d816_4'
    )

    _validate_reference(
        data.get('new_post'),
        'new_post',
        'tab_post_zuv_d816_4'
    )

    _validate_reference(
        data.get('type_raspr'),
        'type_raspr',
        'tab_type_raspr_d816_4'
    )

    return {
        'from_article': from_article,
        'to_article': to_article
    }
# ======================================================================================================================
# Проверка POST / PUT
# ======================================================================================================================
def _validate_korr_data(data):
    if not isinstance(data, dict):
        raise ValueError(
            'Тело запроса должно быть JSON'
        )

    required_fields = [
        'from_bs_id',
        'to_bs_id'
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

    result = {
        'from_bs_id': _to_int(
            data.get('from_bs_id'),
            'from_bs_id'
        ),

        'to_bs_id': _to_int(
            data.get('to_bs_id'),
            'to_bs_id'
        ),

        'new_factory': _to_optional_int(
            data.get('new_factory'),
            'new_factory'
        ),

        'new_sobstv': _to_optional_int(
            data.get('new_sobstv'),
            'new_sobstv'
        ),

        'new_mest': _to_optional_int(
            data.get('new_mest'),
            'new_mest'
        ),

        'new_post': _to_optional_int(
            data.get('new_post'),
            'new_post'
        ),

        'type_raspr': _to_optional_int(
            data.get('type_raspr'),
            'type_raspr'
        )
    }

    return result
# ======================================================================================================================
# 1. GET /korr/rows
# ======================================================================================================================
@ns_korr.route('/rows')
class KorrRows(Resource):
    def get(self):
        try:
            db = _get_db()

            sql = text("""
                SELECT
                    korr.id,

                    korr.from_bs_id,
                    from_article.name AS from_bs_name,

                    korr.to_bs_id,
                    to_article.name AS to_bs_name,

                    korr.new_factory,
                    factory.name AS factory_name,

                    korr.new_sobstv,
                    sobstv.name AS sobstv_name,

                    korr.new_mest,
                    mest.name AS mest_name,

                    korr.new_post,
                    post.name AS post_name,

                    korr.type_raspr,
                    type_raspr.name AS type_raspr_name

                FROM tab_korr_bs_d816_4 AS korr

                LEFT JOIN tab_bud_st_d816_4 AS from_article
                    ON from_article.id = korr.from_bs_id

                LEFT JOIN tab_bud_st_d816_4 AS to_article
                    ON to_article.id = korr.to_bs_id

                LEFT JOIN tab_factory_d816_4 AS factory
                    ON factory.id = korr.new_factory

                LEFT JOIN tab_sobstv_d816_4 AS sobstv
                    ON sobstv.id = korr.new_sobstv

                LEFT JOIN tab_mest_d816_4 AS mest
                    ON mest.id = korr.new_mest

                LEFT JOIN tab_post_zuv_d816_4 AS post
                    ON post.id = korr.new_post

                LEFT JOIN tab_type_raspr_d816_4 AS type_raspr
                    ON type_raspr.id = korr.type_raspr

                ORDER BY korr.id
            """)
            rows = db.execute(sql).fetchall()
            items = []
            for row in rows:
                items.append({
                    'id': row.id,

                    'from_bs': {
                        'id': row.from_bs_id,
                        'label': row.from_bs_name
                    } if row.from_bs_id is not None else None,

                    'to_bs': {
                        'id': row.to_bs_id,
                        'label': row.to_bs_name
                    } if row.to_bs_id is not None else None,

                    'factory': {
                        'id': row.new_factory,
                        'label': row.factory_name
                    } if row.new_factory is not None else None,

                    'owner': {
                        'id': row.new_sobstv,
                        'label': row.sobstv_name
                    } if row.new_sobstv is not None else None,

                    'field': {
                        'id': row.new_mest,
                        'label': row.mest_name
                    } if row.new_mest is not None else None,

                    'supplier': {
                        'id': row.new_post,
                        'label': row.post_name
                    } if row.new_post is not None else None,

                    'distribution_type': {
                        'id': row.type_raspr,
                        'label': row.type_raspr_name
                    } if row.type_raspr is not None else None
                })

            return {
                'items': items
            }, 200

        except Exception as e:
            ns_korr.abort(*errorhandler(e))

    @ns_korr.expect(korr_post_model, validate=True)
    def post(self):
        db = None

        try:
            data = request.get_json(
                force=True,
                silent=False
            )

            korr = _validate_korr_data(data)

            _validate_korr_references(korr)

            db = _get_db()

            next_id_sql = text("""
                    SELECT
                        COALESCE(MAX(id), 0) + 1 AS next_id
                    FROM tab_korr_bs_d816_4
                """)

            next_id_row = db.execute(
                next_id_sql
            ).fetchone()

            new_id = int(next_id_row.next_id)

            # проверяем id
            while True:

                check_sql = text("""
                        SELECT 1
                        FROM tab_korr_bs_d816_4
                        WHERE id = :id
                        LIMIT 1
                    """)

                exists = db.execute(
                    check_sql,
                    {
                        'id': new_id
                    }
                ).fetchone()

                if not exists:
                    break

                new_id += 1

            insert_sql = text("""
                    INSERT INTO tab_korr_bs_d816_4 (
                        id,
                        from_bs_id,
                        to_bs_id,
                        new_factory,
                        new_sobstv,
                        new_mest,
                        new_post,
                        type_raspr
                    )
                    VALUES (
                        :id,
                        :from_bs_id,
                        :to_bs_id,
                        :new_factory,
                        :new_sobstv,
                        :new_mest,
                        :new_post,
                        :type_raspr
                    )
                """)

            db.execute(
                insert_sql,
                {
                    'id': new_id,
                    'from_bs_id': korr['from_bs_id'],
                    'to_bs_id': korr['to_bs_id'],
                    'new_factory': korr['new_factory'],
                    'new_sobstv': korr['new_sobstv'],
                    'new_mest': korr['new_mest'],
                    'new_post': korr['new_post'],
                    'type_raspr': korr['type_raspr']
                }
            )

            db.commit()

            return {
                'message': 'Правило корректировки добавлено',
                'id': new_id,

                'from_bs_id': korr['from_bs_id'],
                'to_bs_id': korr['to_bs_id'],

                'new_factory': korr['new_factory'],
                'new_sobstv': korr['new_sobstv'],
                'new_mest': korr['new_mest'],
                'new_post': korr['new_post'],
                'type_raspr': korr['type_raspr']
            }, 201

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
                'message': 'Не удалось сохранить правило корректировки'
            }, 400
# ======================================================================================================================
# 3. PUT, DELETE /korr/rows/{id}
# ======================================================================================================================
@ns_korr.route('/rows/<int:id>')
class KorrRowsUpdate(Resource):

    @ns_korr.expect(korr_put_model, validate=True)
    def put(self, id):
        db = None

        try:
            data = request.get_json(
                force=True,
                silent=False
            )

            if not isinstance(data, dict):
                raise ValueError(
                    'Тело запроса должно быть JSON-объектом'
                )

            db = _get_db()

            # Проверяем существование записи
            select_sql = text("""
                SELECT
                    id,
                    from_bs_id,
                    to_bs_id,
                    new_factory,
                    new_sobstv,
                    new_mest,
                    new_post,
                    type_raspr
                FROM tab_korr_bs_d816_4
                WHERE id = :id
                LIMIT 1
            """)

            existing = db.execute(
                select_sql,
                {
                    'id': id
                }
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': (
                        f'Правило корректировки с id={id} не найдено'
                    )
                }, 404

            # Проверяем данные
            korr = _validate_korr_data(data)
            _validate_korr_references(korr)

            # UPDATE
            update_sql = text("""
                UPDATE tab_korr_bs_d816_4
                SET
                    from_bs_id = :from_bs_id,
                    to_bs_id = :to_bs_id,
                    new_factory = :new_factory,
                    new_sobstv = :new_sobstv,
                    new_mest = :new_mest,
                    new_post = :new_post,
                    type_raspr = :type_raspr
                WHERE id = :id
            """)

            result = db.execute(
                update_sql,
                {
                    'id': id,

                    'from_bs_id': korr['from_bs_id'],
                    'to_bs_id': korr['to_bs_id'],

                    'new_factory': korr['new_factory'],
                    'new_sobstv': korr['new_sobstv'],
                    'new_mest': korr['new_mest'],
                    'new_post': korr['new_post'],
                    'type_raspr': korr['type_raspr']
                }
            )

            if result.rowcount == 0:
                raise ValueError(
                    'Правило корректировки не было изменено'
                )

            db.commit()

            return {
                'message': 'Правило корректировки обновлено',
                'id': id,

                'from_bs_id': korr['from_bs_id'],
                'to_bs_id': korr['to_bs_id'],

                'new_factory': korr['new_factory'],
                'new_sobstv': korr['new_sobstv'],
                'new_mest': korr['new_mest'],
                'new_post': korr['new_post'],
                'type_raspr': korr['type_raspr']
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
                'message': 'Не удалось изменить правило корректировки'
            }, 400
    def delete(self, id):
        db = None

        try:
            db = _get_db()

            check_sql = text("""
                SELECT
                    id
                FROM tab_korr_bs_d816_4
                WHERE id = :id
                LIMIT 1
            """)

            existing = db.execute(
                check_sql,
                {
                    'id': id
                }
            ).fetchone()

            if not existing:
                return {
                    'code': 'validation_error',
                    'message': (
                        f'Правило корректировки с id={id} не найдено'
                    )
                }, 404
            # DELETE
            delete_sql = text("""
                DELETE FROM tab_korr_bs_d816_4
                WHERE id = :id
            """)

            result = db.execute(
                delete_sql,
                {
                    'id': id
                }
            )

            if result.rowcount == 0:
                raise ValueError(
                    'Правило корректировки не было удалено'
                )

            db.commit()

            return {
                'message': 'Правило корректировки удалено',
                'id': id
            }, 200

        except Exception as e:

            try:
                if db:
                    db.rollback()
            except Exception:
                pass

            if isinstance(e, ValueError):
                return {
                    'code': 'validation_error',
                    'message': str(e)
                }, 400

            return {
                'code': 'validation_error',
                'message': 'Не удалось удалить правило корректировки'
            }, 400

