from flask import request
from flask_restx import Namespace, Resource, fields
from sqlalchemy import text

from msb_zuv_input_data_backend.database import errorhandler
import msb_zuv_input_data_backend.functions.utility_functions as uf

ns_bud_st = Namespace(
    'bud_st',
    description='Справочник бюджетных статей tab_bud_st_d816_4'
)
# ======================================================================================================================
# Модель
# ======================================================================================================================
budget_article_model = ns_bud_st.model(
    'BudgetArticle',
    {
        'id': fields.Integer(
            description='ID бюджетной статьи'
        ),
        'name': fields.String(
            description='Наименование бюджетной статьи'
        )
    }
)
budget_article_response_model = ns_bud_st.model(
    'BudgetArticleResponse',
    {
        'items': fields.List(
            fields.Nested(budget_article_model),
            description='Список бюджетных статей'
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
            f'Параметр {field_name} не может быть пустым'
        )
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f'Параметр {field_name} должен быть числом'
        )
# ======================================================================================================================
# GET /bud_st
# ======================================================================================================================

@ns_bud_st.route('')
class BudgetArticles(Resource):

    @ns_bud_st.doc(
        params={
            'article_id': {
                'description': (
                    'ID бюджетной статьи. '
                    'Если параметр не передан — возвращаются первые 100 записей.'
                ),
                'required': False,
                'type': 'integer'
            }
        }
    )
    @ns_bud_st.marshal_with(budget_article_response_model)
    def get(self):
        """
        Получить справочник бюджетных статей.

        Без article_id:
            возвращаются первые 100 записей.

        С article_id:
            возвращается статья с указанным ID.
        """

        try:
            db = _get_db()

            article_id = request.args.get('article_id')

            # ==========================================================================================================
            # 1. article_id НЕ передан
            # ==========================================================================================================

            if article_id is None or article_id == '':

                sql = text("""
                    SELECT
                        id,
                        name
                    FROM tab_bud_st_d816_4
                    ORDER BY id
                    LIMIT 100
                """)

                rows = db.execute(sql).fetchall()

            # ==========================================================================================================
            # 2. article_id передан
            # ==========================================================================================================

            else:

                article_id = _to_int(
                    article_id,
                    'article_id'
                )

                sql = text("""
                    SELECT
                        id,
                        name
                    FROM tab_bud_st_d816_4
                    WHERE id = :article_id
                    ORDER BY id
                """)

                rows = db.execute(
                    sql,
                    {
                        'article_id': article_id
                    }
                ).fetchall()

            items = [
                {
                    'id': row.id,
                    'name': row.name
                }
                for row in rows
            ]

            return {
                'items': items
            }, 200

        except ValueError as e:

            return {
                'code': 'validation_error',
                'message': str(e)
            }, 400

        except Exception as e:

            return {
                'code': 'validation_error',
                'message': 'Не удалось получить справочник бюджетных статей'
            }, 400