from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from flask_caching import Cache

from msb_zuv_input_data_backend.config import Config
#===================================================================================================================

if Config.SERVERBASE_MODE == 'PYTHON':
    Base = declarative_base()
    engine_py = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_size=30)
    Base.metadata.create_all(bind=engine_py)
    db_py = scoped_session(sessionmaker(bind=engine_py))
else:
    engine_py = None
    db_py = None

cache = Cache()

def errorhandler(e):
    if str(e) == "max() arg - это пустая последовательность":
        return 502, "Нет данных для указанных условий"
    elif isinstance(e, ValueError):
        return 501, f"Неверный ввод: {str(e)}"
    elif isinstance(e, KeyError):
        return 504, f"Ключ не найден: {str(e)}"
    elif isinstance(e, PermissionError):
        return 503, f"Доступ запрещен: {str(e)}"
    else:
        return 500, f"Произошла непредвиденная ошибка: {str(e)}"