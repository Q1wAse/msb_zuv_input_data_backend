class Config:
    # SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://IS_KAO:Gamma12345%25Beta6789%23@kao-dev-db01.codm.gazprom.loc:5433/ISKAO_DATA'
    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:postgres@localhost:5433/msb_zuv_input_data'

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JSON_SORT_KEYS = False
    CACHE_TYPE = 'simple'
    CACHE_DEFAULT_TIMEOUT = 300  # seconds
    ALLOWED_EXTENSIONS = {'xlsx, txt'}
    DATABASE_MODE = 'DIRECT'
    AUTHORIZATION_MODE = 'DIRECT'
    SERVERBASE_MODE = 'PYTHON' # PYTHON OR WSGI depending on server environment
    PORT = '5433'

    TITLE = '"Материально-стоимостной баланс ЖУВ". Ввод данных API'
    VERSION = '1.0',

secret_key = 'supersecretkey'

format_strings = ["0.0f", "0.1f", "0.2f", "0.3f", "0.4f"]


changelog = """
"""