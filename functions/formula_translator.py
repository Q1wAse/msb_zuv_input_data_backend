import re

CUSTOM_OVERRIDES = {
    # "ТЕСТ_ФУНКЦИЯ": "INDEX",
    # "ТЕСТ_МАТЧ": "MATCH"
}

OFFICIAL_EXCEL_MAP = {
    # Поиск и ссылки
    "ИНДЕКС": "INDEX", "ПОИСКПОЗ": "MATCH", "ВПР": "VLOOKUP", "ГПР": "HLOOKUP",
    "ПРОСМОТР": "LOOKUP", "ВЫБОР": "CHOOSE", "АДРЕС": "ADDRESS", "ДВССЫЛ": "INDIRECT",
    "СМЕЩ": "OFFSET", "СТРОКА": "ROW", "СТОЛБЕЦ": "COLUMN", "ЧСТРОК": "ROWS", "ЧСТОЛБЕЦ": "COLUMNS",

    # Логические и условия
    "ЕСЛИ": "IF", "И": "AND", "ИЛИ": "OR", "НЕ": "NOT", "ИСТИНА": "TRUE", "ЛОЖЬ": "FALSE",
    "ЕСЛИОШИБКА": "IFERROR", "ЕСЛИМН": "IFS", "ПЕРЕКЛЮЧ": "SWITCH",

    # Математические и агрегатные
    "СУММ": "SUM", "СУММЕСЛИ": "SUMIF", "СУММЕСЛИМН": "SUMIFS", "СУММПРОИЗВ": "SUMPRODUCT",
    "ОКРУГЛ": "ROUND", "ОКРУГЛВВЕРХ": "ROUNDUP", "ОКРУГЛВНИЗ": "ROUNDDOWN", "ОТБР": "TRUNC",
    "ЦЕЛОЕ": "INT", "ОСТАТ": "MOD", "ABS": "ABS", "КОРЕНЬ": "SQRT", "СТЕПЕНЬ": "POWER",

    # Статистические
    "СРЗНАЧ": "AVERAGE", "СРЗНАЧЕСЛИ": "AVERAGEIF", "СРЗНАЧЕСЛИМН": "AVERAGEIFS",
    "СЧЁТ": "COUNT", "СЧЕТ": "COUNT", "СЧЁТЗ": "COUNTA", "СЧЕТЗ": "COUNTA",
    "СЧЁТЕСЛИ": "COUNTIF", "СЧЕТЕСЛИ": "COUNTIF", "СЧЁТЕСЛИМН": "COUNTIFS", "СЧЕТЕСЛИМН": "COUNTIFS",
    "МАКС": "MAX", "МИН": "MIN", "НАИБОЛЬШИЙ": "LARGE", "НАИМЕНЬШИЙ": "SMALL",

    # Текстовые
    "СЦЕПИТЬ": "CONCATENATE", "СЦЕП": "CONCAT", "ОБЪЕДИНИТЬ": "TEXTJOIN",
    "ЛЕВСИМВ": "LEFT", "ПРАВСИМВ": "RIGHT", "ПСТР": "MID", "ДЛСТР": "LEN",
    "НАЙТИ": "FIND", "ПОИСК": "SEARCH", "ЗАМЕНИТЬ": "REPLACE", "ПОДСТАВИТЬ": "SUBSTITUTE",
    "СЖПРОБЕЛЫ": "TRIM", "ПРОПИСН": "UPPER", "СТРОЧН": "LOWER", "ПРОПНАЧ": "PROPER",
    "ТЕКСТ": "TEXT", "ЗНАЧЕН": "VALUE",

    # Дата и время
    "ДАТА": "DATE", "ВРЕМЯ": "TIME", "ГОД": "YEAR", "МЕСЯЦ": "MONTH", "ДЕНЬ": "DAY",
    "ЧАС": "HOUR", "МИНУТЫ": "MINUTE", "СЕКУНДЫ": "SECOND", "СЕГОДНЯ": "TODAY", "ТДАТА": "NOW",
    "ДЕНЬНЕД": "WEEKDAY", "НОМНЕДЕЛИ": "WEEKNUM", "ДОЛЯГОДА": "YEARFRAC",

    # Информационные
    "ЕПУСТО": "ISBLANK", "ЕОШИБКА": "ISERROR", "ЕОШ": "ISERR", "ЕНД": "ISNA",
    "ЕЧИСЛО": "ISNUMBER", "ЕТЕКСТ": "ISTEXT", "ТИП": "TYPE"
}


def get_combined_mapping():
    mapping = OFFICIAL_EXCEL_MAP.copy()

    for ru, en in CUSTOM_OVERRIDES.items():
        mapping[ru.upper()] = en.upper()

    return mapping

def convert_russian_formula(russian_formula_str):
    formula = russian_formula_str.strip().upper()

    mapping = get_combined_mapping()

    ru_words = set(re.findall(r'\b[А-ЯЁ0-9_.]+\b', formula))

    for ru_word in ru_words:
        if ru_word in mapping:
            en_name = mapping[ru_word]

            # Список функций Excel 2010+, требующих префикс для стабильности в Excel 2013
            new_functions = ["IFERROR", "IFS", "XLOOKUP", "TEXTJOIN", "CONCAT"]
            if en_name in new_functions:
                en_name = f"_xlfn.{en_name}"

            formula = re.sub(rf'\b{ru_word}\b', en_name, formula)

    # Замена с русской формулы точки запятой на запятую для английской
    formula = formula.replace(";", ",")

    if not formula.startswith("="):
        formula = f"={formula}"

    return formula
