import re

formulas = [
    # 'СУММ(2_100335008:2_100335130)/$1000',
    # '{100330001:0}-{100330002:0}',
    # 'ОКРУГЛ({100331977:-1}/{100331977:-1}*$100;$6)',
    # 'ЕСЛИОШИБКА(ОКРУГЛ({100331977:-1}/{100331977:-1}*$100;$6);"-")',
    # 'ROUND({100331977:-1}/{100331977:-1}*$100;$6)',
    # 'Round({100331977:-1}/{100331977:-1}*$100;$6)',
    # '100332166+1_100331977',
    # '1_100335806+1_100331985+1_100331986',
    # 'ОКРУГЛ({100330001:-1}/{100330002:-1}/$1000;$6)+{100330003:0}/{100330004:0}+100332166-1_100331977',
    # 'AB26-Z6+AA12/$1000*$50+$10000-F1',
    # '+',
    # 'ОКРУГЛ({100335769:1}/{100330738:1}*{100330738:0};$1)',
    # '=ОКРУГЛ({100335772:2}/{100335771:2}*{S34:0};$1)',
    # '=$1+100330001-1_100330001+F6+{S34:0}/{100033002:2}'
    '=ОКРУГЛ({100335769:2}/{100330738:2}*{100330738:0};$1)+S34+1000330001'
    # 'СУММ(100332297:100335487)-100335049'
]
# formulas = [
#     '{S34:0}'
# ]
def get_parts_formula(formula):

    # pattern = r'(\{-?[\d_]+:-?[\d_]+\}|[А-Яа-яA-Za-z_]+\(|\$?[\d_]+|[^0-9_${}])'
    # parts = re.findall(pattern, formula)
    # parts = [p for p in parts if p]
    # pattern = r'(\{-?[\d_]+:-?[\d_]+\}|[А-Яа-яA-Za-z_]+\(|\$?[A-Za-z0-9_]+|[^0-9_${}])'
    # parts = re.findall(pattern, formula)
    # parts = [p for p in parts if p]

    # pattern = r'(\{-?[A-Za-z0-9_]+:-?[\d_]+\}|[А-Яа-яA-Za-z_]+\(|\$?[A-Za-z0-9_]+|[^0-9_${}])'
    # pattern = r'^\{(-?[\d_]+):(-?[\d_]+)\}$'

    pattern = r'(?:\$[0-9]+)|(?:[a-zA-Zа-яА-ЯёЁ_][a-zA-Zа-яА-ЯёЁ0-9_]*)|(?:\{[^}]+\})|(?:[0-9]+:[0-9]+)|(?:[0-9]+)|(?:[;(),+\-*/])'

    parts = re.findall(pattern, formula)
    parts = [p for p in parts if p]

    if parts:
        return parts
        # if any(re.match(r'^\$?[A-Za-z]{1,3}\$?\d+$', p) for p in parts):
        #     return parts
        # return 'no pattern'
    return formula

# for i, formula in enumerate(formulas):
#     print(f'{i}: {get_parts_formula(formula)}')

def test_formula(formula):
    pattern = re.compile(
        r'(?P<CELL_OFFSET>\{[^}]+\})'
        r'|(?P<ESCAPED>\$[0-9]+)'
        r'|(?P<FUNC_OR_VAR>[a-zA-Zа-яА-ЯёЁ0-9_]+)'
        r'|(?P<OPERATOR>[;(),+\-*/:])'
        r'|(?P<NUMBER>[0-9]+)'
    )
    print(formula)
    parts = []
    new_fromula = ''
    for match in pattern.finditer(formula):
        token_type = match.lastgroup
        value = match.group()
        parts.append(value)

        # {A1:2}, {1000330001:0} - обработка шаблона
        if token_type == 'CELL_OFFSET':
            q = 0
            # print(f'CELL_OFFSET {value}')
        # A1 - ссылка на ячейку или 1_1000330001 - ключ статьи со ссылкой на лист
        elif token_type == 'FUNC_OR_VAR':
            end_index = match.end()
            next_char = formula[end_index] if end_index < len(formula) else ''
            # Проверяем, если попало имя функции, то просто передаёт его дальше без обработки
            if next_char == '(':
                q = 0
                # print(f'FUNC_OR_VAR FUNC: {value}')
            else:
                q = 0
                # print(f'FUNC_OR_VAR VAR: {value}')

        # экранирование для $
        elif token_type == 'ESCAPED':
            q = 0
            # print(f'escape: {value}')

        # неизвестный элемент
        else:
            q = 0
            # print(f'? {value}')
        new_fromula = f'{new_fromula}{value}'
    print(parts)
    print(new_fromula)
    print(formula==new_fromula)
    return 0

# for i, formula in enumerate(formulas):
#     test_formula(formula)
generating_report_settings = {
# ключ в "Таблица Типы отчётов" | index - индекс именованного диапазона
    '1': {'index': '1'}, # План общий
    '2': {'index': '4'}, # Факт общий
    '3': {'index': '2'}, # Баланс ЗС
    '5': {'index': '3'}, # ЕЖО
}

reports_all_list = [value.get('index') for key, value in generating_report_settings.items()]
print(reports_all_list)