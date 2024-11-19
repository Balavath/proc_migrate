import pyodbc
import pandas as pd
import os
import re
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
import logging
import sys



need_prc_confirmation = True
need_tbl_confirmation = True   
level_log=logging.INFO

for arg in sys.argv[0:]:
    if arg == '-no_prc_confirm':
        need_prc_confirmation = False
    if arg == '-no_table_confirm':
        need_tbl_confirmation = False
    if arg == '-only_errors':
        level_log = logging.ERROR

logging.basicConfig(
    filename='migrate.log',
    filemode='a',
    level=level_log,
    format='%(asctime)s - %(levelname)s %(message)s'
)
logging.error(f"Установлен уровень логирования {level_log}")
renamed_views = {}

def fetch_procedure_code(connection_string, procedure_name, server, db_name):
    """Получает текст процедуры из базы данных."""
    print(f"Получение кода процедуры {server}.{db_name}.{procedure_name}.")
    query = f"""
    SELECT OBJECT_DEFINITION(OBJECT_ID('{procedure_name}'))
    """
    
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
    return result[0] if result else None

def procedure_exists(connection_string, procedure_name, schema="dbo"):
    """
    Проверяет, существует ли хранимая процедура на сервере.
    """
    query = f"""
    SELECT 1
    FROM sys.procedures 
    WHERE name = '{procedure_name}' AND schema_id = SCHEMA_ID('{schema}');
    """
    
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
    return result is not None  # True, если процедура существует

def save_code_to_file(procedure_name, code, folder, suffix):
    """Сохраняет код процедуры в файл для дальнейшей проверки."""
    if not os.path.exists(folder):
        os.makedirs(folder)
        
    file_path = os.path.join(folder, f"{procedure_name}_{suffix}.sql")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(code)

    print(f"Код процедуры {procedure_name} сохранен в файл {file_path}.")
    logging.info(f"Код процедуры {procedure_name} сохранен в файл {file_path}.")    
    return file_path

def modify_procedure_code_temp(procedure_code, old_server, new_server, old_prc_db, old_obj_db, new_db_name, new_schema, proc_name, temp_table_prefix, object_name, log_dict, new_object_name=None):
    """
    Модифицирует код хранимой процедуры, обновляя имена временных таблиц везде: в коде, комментариях, строках.
    """
    print(f"Модификация кода процедуры {proc_name}.")
    logging.info(f"Модификация кода процедуры {proc_name}.")
    
    def log_table_replacement(old_table, new_table, temp_table_prefix):
        if old_table in log_dict:
            log_dict[old_table]['new_name'] = new_table
            log_dict[old_table]['count'] += 1
            if temp_table_prefix:
                log_dict[old_table]['type'] = 'temp'
            else:
                log_dict[old_table]['type'] = 'reg'
        else:
            log_dict[old_table] = {'new_name': new_table, 'count': 1}
            if temp_table_prefix:
                log_dict[old_table]['type'] = 'temp'
            else:
                log_dict[old_table]['type'] = 'reg'

    # Для временных таблиц добавляем префикс, для регулярных объектов просто заменяем
    new_table_name = f"{new_db_name}.{new_schema}.{temp_table_prefix}_{proc_name}_{object_name.strip('#')}"

    print(f"Пробуем заменить {old_obj_db}.{new_schema}.{object_name} на {new_table_name}")
    logging.info(f"Пробуем заменить {old_obj_db}.{new_schema}.{object_name} на {new_table_name}")

    # Регулярное выражение для поиска объекта
    object_pattern = rf"(?:\b([\w]*\.[\w]*\.)?|(\b[\w]*\.\.?)?)?{re.escape(object_name)}\b"

    def replace_object(match):
        # Получаем части до имени объекта (db и schema, если есть)
        old_prefix = match.group(1) if match.group(1) else match.group(2)
        old_name = old_prefix + object_name if old_prefix else object_name
        # Логируем старое имя объекта
        if old_prefix:
            log_table_replacement(old_name, new_table_name, temp_table_prefix)
        else:
            log_table_replacement(object_name, new_table_name, temp_table_prefix)

        return new_table_name

    # Применяем замену по всему коду
    modified_code = re.sub(object_pattern, replace_object, procedure_code, flags=re.DOTALL | re.MULTILINE | re.IGNORECASE)

    print(f"Код процедуры {proc_name} успешно модифицирован.")
    logging.info(f"Код процедуры {proc_name} успешно модифицирован.")

    return modified_code

def modify_procedure_code(procedure_code, old_server, new_server, old_prc_db, old_obj_db, new_db_name, new_schema, proc_name, temp_table_prefix, object_name, log_dict, new_object_name=None):
    """
    Модифицирует код хранимой процедуры, обновляя имена временных таблиц или обычных объектов, везде: в коде, комментариях, строках.
    """
    print(f"Модификация кода процедуры {proc_name}.")
    logging.info(f"Модификация кода процедуры {proc_name}.")

    def log_table_replacement(old_table, new_table, temp_table_prefix):
        if old_table in log_dict:
            log_dict[old_table]['new_name'] = new_table
            log_dict[old_table]['count'] += 1
            if temp_table_prefix:
                log_dict[old_table]['type'] = 'temp'
            else:
                log_dict[old_table]['type'] = 'reg'
        else:
            log_dict[old_table] = {'new_name': new_table, 'count': 1}
            if temp_table_prefix:
                log_dict[old_table]['type'] = 'temp'
            else:
                log_dict[old_table]['type'] = 'reg'

    # Для временных таблиц добавляем префикс, для регулярных объектов просто заменяем
    new_table_name = f"{new_db_name}.{new_schema}.{new_object_name or object_name}"
    allow_to_rename = handle_regular_table(old_server, old_obj_db, new_server, new_db_name, new_schema, object_name, new_object_name, 'dbo')
    print(f"Пробуем заменить {old_obj_db}.{new_schema}.{object_name} на {new_table_name}")
    logging.info(f"Пробуем заменить {old_obj_db}.{new_schema}.{object_name} на {new_table_name}")

    # Регулярное выражение для поиска с учетом квадратных скобок
    object_pattern = (
        rf"(\[?(\w+)\]?\.?\.)?"  # Опционально база данных (с квадратными скобками или без)
        rf"(\[?(\w+)\]?\.)?"  # Опционально схема (с квадратными скобками или без)  
        rf"\b\[?({re.escape(object_name)})\]?\b"  # Имя объекта с квадратными скобками или без
    )
    if allow_to_rename:
        def replace_object(match):
            """
            Заменяем найденный объект на новый, учитывая наличие квадратных скобок у блоков.
            """
            # Определяем, были ли квадратные скобки у каждого блока, проверяем на None перед проверкой на "[".
            db_part = f"[{new_db_name}]" if match.group(1) and "[" in match.group(1) else new_db_name
            schema_part = f"[{new_schema}]" if match.group(2) and "[" in match.group(2) else new_schema
            object_part = f"[{new_object_name or object_name}]" if match.group(3) and "[" in match.group(3) else new_object_name or object_name
            # Исправляем: если любая часть обрамлена в скобки, все части должны быть обрамлены
            if "[" in match.group(0):  # Если исходная строка содержит скобки
                db_part = f"[{new_db_name}]"
                schema_part = f"[{new_schema}]"
                object_part = f"[{new_object_name or object_name}]"

            # Собираем новое имя объекта
            new_name = f"{db_part}.{schema_part}.{object_part}"

            # Логируем старое и новое имя
            old_name = match.group(0)

            log_table_replacement(old_name, new_name, temp_table_prefix)

            return new_name

        # Применяем замену по всему коду
        modified_code = re.sub(object_pattern, replace_object, procedure_code, flags=re.IGNORECASE)
        modified_code = modified_code.replace(']]', ']').replace('[[', '[')

        print(f"Код процедуры {proc_name} успешно модифицирован.")
        logging.info(f"Код процедуры {proc_name} успешно модифицирован.")
        return modified_code
    else:
        return procedure_code

def check_object_exists(connection_string, new_db, new_schema, new_table_name):
    """
    Проверяет, существует ли объект на новом сервере и в новой базе данных.
    """
    print(f"Проверяем, существует ли {new_table_name}")
    logging.info(f"Проверяем, существует ли {new_table_name}")
    object_type_map = {
        'T': 'U',  # Regular table
        'TD': 'U',  # Regular table     
        't': 'U',  # Regular table   
        'td': 'U',  # Regular table
        'V': 'V',  # View
        'v': 'V',  # View
        'F': 'FN', # Function
        'P': 'P',  # Procedure
        'PRC': 'P',  # Procedure
        'TR': 'TR' # Trigger
    }
    
    for prefix, obj_type in object_type_map.items():
        if new_table_name.startswith(prefix):
            check_query = f"""
            IF OBJECT_ID('{new_db}.{new_schema}.{new_table_name}', '{obj_type}') IS NOT NULL
            SELECT 1 AS table_exists
            ELSE
            SELECT 0 AS table_exists
            """
            break
    else:
        return False, None  # Если ни один тип не совпал

    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(check_query)
        result = cursor.fetchone()
    
    return result[0] == 1, prefix  # Возвращаем True, если таблица существует

def get_table_structure_keys_and_indexes(connection_string, table_name, schema): 
    """
    Получает структуру таблицы, информацию о первичных и уникальных ключах, а также индексах с исходного сервера.
    Также проверяет наличие внешних ключей и генерирует запросы для их добавления в новую таблицу.
    """
    print(f"Получаем структуру таблицы {table_name}")
    logging.info(f"Получаем структуру таблицы {table_name}")
    
    # Запрос для получения структуры таблицы
    structure_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'
    """
    
    # Запрос для получения первичных ключей
    primary_keys_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}' AND CONSTRAINT_NAME LIKE 'PK_%';
    """
    
    # Запрос для получения уникальных ключей
    unique_keys_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}' AND CONSTRAINT_NAME LIKE 'UQ_%';
    """

    # Запрос для получения индексов
    index_query = f"""
    SELECT i.name AS index_name, 
           c.name AS column_name,
           i.is_unique,
           i.is_primary_key
    FROM sys.indexes AS i
    INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID('{schema}.{table_name}')
    ORDER BY i.name, ic.key_ordinal;
    """
    
    # Запрос для получения информации о внешних ключах
    foreign_keys_query = f"""
    SELECT fk.name AS foreign_key_name,
           tp.name AS referenced_table_name,
           cp.name AS referenced_column_name,
           c.name as parent_column_name
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.tables AS tp ON fkc.referenced_object_id = tp.object_id
    INNER JOIN sys.columns AS cp ON fkc.referenced_object_id = cp.object_id AND fkc.referenced_column_id = cp.column_id
    INNER JOIN sys.columns AS c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
    WHERE fk.parent_object_id = OBJECT_ID('{schema}.{table_name}');
    """
    # Запрос для получения информации о default-значениях
    default_query = f"""
    SELECT col.name AS column_name, def.definition AS default_value
    FROM sys.default_constraints def
    INNER JOIN sys.columns col ON def.parent_object_id = col.object_id AND def.parent_column_id = col.column_id
    WHERE col.object_id = OBJECT_ID('{schema}.{table_name}')
    """

    with pyodbc.connect(connection_string) as conn:
        df_structure = pd.read_sql(structure_query, conn)
        df_primary_keys = pd.read_sql(primary_keys_query, conn)
        df_unique_keys = pd.read_sql(unique_keys_query, conn)
        df_indexes = pd.read_sql(index_query, conn)
        df_foreign_keys = pd.read_sql(foreign_keys_query, conn)
        df_defaults = pd.read_sql(default_query, conn)

    # Добавляем информацию о первичных и уникальных ключах в структуру
    df_structure['PRIMARY_KEY'] = df_structure['COLUMN_NAME'].isin(df_primary_keys['COLUMN_NAME'])
    df_structure['UNIQUE_KEY'] = df_structure['COLUMN_NAME'].isin(df_unique_keys['COLUMN_NAME'])

    return df_structure, df_indexes, df_foreign_keys, df_defaults

def create_table_on_new_server(connection_string, new_db, new_schema, new_table_name, table_structure, index_structure, foreign_key_structure, df_defaults):
    """
    Создает новую таблицу на новом сервере и в новой БД, используя структуру исходной таблицы.
    """
    print(f"Создаем {new_db}.{new_schema}.{new_table_name}.")
    logging.info(f"Создаем {new_db}.{new_schema}.{new_table_name}.")
    create_query = f"CREATE TABLE {new_db}.{new_schema}.{new_table_name} ("
    
    columns = []
    primary_keys = []
    unique_keys = []
    foreign_key_queries = []
    
    for _, row in table_structure.iterrows():
        if pd.isna(row['COLUMN_NAME']) or not row['COLUMN_NAME']:
            continue
        
        column_def = f"{row['COLUMN_NAME']} "
        
        # Проверка для nvarchar(-1)
        if row['DATA_TYPE'] == 'nvarchar' and row['CHARACTER_MAXIMUM_LENGTH'] == -1:
            column_def += 'nvarchar(max)'
        else:
            column_def += f"{row['DATA_TYPE']}"
            if row['CHARACTER_MAXIMUM_LENGTH'] and not pd.isna(row['CHARACTER_MAXIMUM_LENGTH']):
                column_def += f"({int(row['CHARACTER_MAXIMUM_LENGTH'])})"
        
        if row['IS_NULLABLE'] == 'NO':
            column_def += " NOT NULL"
        
        # Добавление default значений
        default_value = df_defaults.loc[df_defaults['column_name'] == row['COLUMN_NAME'], 'default_value']
        if not default_value.empty:
            column_def += f" DEFAULT {default_value.values[0]}"

        columns.append(column_def)

        # Сбор информации о первичных и уникальных ключах
        if row['PRIMARY_KEY']:
            primary_keys.append(row['COLUMN_NAME'])
        if row['UNIQUE_KEY']:
            unique_keys.append(row['COLUMN_NAME'])

    # Проверка наличия колонок перед добавлением их в запрос
    if columns:
        create_query += ", ".join(columns)
        
        # Добавление первичных ключей, если они есть
        if primary_keys:
            create_query += f", CONSTRAINT PK_{new_table_name} PRIMARY KEY ({', '.join(primary_keys)})"
        
        # Добавление уникальных ключей, если они есть
        if unique_keys:
            create_query += f", CONSTRAINT UQ_{new_table_name} UNIQUE ({', '.join(unique_keys)})"
    else:
        print("Ошибка: Не указаны колонки для создания таблицы.")
        logging.error("Ошибка: Не указаны колонки для создания таблицы.")
        return

    create_query += ");"
    if need_tbl_confirmation:
        confirmation = input("Вы уверены, что хотите создать эту таблицу? (да/нет): ")
        if confirmation.lower() != 'да':
            print("Создание таблицы отменено.")
            logging.info("Создание таблицы отменено.")
            return
    
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(create_query)
            print(f"Таблица {new_db}.{new_schema}.{new_table_name} успешно создана.")
            logging.info(f"Таблица {new_db}.{new_schema}.{new_table_name} успешно создана.")
        except Exception as e:
            print(f"Ошибка при создании таблицы: {e}")
            logging.error(f"Ошибка при создании таблицы: {e}")
        conn.commit()
    
    # Создание индексов, проверка существования
    for _, index_row in index_structure.iterrows():
        # Проверка на то, что индекс не является автоматически созданным для PRIMARY KEY
        if index_row['is_primary_key']:
            continue  # Пропускаем создание индекса, если он связан с первичным ключом
        
        index_create_query = f"CREATE {'UNIQUE ' if index_row['is_unique'] else ''}INDEX {index_row['index_name']} ON {new_db}.{new_schema}.{new_table_name} ({index_row['column_name']});"
        try:
            cursor.execute(index_create_query)       
            print("Индексы успешно созданы.")
            logging.info("Индексы успешно созданы.")
        except pyodbc.ProgrammingError as e:
            print(f"Ошибка при добавлении индекса: {e}")
            logging.error(f"Ошибка при добавлении индекса: {e}")

    for fk in foreign_key_structure.itertuples(index=False):
        referenced_table = fk.referenced_table_name
        referenced_column = fk.referenced_column_name
        foreign_key_name = fk.foreign_key_name
        parent_column = fk.parent_column_name

        # Генерируем запрос для добавления внешнего ключа
        query = f"""
        ALTER TABLE {new_db}.{new_schema}.{new_table_name}
        ADD CONSTRAINT {foreign_key_name} 
        FOREIGN KEY ({parent_column})
        REFERENCES {referenced_table}({referenced_column});
        """
        foreign_key_queries.append(query)

    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        for query in foreign_key_queries:
            try:
                cursor.execute(query)
                print(f"Внешний ключ добавлен: {query}")
                logging.info(f"Внешний ключ добавлен: {query}")
            except Exception as e:
                print(f"Ошибка при добавлении внешнего ключа: {e}")
                logging.error(f"Ошибка при добавлении внешнего ключа: {e}")
        conn.commit()

def handle_regular_table(old_server, old_db, new_server, new_db, new_schema, old_table_name, new_table_name, schema_old):
    """
    Обрабатывает регулярную таблицу: проверяет существование, переименовывает или создает при необходимости.
    """
    # Строка подключения для старого и нового серверов
    connection_string_old = f"Driver={{SQL Server}};Server={old_server};Database={old_db};Trusted_Connection=yes;"
    connection_string_new = f"Driver={{SQL Server}};Server={new_server};Database={new_db};Trusted_Connection=yes;"

    # Проверяем, существует ли таблица на новом сервере
    object_exists, prefix = check_object_exists(connection_string_new, new_db, new_schema, new_table_name)
    if object_exists:
        if prefix in ['T','t','TD','td']:
            print(f"Таблица {new_db}.{new_schema}.{new_table_name} уже существует на сервере {new_server}. Переименовываем...")
            logging.info(f"Таблица {new_db}.{new_schema}.{new_table_name} уже существует на сервере {new_server}. Переименовываем...")
        elif prefix in ['V','v']:
            print(f"Витрина {new_db}.{new_schema}.{new_table_name} уже существует на сервере {new_server}. Переименовываем...")
            logging.info(f"Витрина {new_db}.{new_schema}.{new_table_name} уже существует на сервере {new_server}. Переименовываем...")
            renamed_views[f"{old_server}.{old_db}.{schema_old}.{old_table_name}"] = f"{new_server}.{new_db}.{new_schema}.{new_table_name}"
            print(f"Добавили запись: Заменили {old_server}.{old_db}.{schema_old}.{old_table_name} на {new_server}.{new_db}.{new_schema}.{new_table_name} в список витрин")
            logging.info(f"Добавили запись: Заменили {old_server}.{old_db}.{schema_old}.{old_table_name} на {new_server}.{new_db}.{new_schema}.{new_table_name} в список витрин")
        else:
            print(f"Данный тип объекта {new_db}.{new_schema}.{new_table_name} запрещено переименовывать.")
            logging.info(f"Данный тип объекта {new_db}.{new_schema}.{new_table_name} запрещено переименовывать.")
            exit
    else:
        if prefix in ['T','t','TD','td']:
            print(f"Таблица {new_db}.{new_schema}.{new_table_name} не найдена на сервере {new_server}. Создаем новую...")
            logging.info(f"Таблица {new_db}.{new_schema}.{new_table_name} не найдена на сервере {new_server}. Создаем новую...")
            # Получаем структуру таблицы с исходного сервера
            table_structure, index_structure, foreign_key_structure, defaults = get_table_structure_keys_and_indexes(connection_string_old, old_table_name, schema_old)
            # Создаем таблицу на новом сервере с аналогичной структурой
            create_table_on_new_server(connection_string_new, new_db, new_schema, new_table_name, table_structure, index_structure, foreign_key_structure, defaults)
        elif prefix in ['V','v']:
            print(f"Витрина {new_db}.{new_schema}.{new_table_name} не найдена на сервере {new_server}. Создание запрещено, переименовываем...")
            logging.info(f"Витрина {new_db}.{new_schema}.{new_table_name} не найдена на сервере {new_server}. Создание запрещено, переименовываем...")
            # Добавить список витрин в отдельный список было-стало
        else:
            print(f"Данный тип объекта {new_db}.{new_schema}.{new_table_name} запрещено создавать и переименовывать.")
            logging.info(f"Данный тип объекта {new_db}.{new_schema}.{new_table_name} запрещено создавать и переименовывать.")
            exit

    return True

def append_drop_statements(procedure_code, log_dict):
    """Добавляет команды DROP TABLE только для временных таблиц перед последним END в текущей процедуре."""
    drop_statements = set()  # Используем множество для уникальности

    # Генерация DROP TABLE команд только для временных таблиц
    for old_table_name, data in log_dict.items():
        new_table_name = data['new_name']
        table_type = data['type']
        # Проверка, что это временная таблица (например, начинается с 'T_TMP')
        if table_type == 'temp':
            drop_statement = f"IF OBJECT_ID('{new_table_name}') IS NOT NULL DROP TABLE {new_table_name};\n"
            drop_statements.add(drop_statement)

    if drop_statements:
        drop_code = ''.join(drop_statements)

        # Поиск последнего вхождения END
        last_end_pos = list(re.finditer(r'\bEND\b', procedure_code, re.IGNORECASE))[-1]
        # Добавляем DROP TABLE перед последним END
        procedure_code = procedure_code[:last_end_pos.start()] + drop_code + procedure_code[last_end_pos.start():]


    print(f"Команды DROP TABLE добавлены для {len(drop_statements)} временных таблиц.")
    logging.info(f"Команды DROP TABLE добавлены для {len(drop_statements)} временных таблиц.")
    return procedure_code

def create_procedure_in_db(connection_string, procedure_name, procedure_code):
    """Создает процедуру в новой базе данных."""
    print(f"Создание процедуры {procedure_name} в новой базе данных.")
    logging.info(f"Создание процедуры {procedure_name} в новой базе данных.")
    create_query = f"{procedure_code}"

    if procedure_exists(connection_string, procedure_name):
            print(f"Процедура {procedure_name} уже существует. Сохранение невозможно")
            logging.error(f"Процедура {procedure_name} уже существует. Сохранение невозможно")
            return

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(create_query)
            conn.commit()
        print(f"Процедура {procedure_name} успешно создана в базе данных.")
        logging.info(f"Процедура {procedure_name} успешно создана в базе данных.")
    except pyodbc.Error as e:
        # Обработка ошибок подключения и создания процедуры
        error_message = str(e)
        print(f"Ошибка при создании процедуры {procedure_name}: {error_message}")
        logging.error(f"Ошибка при создании процедуры {procedure_name}: {error_message}")
    except Exception as e:
        general_error_message = str(e)
        print(f"Неизвестная ошибка при создании процедуры {procedure_name}: {general_error_message}")
        logging.error(f"Неизвестная ошибка при создании процедуры {procedure_name}: {general_error_message}")

def append_log_to_procedure_code(procedure_code, log_dict):
    """
    Добавляет комментарии с логом изменений, временем и именем пользователя в конец процедуры.
    """
    # Получаем имя пользователя и текущее время
    username = os.getlogin()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Формируем комментарии с логом изменений
    log_comments = "\n-- Изменения в процедуре:\n"
    for old_name, data in log_dict.items():
        new_name = data['new_name']
        count = data['count']
        table_type = data['type']
        log_comments += f"-- Замена: {table_type} {old_name} на {new_name}, количество замен: {count}\n"

    # Добавляем время сохранения и пользователя
    log_comments += f"-- Время сохранения: {current_time}\n"
    log_comments += f"-- Пользователь: {username}\n"

    # Добавляем комментарии в конец процедуры
    modified_code_with_log = procedure_code + "\n" + log_comments
    return modified_code_with_log

def save_renamed_views_to_excel(renamed_views, file_name="renamed_views.xlsx"):
    # Преобразуем словарь в DataFrame
    df = pd.DataFrame(list(renamed_views.items()), columns=['Old Name', 'New Name'])
    
    # Сохраняем в Excel
    df.to_excel(file_name, index=False)
    print(f"Переименованные витрины сохранены в файл {file_name}.")
    logging.info(f"Переименованные витрины сохранены в файл {file_name}.")

def process_procedures_from_excel(file_path, replace_temptable=True, replace_objects=True):
    """
    Проходит по всем процедурам из Excel-файла, группирует их по процедурам,
    и применяет изменения, создавая новые процедуры на указанном пользователем сервере и базе данных.
    """
    print(f"Чтение Excel-файла: {file_path}.")
    logging.info(f"Чтение Excel-файла: {file_path}.")
    df = pd.read_excel(file_path, sheet_name=None)

    log_dict = {}

    for sheet_name, df_sheet in df.items():
        # Группируем по серверу, базе данных и процедуре
        grouped = df_sheet[df_sheet['Перенесено'] == 0].groupby(['Сервер', 'БД процедуры', 'Процедура'])

        for (server, db_name, procedure_name), group in grouped:
            print(f"Обработка процедуры: {procedure_name} на сервере: {server}, БД: {db_name}")
            logging.info(f"Обработка процедуры: {procedure_name} на сервере: {server}, БД: {db_name}")
            
            # Строка подключения
            connection_string = f"Driver={{SQL Server}};Server={server};Database={db_name};Trusted_Connection=yes;"
            
            # Получаем исходный код процедуры
            original_code = fetch_procedure_code(connection_string, procedure_name, server, db_name)
            
            if original_code:
                # Сохраняем исходный код
                save_code_to_file(procedure_name, original_code, 'procedure_backup', 'original')

                # Модифицируем код для временных таблиц, если разрешено
                if replace_temptable:
                    for index, row in group.iterrows():
                        object_name = row['Таблица\\вьюха\\процедура без БД']
                        old_server = row['Сервер']
                        new_server = row['Новый сервер']
                        old_prc_db = row['БД процедуры']
                        new_prc_db = row['Новая БД процедуры']
                        old_obj_db = row['БД объекта']
                        new_obj_db = row['Новая БД объекта']
                        new_schema = row['Новая схема']
                        new_object_name = row['Новое имя объекта']
                        if object_name.startswith("#"):
                            #def procedure_code, old_server, new_server, old_prc_db, old_obj_db, new_db_name, new_schema, proc_name, temp_table_prefix, object_name, log_dict, new_object_name=None):
                            original_code = modify_procedure_code_temp(
                                original_code, old_server, new_server, old_prc_db, old_obj_db, new_obj_db, new_schema, procedure_name, 'T_TMP', object_name, log_dict, ''
                            )

                # Модифицируем код для обычных объектов, если разрешено
                if replace_objects:
                    for index, row in group.iterrows():
                        object_name = row['Таблица\\вьюха\\процедура без БД']
                        old_server = row['Сервер']
                        new_server = row['Новый сервер']
                        old_prc_db = row['БД процедуры']
                        new_prc_db = row['Новая БД процедуры']
                        old_obj_db = row['БД объекта']
                        new_obj_db = row['Новая БД объекта']
                        new_schema = row['Новая схема']
                        new_object_name = row['Новое имя объекта']
                        if not object_name.startswith("#"):
                            original_code = modify_procedure_code(
                                original_code, old_server, new_server, old_prc_db, old_obj_db, new_obj_db, new_schema, procedure_name, '', object_name, log_dict, new_object_name
                            )

                # Добавляем команды DROP TABLE для временных таблиц
                if replace_temptable:
                    print(f"Добавление команд DROP TABLE для временных таблиц в процедуру {procedure_name}")
                    logging.info(f"Добавление команд DROP TABLE для временных таблиц в процедуру {procedure_name}")
                    original_code = append_drop_statements(original_code, log_dict)

                # Добавляем лог и комментарии
                original_code = append_log_to_procedure_code(original_code, log_dict)
                save_code_to_file(procedure_name, original_code, 'procedure_backup', 'with_log')

                # Подтверждение создания новой процедуры
                if need_prc_confirmation: 
                    confirm = input(f"Вы уверены, что хотите создать новую процедуру {procedure_name} в базе {new_prc_db}? (да/нет): ")
                    if confirm.lower() == 'да':
                        create_procedure_in_db(f"Driver={{SQL Server}};Server={new_server};Database={new_prc_db};Trusted_Connection=yes;", procedure_name, original_code)
                        # Логируем изменение в Excel
                        df_sheet.loc[group.index, 'Перенесено'] = 1
                else: 
                    create_procedure_in_db(f"Driver={{SQL Server}};Server={new_server};Database={new_prc_db};Trusted_Connection=yes;", procedure_name, original_code)
                    # Логируем изменение в Excel
                    df_sheet.loc[group.index, 'Перенесено'] = 1

    # Сохраняем изменения в Excel-файле
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
            for sheet_name, df_sheet in df.items():
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
    except PermissionError as e:
        error_message = str(e)
        print(f"Ошибка при сохранении изменений в файл {file_path}: {error_message}")
        logging.error(f"Ошибка при сохранении изменений в файл {file_path}: {error_message}")
    save_renamed_views_to_excel(renamed_views)
    print("Все процедуры обработаны и сохранены.")
    logging.info("Все процедуры обработаны и сохранены.")
    
def select_file():
    # Создаем корневое окно, но скрываем его (нам нужно только диалоговое окно)
    root = tk.Tk()
    root.withdraw()  # Скрываем основное окно

    # Открываем диалог выбора файла и сохраняем путь к файлу
    file_path = filedialog.askopenfilename(
        title="Выберите файл",
        filetypes=(("Файлы Excel", "*.xlsx"), ("Все файлы", "*.*"))
    )

    if file_path:
        print(f"Выбран файл: {file_path}")
        process_procedures_from_excel(file_path)
    else:
        print("Файл не выбран")
if __name__ == "__main__":
    select_file()
    input("Нажмите Enter, чтобы завершить...")