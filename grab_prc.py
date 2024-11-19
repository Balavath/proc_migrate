import pyodbc
import re
import pandas as pd
import logging

# Настраиваем логирование
logging.basicConfig(filename='process_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Чтение списка серверов и баз данных из файла Excel
input_file = 'connections.xlsx'
connections_df = pd.read_excel(input_file)

# Преобразуем строки подключения в список словарей
connections = connections_df.to_dict('records')

logging.info("Загружены подключения из файла Excel.")
print("Загружены подключения:", connections)

# Функция для извлечения объектов из текста процедуры
def extract_objects(procedure, text):
    pattern = (
        r'\b(?:FROM|JOIN|INTO|UPDATE|DELETE FROM|DROP TABLE)\s+'  # Ключевые слова SQL
        r'(?:\[(\w+)\]\.|(\w+)\.)?'  # Опционально: база данных
        r'(?:\[(\w+)\]\.|(\w+)\.)?'  # Опционально: схема
        r'(#?\[?(\w+)\]?)'  # Имя объекта
    )
    matches = re.findall(pattern, text, re.IGNORECASE)
    logging.info(f"Найдено {len(matches)} объектов в тексте процедуры {procedure}.")
    print(f"Найдено {len(matches)} объектов в тексте процедуры {procedure}.")

    objects = []
    for match in matches:
        db = match[0] or match[1]
        schema = match[2] or match[3]
        obj_name = match[4]
        db = db.strip('[]') if db else None
        schema = schema.strip('[]') if schema else None
        obj_name = obj_name.strip('[]') if obj_name else None
        full_object_name = '.'.join(part for part in [db, schema, obj_name] if part)
        if obj_name or schema:
            objects.append({
                'database': db,
                'schema': schema,
                'object_name': obj_name,
                'full_object_name': full_object_name
            })
    return objects

# Список для хранения результатов
all_results = []

# Проходим по каждому подключению
for conn_info in connections:
    server = conn_info.get('server')
    database = conn_info.get('database')

    logging.info(f"Начало обработки сервера {server}, базы данных {database}.")
    print(f"Обработка сервера {server}, базы данных {database}...")

    try:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        conn = pyodbc.connect(connection_string)

        # Запросы
        procedure_query = """
        SELECT p.name AS procedure_name, 
               s.name AS schema_name, 
               db_name() AS database_name,
               m.definition AS procedure_text
        FROM sys.procedures p
        JOIN sys.sql_modules m ON p.object_id = m.object_id
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        """
        jobs_query = """
        SELECT j.name AS job_name,
               js.step_id,
               js.step_name,
               js.command
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
        WHERE js.subsystem = 'TSQL' AND js.command LIKE '%EXEC%'
        """

        # Выполняем запросы
        procedures = pd.read_sql(procedure_query, conn)
        jobs = pd.read_sql(jobs_query, conn)
        conn.close()

        logging.info(f"Извлечено {len(procedures)} процедур и {len(jobs)} шагов джобов.")

        for _, row in procedures.iterrows():
            procedure_name = row['procedure_name']
            procedure_text = row['procedure_text']
            procedure_db = row['database_name']
            procedure_schema = row['schema_name']
            objects = extract_objects(procedure_name, procedure_text)
            relevant_jobs = jobs[jobs['command'].str.contains(procedure_name, case=False, regex=False)]

            # Добавляем записи
            if relevant_jobs.empty:
                for obj in objects:
                    all_results.append({
                        'Название Джоба': None,
                        'Номер шага джоба': None,
                        'Название шага джоба': None,
                        'БД процедуры': procedure_db,
                        'Процедура': procedure_name,
                        'Таблица\\вьюха\\процедура': obj['full_object_name'],
                        'БД объекта': obj['database'],
                        'Схема объекта': obj['schema'],
                        'Таблица\\вьюха\\процедура без БД': obj['object_name'],
                        'Сервер': server,
                        'Новый сервер': None,
                        'Новая БД процедуры': None,
                        'Новая БД объекта': None,
                        'Новая схема': None,
                        'Новое имя объекта'	: None,
                        'Перенесено': None        
                    })

            for obj in objects:
                for _, job_row in relevant_jobs.iterrows():
                    all_results.append({
                        'Название Джоба': job_row['job_name'],
                        'Номер шага джоба': job_row['step_id'],
                        'Название шага джоба': job_row['step_name'],
                        'БД процедуры': procedure_db,
                        'Процедура': procedure_name,
                        'Таблица\\вьюха\\процедура': obj['full_object_name'],
                        'БД объекта': obj['database'],
                        'Схема объекта': obj['schema'],
                        'Таблица\\вьюха\\процедура без БД': obj['object_name'],
                        'Сервер': server,
                        'Новый сервер': None,
                        'Новая БД процедуры': None,
                        'Новая БД объекта': None,
                        'Новая схема': None,
                        'Новое имя объекта'	: None,
                        'Перенесено': None 
                    })

    except Exception as e:
        logging.error(f"Ошибка при обработке сервера {server}, базы {database}: {e}")
        print(f"Ошибка при обработке сервера {server}, базы {database}: {e}")

# Конвертируем результаты в DataFrame
df_results = pd.DataFrame(all_results)

# Удаляем дубликаты и сохраняем в Excel
output_file = 'procedures_objects_with_jobs.xlsx'
df_results.drop_duplicates(inplace=True)
df_results.to_excel(output_file, index=False)

logging.info(f"Результаты сохранены в файл {output_file}.")
print(f"Результаты сохранены в файл {output_file}.")
input("Нажмите Enter, чтобы завершить...")
