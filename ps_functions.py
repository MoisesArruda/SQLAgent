import pandas as pd
import psycopg2
import csv
import json

def csv_to_postgres(db_config, table_name, csv_path, schema_path, truncate_table=True):
    """
    Upload CSV data to PostgreSQL with schema validation.

    Args:
        db_config: Dictionary with PostgreSQL connection parameters (host, dbname, user, password, port).
        table_name: PostgreSQL table name.
        csv_path: Path to CSV file.
        schema_path: Path to JSON schema file.
        truncate_table: Whether to truncate the table before inserting data.
    """
    # Load schema from JSON file
    try:
        print(f"Carregando o esquema do arquivo JSON: {schema_path}")
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_json = json.load(f)
        print("Esquema JSON carregado com sucesso.")
    except UnicodeDecodeError as e:
        print(f"Erro de codificação ao carregar o arquivo JSON: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Erro de formatação no arquivo JSON: {e}")
        return

    try:
        print("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Conexão com o banco de dados bem-sucedida.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return

    try:
        print(f"Criando a tabela '{table_name}' no banco de dados...")
        create_table_query = generate_create_table_query(table_name, schema_json)
        print(f"Query de criação da tabela: {create_table_query}")
        cursor.execute(create_table_query)
        print(f"Tabela '{table_name}' criada ou já existente.")
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")
        conn.close()
        return

    if truncate_table:
        try:
            print(f"Truncando a tabela '{table_name}'...")
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            print(f"Tabela '{table_name}' truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela: {e}")
            conn.close()
            return

    try:
        print(f"Lendo o arquivo CSV: {csv_path}")
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            headers = next(reader)  # Read the header row
            print(f"Headers do CSV: {headers}")
            insert_query = generate_insert_query(table_name, headers)
            print(f"Query de inserção: {insert_query}")

            for row in reader:
                # Substituir strings vazias por None (equivalente a NULL no PostgreSQL)
                row = [None if value == "" else value for value in row]
                cursor.execute(insert_query, row)
        print(f"Dados do arquivo CSV '{csv_path}' inseridos com sucesso na tabela '{table_name}'.")
    except Exception as e:
        print(f"Erro ao inserir dados do CSV: {e}")
        conn.close()
        return

    try:
        conn.commit()
        print("Transação confirmada.")
    except Exception as e:
        print(f"Erro ao confirmar a transação: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")


def generate_create_table_query(table_name, schema_json):
    """
    Generate a CREATE TABLE query based on the schema JSON.

    Args:
        table_name: PostgreSQL table name.
        schema_json: JSON schema defining the table structure.

    Returns:
        CREATE TABLE SQL query as a string.
    """
    columns = []
    for field in schema_json:
        column_name = f'"{field["name"]}"'  # Escapar o nome da coluna com aspas duplas
        column_type = map_json_type_to_postgres(field['type'])
        column_mode = 'NOT NULL' if field.get('mode', 'NULLABLE') == 'REQUIRED' else ''
        columns.append(f"{column_name} {column_type} {column_mode}")

    columns_sql = ', '.join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"

def map_json_type_to_postgres(json_type):
    """
    Map JSON schema types to PostgreSQL types.

    Args:
        json_type: JSON schema type.

    Returns:
        Corresponding PostgreSQL type as a string.
    """
    type_mapping = {
        'STRING': 'TEXT',
        'INTEGER': 'INTEGER',
        'FLOAT': 'REAL',
        'BOOLEAN': 'BOOLEAN',
        'DATETIME': 'TIMESTAMP',
    }
    postgres_type = type_mapping.get(json_type.upper(), 'TEXT')  # Default to TEXT
    print(f"Mapeando tipo '{json_type}' para '{postgres_type}'")
    return postgres_type

def generate_insert_query(table_name, headers):
    """
    Generate an INSERT query for the given table and headers.

    Args:
        table_name: PostgreSQL table name.
        headers: List of column names.

    Returns:
        INSERT SQL query as a string.
    """
    columns = ', '.join([f'"{header}"' for header in headers])  # Escapar os nomes das colunas
    placeholders = ', '.join(['%s'] * len(headers))
    return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

def test_postgres_connection(db_config):
    """
    Testa a conexão com o banco de dados PostgreSQL.

    Args:
        db_config: Dicionário com os parâmetros de conexão (host, dbname, user, password, port).
    """
    try:
        print("Tentando conectar com os seguintes parâmetros:")
        for key, value in db_config.items():
            print(f"{key}: {value}")
        conn = psycopg2.connect(**db_config, options="-c client_encoding=UTF8")
        print("Conexão com o banco de dados bem-sucedida!")
        conn.close()
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")

if __name__ == "__main__":

    # csv_path="datasets/tables_descriptions/tables_descriptions.csv"
    # df = pd.read_csv(csv_path)
    # print(df)

    db_config = {
    'host': "localhost",
    'dbname': "postgres",
    'user': "postgres",
    'password': "admin",
    'port': 5432,
}

    test_postgres_connection(db_config)

    # tables_description = csv_to_postgres(
    #     db_config=db_config,
    #     table_name="tables_descriptions",
    #     csv_path="datasets/tables_descriptions/tables_descriptions.csv", 
    #     schema_path="datasets/tables_descriptions/schema.json",
    #     # truncate_table=True
    # )

    
    hotel_bookings = csv_to_postgres(db_config=db_config,
        table_name="hotel_bookings", 
                                csv_path="datasets/hotel_bookings/hotel_bookings.csv", 
                                schema_path="datasets/hotel_bookings/schema.json")

    csv_path="datasets/hotel_bookings/hotel_bookings.csv"
    df = pd.read_csv(csv_path)
    print(df)


    # upload supermarket_sales dataset
    # https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales
    # df = pd.read_csv("datasets/supermarket_sales/supermarket_sales.csv")

    # supermaket_sales = csv_to_postgres(db_config=db_config,
    #     table_name="supermarket_sales", 
    #                             csv_path="datasets/supermarket_sales/supermarket_sales.csv", 
    #                             schema_path="datasets/supermarket_sales/schema.json")


    # # upload netflix_movies_and_tv_shows dataset
    # # https://www.kaggle.com/datasets/shivamb/netflix-shows
    # # df = pd.read_csv("datasets/netflix_movies_and_tv_shows/netflix_movies_and_tv_shows.csv")

    # netflix = csv_to_postgres(db_config=db_config,
    #     table_name="netflix_movies_and_tv_shows", 
    #                             csv_path="datasets/netflix_movies_and_tv_shows/netflix_movies_and_tv_shows.csv", 
    #                             schema_path="datasets/netflix_movies_and_tv_shows/schema.json")


    # # upload video_games_sales dataset
    # # https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
    # # df = pd.read_csv("datasets/video_games_sales/video_games_sales.csv")

    # games_sales = csv_to_postgres(db_config=db_config,
    #     table_name="video_games_sales", 
    #                             csv_path="datasets/video_games_sales/video_games_sales.csv", 
    #                             schema_path="datasets/video_games_sales/schema.json")

