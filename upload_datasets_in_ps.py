import pandas as pd
import ps_functions
import psycopg2

db_config = {
'host': "localhost",
'dbname': "postgres",
'user': "postgres",
'password': "admin",
'port': 5432,
}

ps_functions.test_postgres_connection(db_config)


# #tables_description = 
# ps_functions.csv_to_postgres(
#     db_config=db_config,
#     table_name="tables_descriptions",
#     csv_path="datasets/tables_descriptions/tables_descriptions.csv", 
#     schema_path="datasets/tables_descriptions/schema.json",
#     # truncate_table=True
# )


# csv_path="datasets/hotel_bookings/hotel_bookings.csv"
# df = pd.read_csv(csv_path)
# print(df)

# #hotel_bookings = 
# ps_functions.csv_to_postgres(db_config=db_config,
#     table_name="hotel_bookings", 
#                             csv_path="datasets/hotel_bookings/hotel_bookings.csv", 
#                             schema_path="datasets/hotel_bookings/schema.json")


# # upload supermarket_sales dataset
# # https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales
# df = pd.read_csv("datasets/supermarket_sales/supermarket_sales.csv")
# print(df)

# #supermaket_sales = 
# ps_functions.csv_to_postgres(db_config=db_config,
#     table_name="supermarket_sales", 
#                             csv_path="datasets/supermarket_sales/supermarket_sales.csv", 
#                             schema_path="datasets/supermarket_sales/schema.json")


# # # upload netflix_movies_and_tv_shows dataset
# # https://www.kaggle.com/datasets/shivamb/netflix-shows
# df = pd.read_csv("datasets/netflix_movies_and_tv_shows/netflix_movies_and_tv_shows.csv")
# print(df)

# #netflix = 
# ps_functions.csv_to_postgres(db_config=db_config,
#     table_name="netflix_movies_and_tv_shows", 
#                             csv_path="datasets/netflix_movies_and_tv_shows/netflix_movies_and_tv_shows.csv", 
#                             schema_path="datasets/netflix_movies_and_tv_shows/schema.json")


# # # upload video_games_sales dataset
# # # https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
# df = pd.read_csv("datasets/video_games_sales/video_games_sales.csv")
# print(df)

# #games_sales = 
# ps_functions.csv_to_postgres(db_config=db_config,
#     table_name="video_games_sales", 
#                             csv_path="datasets/video_games_sales/video_games_sales.csv", 
#                             schema_path="datasets/video_games_sales/schema.json")


def compare_csv_and_postgres(db_config, table_name, csv_path):
    """
    Compara as colunas e a quantidade de linhas entre um arquivo CSV e uma tabela no PostgreSQL.

    Args:
        db_config: Dicionário com os parâmetros de conexão ao PostgreSQL.
        table_name: Nome da tabela no PostgreSQL.
        csv_path: Caminho para o arquivo CSV.

    Returns:
        None
    """
    # Ler o arquivo CSV
    print(f"Lendo o arquivo CSV: {csv_path}")
    csv_data = pd.read_csv(csv_path)
    csv_columns = list(csv_data.columns)
    csv_row_count = len(csv_data)
    print(f"Colunas no CSV: {csv_columns}")
    print(f"Quantidade de linhas no CSV: {csv_row_count}")

    # Conectar ao PostgreSQL
    try:
        print("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Obter as colunas da tabela no PostgreSQL
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        postgres_columns = [row[0] for row in cursor.fetchall()]
        print(f"Colunas na tabela '{table_name}' no PostgreSQL: {postgres_columns}")

        # Obter a quantidade de linhas na tabela no PostgreSQL
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        postgres_row_count = cursor.fetchone()[0]
        print(f"Quantidade de linhas na tabela '{table_name}' no PostgreSQL: {postgres_row_count}")

        # Comparar colunas
        missing_in_csv = set(postgres_columns) - set(csv_columns)
        missing_in_postgres = set(csv_columns) - set(postgres_columns)

        print("\nComparação de colunas:")
        if missing_in_csv:
            print(f"Colunas presentes no PostgreSQL, mas ausentes no CSV: {missing_in_csv}")
        else:
            print("Todas as colunas do PostgreSQL estão presentes no CSV.")

        if missing_in_postgres:
            print(f"Colunas presentes no CSV, mas ausentes no PostgreSQL: {missing_in_postgres}")
        else:
            print("Todas as colunas do CSV estão presentes no PostgreSQL.")

        # Comparar quantidade de linhas
        print("\nComparação de quantidade de linhas:")
        if csv_row_count == postgres_row_count:
            print("A quantidade de linhas no CSV e no PostgreSQL é igual.")
        else:
            print(f"Diferença na quantidade de linhas: CSV ({csv_row_count}) vs PostgreSQL ({postgres_row_count})")

    except Exception as e:
        print(f"Erro ao conectar ao banco de dados ou executar a consulta: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":

    # Configuração do banco de dados PostgreSQL
    db_config = {
        'host': 'localhost',
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin',
        'port': 5432
    }

    compare_csv_and_postgres(
    db_config=db_config,
    table_name="hotel_bookings",
    csv_path="datasets/hotel_bookings/hotel_bookings.csv"
    )

    # Comparar o CSV com a tabela no PostgreSQL
    compare_csv_and_postgres(
    db_config=db_config,
    table_name="video_games_sales",
    csv_path="datasets/video_games_sales/video_games_sales.csv"
)

    compare_csv_and_postgres(
    db_config=db_config,
    table_name="netflix_movies_and_tv_shows",
    csv_path="datasets/netflix_movies_and_tv_shows/netflix_movies_and_tv_shows.csv"
)

    compare_csv_and_postgres(
    db_config=db_config,
    table_name="supermarket_sales",
    csv_path="datasets/supermarket_sales/supermarket_sales.csv"
)

    compare_csv_and_postgres(
    db_config=db_config,
    table_name="tables_descriptions",
    csv_path="datasets/tables_descriptions/tables_descriptions.csv"
)