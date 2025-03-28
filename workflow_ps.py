from typing import Dict, TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
# from langchain_google_community import (
#     VertexAISearchRetriever,
# )
import prompts_ps
# import settings
# from google.cloud import bigquery
import ps_functions
import utils
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


class AgentState(TypedDict):
    question: str
    database_schemas: str
    query: str
    max_num_retries_debug: int
    num_retries_debug_sql: int
    result_debug_sql: str
    error_msg_debug_sql: str
    df: pd.DataFrame
    visualization_request: str
    python_code_data_visualization: str
    python_code_store_variables_dict: dict
    num_retries_debug_python_code_data_visualization: int
    result_debug_python_code_data_visualization: str
    error_msg_debug_python_code_data_visualization: str


llm = ChatGroq(model="llama3-70b-8192", temperature=0.3)
max_characters_error_msg_debug = 300

db_config = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin',
    'port': 5432
}


def get_postgres_schemas_and_tables(db_config):
    """
    Busca os esquemas e tabelas no PostgreSQL.

    Args:
        db_config: Dicionário com os parâmetros de conexão ao PostgreSQL.

    Returns:
        Uma string formatada com os esquemas e tabelas disponíveis.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Consulta para obter esquemas e tabelas
        query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema');
        """
        cursor.execute(query)
        tables = cursor.fetchall()

        # Formatar os resultados
        schemas_and_tables = []
        for schema, table in tables:
            query_columns = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}';
            """
            cursor.execute(query_columns)
            columns = cursor.fetchall()
            column_details = ", ".join([f"{col[0]} ({col[1]})" for col in columns])            
            schemas_and_tables.append(f"{schema}.{table}: {column_details}")


        return "\n".join(schemas_and_tables)

    except Exception as e:
        print(f"Erro ao buscar esquemas e tabelas: {e}")
        return ""
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def search_tables_and_schemas(state: AgentState) -> AgentState:
    print("Buscando tabelas, esquemas e colunas no PostgreSQL...")
    schemas_and_tables = get_postgres_schemas_and_tables(db_config)
    if schemas_and_tables:
        print("Esquemas, tabelas e colunas encontrados:")
        print(schemas_and_tables)
        state["database_schemas"] = schemas_and_tables
    else:
        print("Nenhuma tabela, esquema ou coluna encontrada.")
        state["database_schemas"] = "Nenhuma tabela, esquema ou coluna disponível."

    return state


def agent_sql_writer_node(state: AgentState) -> AgentState:

    prompt_template = ChatPromptTemplate(("system", prompts_ps.system_prompt_agent_sql_writer))

    chain = prompt_template | llm

    response = chain.invoke({"question": state["question"], 
                             "database_schemas": state["database_schemas"]}).content
    state["query"] = utils.extract_code_block(content=response,language="sql")
    print(f"### Agent SQL Writer query:\n {state["query"]}")
    return state



def agent_sql_validator_node(state: AgentState) -> AgentState:
    print("\n\n### Validating query:")
    
    try:
        query = state["query"]
        print(f"Query gerada: {query}")

        # Conectar ao PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Executar a consulta em modo dry-run (simulação)
        cursor.execute(f"EXPLAIN {query}")
        print("Consulta validada com sucesso.")

        # Executar a consulta e armazenar os resultados
        cursor.execute(query)
        rows = cursor.fetchall()
        state["df"] = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

        state["result_debug_sql"] = "Pass"
        state["error_msg_debug_sql"] = ""
        print(f"Resultado: {state['result_debug_sql']}")

        return state

    except Exception as e:
        state["num_retries_debug_sql"] += 1
        state["result_debug_sql"] = "Not Pass"
        state["error_msg_debug_sql"] = str(e)[:max_characters_error_msg_debug]
        print(f"Resultado: {state['result_debug_sql']}")
        print(f"Mensagem de erro: {state['error_msg_debug_sql']}")

        # Tentar corrigir a consulta
        print("\n### Tentando corrigir a consulta:")
        prompt_template = ChatPromptTemplate(("system", prompts_ps.system_prompt_agent_sql_validator_node))

        chain = prompt_template | llm

        response = chain.invoke({
            "query": state["query"], 
            "error_msg_debug": state["error_msg_debug_sql"]
        }).content

        state["query"] = utils.extract_code_block(content=response, language="sql")
        print(f"\n### Consulta ajustada:\n {state['query']}")

        return state

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def agent_bi_expert_node(state: AgentState) -> AgentState:
    
    prompt_template = ChatPromptTemplate(("system", prompts_ps.system_prompt_agent_bi_expert_node))

    chain = prompt_template | llm

    response = chain.invoke({"question": state["question"],
                             "query": state["query"],
                             "df_structure": state["df"].dtypes,
                             "df_sample": state["df"].head(5)
                             }).content

    state["visualization_request"] = response
    print(f"\n### Visualization Request:\n {state["visualization_request"]}")

    return state


def agent_python_code_data_visualization_generator_node(state: AgentState) -> AgentState:

    prompt_template = ChatPromptTemplate(("system", prompts_ps.system_prompt_agent_python_code_data_visualization_generator_node))

    chain = prompt_template | llm

    response = chain.invoke({"visualization_request": state["visualization_request"],
                             "df_structure": state["df"].dtypes,
                             "df_sample": state["df"].head(5)
                             }).content
    state["python_code_data_visualization"] = utils.extract_code_block(content=response,language="python")

    print(f"\n### Data visualization code:\n {state["python_code_data_visualization"]}")

    return state


def agent_python_code_data_visualization_validator_node(state: AgentState) -> AgentState:    

    print("\n\n### Validating data visualization code:")
    
    try:
        df = state["df"]
        # Create a dictionary to store the executed variables for the python code generated
        exec_globals = {"df": df}
        exec(state["python_code_data_visualization"], exec_globals)
        state["python_code_store_variables_dict"] = exec_globals
        state["result_debug_python_code_data_visualization"] = "Pass"
        state["error_msg_debug_python_code_data_visualization"] = ""
        print(f"result: {state["result_debug_python_code_data_visualization"]}")

        return state
        
    except Exception as e:
        state["num_retries_debug_python_code_data_visualization"] += 1

        # return False, f"Error validating query: {str(e)}"
        state["result_debug_python_code_data_visualization"] = "Not Pass"
        state["error_msg_debug_python_code_data_visualization"] = str(e)[0:max_characters_error_msg_debug]
        print(f"result: {state["result_debug_python_code_data_visualization"]}")
        print(f'error message: {state["error_msg_debug_python_code_data_visualization"]}')

        #trying to fix the query
        print("\n### Trying to fix the plotly code:")
        prompt_template = ChatPromptTemplate(("system", prompts_ps.system_prompt_agent_python_code_data_visualization_validator_node))

        chain = prompt_template | llm


        response = chain.invoke({"python_code_data_visualization": state["python_code_data_visualization"], 
                                "error_msg_debug": state["error_msg_debug_python_code_data_visualization"]}).content

        state["python_code_data_visualization"] = utils.extract_code_block(content=response,language="python")

        print(f"\n### Plotly code adjusted:\n {state["python_code_data_visualization"]}")

        return state


workflow = StateGraph(state_schema=AgentState)


workflow.add_node("search_tables_and_schemas",search_tables_and_schemas)
workflow.add_node("agent_sql_writer_node",agent_sql_writer_node)
workflow.add_node("agent_sql_validator_node",agent_sql_validator_node)
workflow.add_node("agent_bi_expert_node",agent_bi_expert_node)
workflow.add_node("agent_python_code_data_visualization_generator_node",agent_python_code_data_visualization_generator_node)
workflow.add_node("agent_python_code_data_visualization_validator_node",agent_python_code_data_visualization_validator_node)


workflow.add_edge("search_tables_and_schemas","agent_sql_writer_node")
workflow.add_edge("agent_sql_writer_node","agent_sql_validator_node")

workflow.add_conditional_edges(
    'agent_sql_validator_node',
    lambda state: 'agent_bi_expert_node' 
    if state['result_debug_sql']=="Pass" or state['num_retries_debug_sql'] >= state['max_num_retries_debug'] 
    else 'agent_sql_validator_node',
    {'agent_bi_expert_node': 'agent_bi_expert_node','agent_sql_validator_node': 'agent_sql_validator_node'}
)
workflow.add_edge("agent_bi_expert_node","agent_python_code_data_visualization_generator_node")
workflow.add_edge("agent_python_code_data_visualization_generator_node","agent_python_code_data_visualization_validator_node")

workflow.add_conditional_edges(
    'agent_python_code_data_visualization_validator_node',
    lambda state: "end" 
    if state['result_debug_python_code_data_visualization']=="Pass" or state['num_retries_debug_python_code_data_visualization'] >= state['max_num_retries_debug'] 
    else 'agent_python_code_data_visualization_validator_node',
    {'end': END,'agent_python_code_data_visualization_validator_node': 'agent_python_code_data_visualization_validator_node'}
)


workflow.set_entry_point("search_tables_and_schemas")

app = workflow.compile()

### Run workflow

def run_workflow(question: str) -> dict:
    initial_state = AgentState(
        question = question,
        database_schemas = "",
        query = "",
        num_retries_debug_sql = 0,
        max_num_retries_debug = 3,
        result_debug_sql = "",
        error_msg_debug_sql = "",
        df = pd.DataFrame(),
        visualization_request = "",
        python_code_data_visualization = "",
        python_code_store_variables_dict = {},
        num_retries_debug_python_code_data_visualization = 0,
        result_debug_python_code_data_visualization = "",
        error_msg_debug_python_code_data_visualization = ""
    )
    final_state = app.invoke(initial_state)
    return final_state

# state = run_workflow(question = "Quantidade de filmes lançados em 2020 na netflix?")
# print(state)
# state = run_workflow(question = "How many movies were released in 2020 in netflix?") 
# state = run_workflow(question = "What are the 3 video game platforms more sold in the history?") 