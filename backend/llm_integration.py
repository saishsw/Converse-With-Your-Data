import logging
import os
from dotenv import load_dotenv
import openai

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
verde_url = os.getenv("verde_api_url")
verde_api_key = os.getenv("verde_api_key")

client = openai.OpenAI(
    base_url= verde_url, 
    api_key = verde_api_key
)

def llm_to_sql(schema_information, unstructured_language_query):
    table_name = schema_information.get("table_name", "")
    columns = schema_information.get("column_names", [])

    schema_string = f"Table: {table_name}\nColumns:\n"
    for col in columns:
        schema_string += f" - {col['column_name']} ({col['data_type']})\n"

    llm_prompt = f"""
    You are an expert SQL query generator who specializes in converting unstructured language queries into SQL queries that are valid DuckDB queries.

    You have access to:
    1. Database schema information which includes table and column names. It is provided in:
         - {schema_string}
    2. A unstructured language query.
 
    
    When writing the SQL Query, follow these rules:
    1. Only use the table name and column names provided in the schema information.
    2. Do not use markdown or backticks in your response.
    3. Ensure that it is valid DuckDB SQL syntax.
    4. If the user asks for aggregation (e.g., 'total', 'average', 'count'), use appropriate SQL aggregate functions (SUM, AVG, COUNT, etc.) and GROUP BY clauses if needed.
    5. Be precise with column names.
    6. For boolean types, use TRUE/FALSE.
    7. For date filtering, use 'YYYY-MM-DD' format.
    8. The output **MUST ONLY** be a valid **DUCKDB SQL QUERY**
    9. **DO NOT** include **ANY** additional text, explanations, or comments in the response.

    Check if you have followed these rules before generating the SQL query. If you found that you have not, then fix the code and re-check by going through the list.
    """

    user_query = f"Convert the following unstructured language query into a valid DuckDB SQL query: {unstructured_language_query}"

    messages = [
    {"role": "system", "content": llm_prompt},
    {"role": "user", "content": user_query}
    ]

    try:
        response = client.chat.completions.create(
            model = "js2/llama-4-scout",
            messages = messages,
            max_tokens = 200,
             temperature = 0.0,
            stop = [";", "\n\n"]
        )
        llm_sql_query = response.choices[0].message.content.strip()
        return llm_sql_query

    except openai.APIStatusError as e:
        logging.error(f"API Error: {e.status_code} - {e.response}")
        logging.error(f"Response Content: {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
