from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, Response 
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import pandas as pd
import duckdb
import os
import sys
import logging
import kaleido
import json
import numpy as np


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from .data_ingestion import ingest_data, get_schema
from .llm_integration import llm_to_sql
from .visualization import visualize_data, suggest_visualizations, recognize_visualization_request

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


app = FastAPI(
    title="Converse with Your Data API",
    description="API for ingesting CSV data, extracting schema, converting NL to SQL, executing, and visualizing.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"], 
    allow_headers=["*"], 
)


database_connections: Dict[str, duckdb.DuckDBPyConnection] = {}
database_schemas: Dict[str, Dict[str, Any]] = {}    
full_dataset_dfs: Dict[str, pd.DataFrame] = {}      
last_query_results: Dict[str, pd.DataFrame] = {}
generated_plotly_figures: Dict[str, List[Dict[str, Any]]] = {}  


current_session_id = "default_user_session"

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Converse with Your Data API! Visit /docs for API documentation."}

@app.post("/ingestion_suggestion/")
async def ingest_data_and_extract_schema_endpoint(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    data_start_line: int = Form(...)
):

    logging.info(f"Received file upload request: {file.filename}, table: {table_name}, start_line: {data_start_line}")

    if data_start_line < 1:
        raise HTTPException(status_code=400, detail="Data start line number must be 1 or greater.")

    skip_rows = data_start_line - 1
    temp_file_path = f"temp_{file.filename}"

    try:
        with open(temp_file_path, "wb") as buffer:
            while contents := await file.read(1024 * 1024):
                buffer.write(contents)
        logging.info(f"File '{file.filename}' saved temporarily to '{temp_file_path}'.")

        conn = ingest_data(temp_file_path, table_name, skiprows=skip_rows)
        if conn is None:
            raise HTTPException(status_code=500, detail="Failed to ingest data into DuckDB. Check server logs for details.")

        schema = get_schema(conn, table_name)
        if schema is None:
            conn.close()
            raise HTTPException(status_code=500, detail="Failed to retrieve schema from DuckDB.")

        database_connections[current_session_id] = conn
        database_schemas[current_session_id] = schema

        full_df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        full_df.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)
        
        # --- MODIFIED LINE: Directly convert to dict, let NpEncoder handle NaNs/infs ---
        full_data_records = full_df.to_dict(orient='records')
        # --- END MODIFIED LINE ---

        print(f"\n--- DEBUG: Ingestion Endpoint - full_df details ---")
        print(f"DEBUG: full_df is empty: {full_df.empty}")
        print(f"DEBUG: full_df shape: {full_df.shape}")
        if not full_df.empty:
            print(f"DEBUG: full_df columns: {list(full_df.columns)}")
            print(f"DEBUG: full_df dtypes:\n{full_df.dtypes}")
            print(f"DEBUG: full_df head:\n{full_df.head()}")
        print(f"--- END DEBUG ---")


        full_dataset_dfs[current_session_id] = full_df

        if current_session_id in last_query_results:
            del last_query_results[current_session_id]
        if current_session_id in generated_plotly_figures:
            del generated_plotly_figures[current_session_id]


        initial_suggestions = suggest_visualizations(schema)
        logging.info(f"Initial visualization suggestions generated: {initial_suggestions}")

        final_initial_data_preview = json.loads(json.dumps(full_data_records, cls=NpEncoder))

        logging.info(f"Data ingestion and schema extraction successful for session '{current_session_id}'.")
        return JSONResponse(content={
            "message": "File processed and schema extracted.",
            "schema": schema,
            "initial_suggestions": initial_suggestions,
            "initial_data_preview": final_initial_data_preview
        })

    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error processing file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during file processing: {e}")
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logging.info(f"Temporary file '{temp_file_path}' removed.")



@app.post("/query/")
async def query_data_endpoint(user_query: str = Form(...)):
    logging.info(f"Received user query: '{user_query}' for session '{current_session_id}'")

    if current_session_id not in database_connections or current_session_id not in database_schemas:
        raise HTTPException(status_code=400, detail="No data ingested for this session. Please upload a CSV file first.")

    conn = database_connections[current_session_id]
    schema = database_schemas[current_session_id]

    generated_sql = llm_to_sql(schema, user_query)

    if not generated_sql:
        raise HTTPException(status_code=500, detail="Failed to generate SQL query from natural language. Please rephrase your question.")

    logging.info(f"Generated SQL: {generated_sql}")

    try:
        output_dataframe = conn.execute(generated_sql).fetchdf()
        logging.info("SQL query executed successfully.")
    except duckdb.ParserException as e:
        logging.error(f"DuckDB SQL Parser Error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid SQL generated: {e}. Please try another query or rephrase.")
    except duckdb.Error as e:
        logging.error(f"DuckDB Execution Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}. Check column names or data types in your query.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during SQL execution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during query execution: {e}")

    last_query_results[current_session_id] = output_dataframe

    if output_dataframe.empty:
        return JSONResponse(content={
            "message": "Query executed successfully, but returned no results.",
            "sql_query": generated_sql,
        })
    else:
        output_dataframe.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)
        return JSONResponse(content={
            "message": "Query executed successfully. Tabular results:",
            "sql_query": generated_sql,
            "results": output_dataframe.to_dict(orient='records'), 
        })

@app.post("/generate_visualization/")
async def generate_visualization_endpoint(plot_types: str = Form(...)):
    
    logging.info(f"Received visualization request for types: '{plot_types}' for session '{current_session_id}'")

    if current_session_id not in full_dataset_dfs or current_session_id not in database_schemas:
        raise HTTPException(status_code=400, detail="No full dataset available for visualization. Please upload a CSV first via /ingestion_suggestion/.")

    df_to_visualize = full_dataset_dfs[current_session_id]
    schema = database_schemas[current_session_id]

    if df_to_visualize.empty:
        raise HTTPException(status_code=400, detail="Last query returned no data. Cannot generate a plot.")

    requested_plot_types = [p.strip() for p in plot_types.split(',') if p.strip()]

    normalized_plot_types = []
    for p_type in requested_plot_types:
        normalized = recognize_visualization_request(p_type)
        if normalized:
            normalized_plot_types.extend(normalized) 
        else:
            logging.warning(f"Skipping unrecognized plot type: {p_type}")

    print(f"\n--- DEBUG: generate_visualization_endpoint details ---")
    print(f"DEBUG: raw_requested_plot_types (from form): {requested_plot_types}")
    print(f"DEBUG: normalized_plot_types (after recognize_visualization_request): {normalized_plot_types}")
    print(f"--- END DEBUG ---")


    if not normalized_plot_types:
        raise HTTPException(status_code=400, detail="No valid visualization types were provided or recognized.")
    try:
        raw_plotly_figures = visualize_data(df_to_visualize, normalized_plot_types, schema['table_name'])
        json_serializable_figures = []
        for fig_data in raw_plotly_figures:

            if hasattr(fig_data, 'to_dict'): 
                json_serializable_figures.append(fig_data.to_dict())
            else: 
                json_serializable_figures.append(fig_data)
        final_plots_content = json.loads(json.dumps(json_serializable_figures, cls=NpEncoder))

        if not final_plots_content:
            raise ValueError("No plots could be generated or converted to JSON.")

        return JSONResponse(content={
            "message": "Visualizations generated successfully.",
            "plots": final_plots_content
        })

    except ValueError as ve:
        logging.error(f"Plot generation failed: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Cannot generate plots: {ve}. Data might not be suitable for requested types.")
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"An unexpected error occurred during visualization generation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during visualization generation: {e}")

