import logging
import os
from dotenv import load_dotenv
import openai
import json
import pandas as pd
import io
import base64
from fastapi import HTTPException
import plotly.express as px 
import plotly.graph_objects as go 
import plotly.io as pio 
import kaleido


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
verde_url = os.getenv("verde_api_url")
verde_api_key = os.getenv("verde_api_key")

client = openai.OpenAI(
    base_url=verde_url,
    api_key=verde_api_key
)

supported_plot_types = ["BAR_CHART", "LINE_CHART", "SCATTER_PLOT", "HISTOGRAM", "BOX_PLOT", "PIE_CHART"]
# --- DEBUG: Print supported_plot_types ---
print(f"DEBUG: visualization.py - supported_plot_types: {supported_plot_types}")
# --- END DEBUG ---


def suggest_visualizations(schema_information: dict) -> list[str]:
    table_name = schema_information.get("table_name", "UNKNOWN_TABLE")
    columns = schema_information.get("columns", [])

    if not table_name or not columns:
        logging.warning("WARNING---Schema information is incomplete.")
        return []
    
    schema_string = f"Table: {table_name}\nColumns:\n"
    for col in columns:
        schema_string += f" - {col.get('column_name', 'UNKNOWN_COLUMN')} (DuckDB Type: {col.get('data_type', 'UNKNOWN_TYPE')})\n"

    suggest_visualizations_system_prompt = f"""
    You are an expert data visualization assistant. Your task is to analyze a given database table schema and suggest up to 4 highly relevant and common visualization types.
    Suggest only plot types that are universally applicable based on the column types available in the schema provided. Prioritize plot types that make sense for common data type combinations.

    Here is the database schema information you must use:
     - {schema_string}

    The possible visualization types you can suggest are:
    - 'BAR_CHART' (useful for comparing categorical data, or counts)
    - 'LINE_CHART' (useful for trends over time, or continuous numerical data with an order)
    - 'SCATTER_PLOT' (useful for showing relationships between two numerical variables)
    - 'HISTOGRAM' (useful for showing the distribution of a single numerical variable)
    - 'BOX_PLOT' (useful for showing distribution and outliers for numerical data across categories)
    - 'PIE_CHART' (useful for showing proportions of a whole for a few categories, with a numerical value)

    Your response must follow these rules:
    1. You **MUST** make it be a comma-separated list of the suggested visualization types - verbaitim.
    2. **DO NOT** include any conversational text, explanations, markdown, or backticks.
    3. Your response should only contain **MAXIMUM** 4 suggestions
    4. Example output: BAR_CHART, LINE_CHART, HISTOGRAM, SCATTER_PLOT
    5. **AT LEAST** one suggestion must be made from the list of possible visualization types.
    """

    messages = [
        {"role": "system", "content": suggest_visualizations_system_prompt},
        {"role": "user", "content": f"Suggest visualizations for the following schema: {schema_string}"}
    ]
    try:
        logging.info("INFO---Sending visualization suggestion request to LLM.")
        response = client.chat.completions.create(
            model = "js2/llama-4-scout",
            messages = messages,
            max_tokens = 200,
            temperature = 0.0,
            stop=["\n\n", "\n"]
        )

        suggest_visualizations_system_output = response.choices[0].message.content.strip()
        logging.info(f"INFO---LLM suggested: {suggest_visualizations_system_output}")

        suggested_types = [
            s.strip().replace("'", "").replace('"', '') for s in suggest_visualizations_system_output.split(',') if s.strip()
        ]
        
        final_suggestions = []
        seen_types = set()
        for t in suggested_types:
            if t in supported_plot_types and t not in seen_types:
                final_suggestions.append({"type": t, "reason": "LLM-suggested based on schema."}) # Add a generic reason
                seen_types.add(t)

        return final_suggestions

    except openai.APIStatusError as e:
        logging.error(f"ERROR---API Error during visualization suggestion: {e.status_code} - {e.response.text}")
        return []
    except Exception as e:
        logging.error(f"ERROR---An unexpected error occurred during visualization suggestion: {e}", exc_info=True)
        return []

def recognize_visualization_request(visualization_request: str) -> list[str]:
    recognize_visualization_request_llm_prompt = f"""
    You are an expert in understanding requests for data visualizations. 
    Your task is to recognize the type of visualization requested based on the provided input by the user.
    That input is:
        - {visualization_request}
    The possible visualization types are the following:
    - 'BAR_CHART'
    - 'LINE_CHART'
    - 'SCATTER_PLOT'
    - 'HISTOGRAM'
    - 'BOX_PLOT'
    - 'PIE_CHART'

    Your response must follow:
    1. **ONLY** return the verbatim string of the visualization type. Those strings are listed above.
    2. **DO NOT** include any additional text, explanations, or comments in the response.
    3. **DO NOT** use markdown or backticks in your response.
    4. If the input does not match any of the visualization types, return 'NONE'.
    5. If the user requests multiple, return them as a comma-separated list, verbatim.
    6. Example output for multiple requests: BAR_CHART, LINE_CHART, HISTOGRAM, SCATTER_PLOT
    7. Example output for single request: BAR_CHART
    """

    messages = [
    {"role": "system", "content": recognize_visualization_request_llm_prompt},
    {"role": "user", "content": f"What type of visualiztion is requested by the user? The input is: {visualization_request}"}
    ]

    try:
        logging.info("INFO---Sending request to LLM for visualization recognition.")
        response = client.chat.completions.create(
            model = "js2/llama-4-scout",
            messages = messages,
            max_tokens = 200,
             temperature = 0.0,
            stop=["\n\n", "\n"]
        )
        llm_recognized_visualization = response.choices[0].message.content.strip()
        logging.info(f"INFO---LLM recognized: {llm_recognized_visualization}")

        # --- DEBUG: Print LLM's raw recognition output ---
        print(f"DEBUG: recognize_visualization_request - LLM raw output: '{llm_recognized_visualization}'")
        # --- END DEBUG ---

        if llm_recognized_visualization.upper() == 'NONE':
            # --- DEBUG: LLM returned NONE ---
            print(f"DEBUG: recognize_visualization_request - LLM returned 'NONE'. Returning empty list.")
            # --- END DEBUG ---
            return []

        if llm_recognized_visualization.upper() == 'NONE':
            return []
        
        recognized_types = [
            s.strip().replace("'", "").replace('"', '') for s in llm_recognized_visualization.split(',') if s.strip()
        ]
        
        final_recognized_types = [t for t in recognized_types if t in supported_plot_types]

        # --- DEBUG: Print final recognized types ---
        print(f"DEBUG: recognize_visualization_request - Final recognized types (after filtering): {final_recognized_types}")
        # --- END DEBUG ---
        return final_recognized_types
    
    except openai.APIStatusError as e:
        logging.error(f"ERROR---API Error during visualization recognition: {e.status_code} - {e.response.text}")
        return []
    except Exception as e:
        logging.error(f"ERROR---An unexpected error occurred during visualization recognition: {e}", exc_info=True)
        return []

def visualize_data(df: pd.DataFrame, vis_types: list[str], vis_name: str) -> list[dict]:
    if df.empty:
        logging.error("ERROR---DataFrame is empty.")
        raise ValueError("ERROR---DataFrame is empty.")
    # --- DEBUGGING PRINTS AT START OF visualize_data ---
    print(f"\n--- DEBUG: visualize_data called ---")
    print(f"DEBUG: vis_types received: {vis_types}")
    print(f"DEBUG: DataFrame columns: {list(df.columns)}")
    print(f"DEBUG: DataFrame dtypes:\n{df.dtypes}")
    print(f"DEBUG: DataFrame head:\n{df.head()}")
    # --- END DEBUGGING PRINTS ---


    valid_vis_types = [vt for vt in vis_types if vt in supported_plot_types]

    # --- DEBUGGING PRINT FOR VALIDATED TYPES ---
    print(f"DEBUG: Validated vis_types for plotting: {valid_vis_types}")
    # --- END DEBUGGING PRINT ---
    if not valid_vis_types:
        logging.warning(f"WARNING---No supported visualization types provided.")
        return []
    
    generated_plotly_visualizations = []

    for vis_type in valid_vis_types:
        fig = None

        try:
            title = f"{vis_name} - {vis_type.replace('_', ' ').title()}"
            logging.info(f"INFO---Attempting to generate {vis_type} for title: {title}")

            if vis_type == "BAR_CHART":
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns
                numerical_cols = df.select_dtypes(include=['number']).columns
                logging.info(f"INFO---BAR_CHART: Detected categorical cols: {list(categorical_cols)}, numerical cols: {list(numerical_cols)}")
                print(f"DEBUG: BAR_CHART - Categorical: {list(categorical_cols)}, Numerical: {list(numerical_cols)}")

                if len(categorical_cols) >= 1 and len(numerical_cols) >= 1:
                    fig = px.bar(df, x=categorical_cols[0], y=numerical_cols[0], title=title)
                elif len(categorical_cols) >= 1:
                    value_counts_df = df[categorical_cols[0]].value_counts().reset_index()
                    value_counts_df.columns = [categorical_cols[0], 'count']
                    fig = px.bar(value_counts_df, x=categorical_cols[0], y='count', title=title)
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for BAR_CHART from '{title}'.")

            elif vis_type == "LINE_CHART":
                numerical_cols = df.select_dtypes(include=['number']).columns
                date_cols = df.select_dtypes(include=['datetime']).columns
                object_cols = df.select_dtypes(include=['object']).columns
                logging.info(f"INFO---LINE_CHART: Detected numerical cols: {list(numerical_cols)}, date cols: {list(date_cols)}, object cols (for date parsing): {list(object_cols)}")
                print(f"DEBUG: LINE_CHART - Numerical: {list(numerical_cols)}, Date: {list(date_cols)}, Object: {list(object_cols)}")

                x_col, y_col = None, None
                df_for_plot = df.copy() 

                if len(date_cols) >= 1 and len(numerical_cols) >= 1:
                    x_col = date_cols[0]
                    y_col = numerical_cols[0]
                elif len(object_cols) >= 1 and len(numerical_cols) >= 1:
                    df_for_plot[object_cols[0]] = pd.to_datetime(df_for_plot[object_cols[0]], errors='coerce')
                    if not df_for_plot[object_cols[0]].isnull().all():
                        x_col = object_cols[0]
                        y_col = numerical_cols[0]
                
                if x_col and y_col:
                    fig = px.line(df_for_plot, x=x_col, y=y_col, title=title)
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for LINE_CHART from '{title}'. Requires date-like and numerical column.")
                
            elif vis_type == "SCATTER_PLOT":
                numerical_cols = df.select_dtypes(include=['number']).columns
                print(f"DEBUG: SCATTER_PLOT - Numerical: {list(numerical_cols)}")
                logging.info(f"INFO---SCATTER_PLOT: Detected numerical cols: {list(numerical_cols)}")
                if len(numerical_cols) >= 2:
                    fig = px.scatter(df, x=numerical_cols[0], y=numerical_cols[1], title=title)
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for SCATTER_PLOT from '{title}'. Requires at least two numerical columns.")

            elif vis_type == "HISTOGRAM":
                numerical_cols = df.select_dtypes(include=['number']).columns
                print(f"DEBUG: HISTOGRAM - Numerical: {list(numerical_cols)}")
                logging.info(f"INFO---HISTOGRAM: Detected numerical columns: {list(numerical_cols)}")
                if len(numerical_cols) >= 1:
                    fig = px.histogram(df, x=numerical_cols[0], title=title)
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for HISTOGRAM from '{title}'. Requires at least one numerical column.")
            
            elif vis_type == "BOX_PLOT":
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns
                numerical_cols = df.select_dtypes(include=['number']).columns
                print(f"DEBUG: BOX_PLOT - Categorical: {list(categorical_cols)}, Numerical: {list(numerical_cols)}")
                logging.info(f"INFO---BOX_PLOT: Detected categorical cols: {list(categorical_cols)}, numerical cols: {list(numerical_cols)}")
                
                if len(numerical_cols) >= 1 and len(categorical_cols) >= 1:
                    fig = px.box(df, x=categorical_cols[0], y=numerical_cols[0], title=title)
                elif len(numerical_cols) >= 1:
                    fig = px.box(df, y=numerical_cols[0], title=title)
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for BOX_PLOT from '{title}'. Requires at least one numerical column.")
            
            elif vis_type == "PIE_CHART":
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns
                numerical_cols = df.select_dtypes(include=['number']).columns
                print(f"DEBUG: PIE_CHART - Categorical: {list(categorical_cols)}, Numerical: {list(numerical_cols)}")
                logging.info(f"INFO---PIE_CHART: Detected categorical cols: {list(categorical_cols)}, numerical cols: {list(numerical_cols)}")

                if len(categorical_cols) >= 1 and len(numerical_cols) >= 1:
                    grouped_df = df.groupby(categorical_cols[0])[numerical_cols[0]].sum().reset_index()
                    if len(grouped_df) > 1 and len(grouped_df) <= 10:
                        fig = px.pie(grouped_df, names=categorical_cols[0], values=numerical_cols[0], title=title)
                    else:
                        logging.warning(f"WARNING---Too many or too few categories ({len(grouped_df)}) for PIE_CHART. Best for 2-10 categories when summing a numerical column.")
                elif len(categorical_cols) >= 1: 
                    value_counts_df = df[categorical_cols[0]].value_counts().reset_index()
                    value_counts_df.columns = [categorical_cols[0], 'count']
                    if len(value_counts_df) > 1 and len(value_counts_df) <= 10:
                        fig = px.pie(value_counts_df, names=categorical_cols[0], values='count', title=title)
                    else:
                        logging.warning(f"WARNING---Too many or too few categories ({len(value_counts_df)}) for PIE_CHART. Best for 2-10 categories from value counts.")
                else:
                    logging.warning(f"WARNING---Not enough suitable columns for PIE_CHART from '{title}'. Requires at least one categorical column.")
                       
            if fig: 
                generated_plotly_visualizations.append(fig.to_dict()) 
                logging.info(f"INFO---Successfully generated {vis_type} Plotly figure.")
            else:
                logging.warning(f"WARNING---Plotly figure generation failed for '{vis_type}' due to unsuitable data or lack of columns. Skipping this plot.")

        except Exception as e:
            logging.error(f"ERROR---Error generating Plotly figure for type {vis_type}: {e}", exc_info=True)
            logging.warning(f"WARNING---Plotly figure generation failed for '{vis_type}' due to an error. Skipping this plot.")

    if not generated_plotly_visualizations:
        logging.warning("WARNING---No Plotly figures were successfully generated from the requested types.")
        raise ValueError("ERROR---No plots could be generated from the provided data and visualization types.")

    return generated_plotly_visualizations

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')