import pandas as pd
import duckdb
import os
import sys

def ingest_data(file_path, table_name='ingested_data', skiprows=0):
    try:
        df = pd.read_csv(file_path, skiprows=skiprows)
        conn_obj = duckdb.connect(database=':memory:')
        conn_obj.register(table_name, df)
        return conn_obj
    except FileNotFoundError:
        print(f"ERROR---CSV file not found")
        return None
    except pd.errors.ParserError as e:
        print(f"ERROR---Parser Error: {e}")
        return None
    except Exception as e:
        print(f"ERROR---Unexpected: {e}")
        return None

def get_schema(conn, table_name):
    schema_query = f"""
        SELECT
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            table_name = '{table_name}'
     """
    schema_final = conn.execute(schema_query).fetchall()
    schema_dict = {
        "table_name": table_name,
        "columns": [
            {
            "column_name": col_name,
            "data_type": data_type
        }
            for col_name, data_type in schema_final
        ]
    }
    return schema_dict

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"ERROR---argument must be: <file_path> <table_name> <start_line>")
        sys.exit(1)
    
    dataset_file_path = sys.argv[1]
    dataset_table_name = sys.argv[2]

    try:
        start_line = int(sys.argv[3])
        if start_line < 1:
            raise ValueError("ERROR---start line number must greater than one.")
        rows_to_skip = start_line - 1

    except ValueError as e:
        print(f"ERROR---Line number must be an integer")
        sys.exit(1)

    if not os.path.exists(dataset_file_path):
        print(f"ERROR---File must be in same directory")
        sys.exit(1)

    else:
        db_connection = ingest_data(dataset_file_path, dataset_table_name, skiprows=rows_to_skip)
        if db_connection:
            extracted_schema = get_schema(db_connection, dataset_table_name)
            if extracted_schema:
                print(f"Table Name: {extracted_schema['table_name']}")
                print("Columns:")
                for col in extracted_schema['columns']:
                    print(f"  - {col['column_name']} (DuckDB Type: {col['data_type']})")
            db_connection.close()
