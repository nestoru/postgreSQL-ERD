import os
import psycopg2
import psycopg2.extras

# Make sure these environment variables are set in your environment
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

def get_db_schemas(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND schema_name NOT LIKE 'pg_temp_%'
          AND schema_name NOT LIKE 'pg_toast_temp_%';
    """)
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return schemas

def get_db_schema(conn, schema):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Query to initialize all columns with basic structure
    cursor.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = %s;
    """, (schema,))

    schema_info = {}
    for row in cursor:
        full_table_name = f"{schema}.{row['table_name']}"
        column = row['column_name']

        if full_table_name not in schema_info:
            schema_info[full_table_name] = {'columns': {}}

        schema_info[full_table_name]['columns'][column] = {'constraints': [], 'is_primary': False, 'is_foreign': False}

    # Query to get table and column constraints
    cursor.execute("""
        SELECT 
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name, 
            tc.constraint_type,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE 
            tc.table_schema = %s
        ORDER BY 
            tc.table_schema, 
            tc.table_name;
    """, (schema,))

    for row in cursor:
        full_table_name = f"{row['table_schema']}.{row['table_name']}"
        column = row['column_name']
        constraint_type = row['constraint_type']
        foreign_table = row['foreign_table_name']
        foreign_column = row['foreign_column_name']

        if constraint_type == 'PRIMARY KEY':
            schema_info[full_table_name]['columns'][column]['is_primary'] = True

        if constraint_type == 'FOREIGN KEY' and foreign_table and foreign_column:
            schema_info[full_table_name]['columns'][column]['is_foreign'] = True
            schema_info[full_table_name]['columns'][column]['references'] = {
                'table': f"{row['foreign_table_schema']}.{foreign_table}", 
                'column': foreign_column
            }

    # Now, get the column types and update each column
    for full_table_name in schema_info:
        for column in schema_info[full_table_name]['columns']:
            cursor.execute("""
                SELECT 
                    data_type, 
                    is_nullable
                FROM 
                    information_schema.columns 
                WHERE 
                    table_schema = %s 
                    AND table_name = %s
                    AND column_name = %s;
            """, (schema, full_table_name.split('.')[1], column))

            col_info = cursor.fetchone()
            if col_info:
                schema_info[full_table_name]['columns'][column]['type'] = col_info['data_type']
                schema_info[full_table_name]['columns'][column]['is_nullable'] = col_info['is_nullable']

    cursor.close()
    return schema_info

def convert_to_d2_format(all_schema_data):
    d2_output = ""
    relationships = []

    reserved_keywords = ["direction", "shape", "table"]  # Add more if needed
    escape_column = lambda col: f"`{col}`" if col in reserved_keywords else col

    # Iterate through each table and add its structure
    for table_name, table_data in all_schema_data.items():
        d2_output += f"{table_name}: {{\n  shape: sql_table\n"

        # Adding columns and constraints
        for column, details in table_data['columns'].items():
            escaped_column = escape_column(column)
            column_line = f"  {escaped_column}: {details['type']}"
            constraints = []
            if details['is_primary']:
                constraints.append("primary_key")
            if details['is_foreign']:
                constraints.append("foreign_key")
                ref_table = details['references']['table']
                ref_column = details['references']['column']
                if ref_table and ref_column:  # To handle cases where there's a foreign key without a clear reference
                    relationships.append(f"{table_name}.{escaped_column} -> {ref_table}.{escape_column(ref_column)}")
            if constraints:
                column_line += f" {{constraint: {', '.join(constraints)}}}"
            d2_output += f"{column_line}\n"

        d2_output += "}\n\n"

    # Add relationships
    for relationship in relationships:
        d2_output += f"{relationship}\n"

    return d2_output

conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)

try:
    all_schemas = get_db_schemas(conn)
    all_schema_data = {}

    for schema in all_schemas:
        schema_data = get_db_schema(conn, schema)
        all_schema_data.update(schema_data)

    d2_data = convert_to_d2_format(all_schema_data)
    print(d2_data)
finally:
    conn.close()
