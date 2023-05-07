import json

def extract_table_info(table_name, columns):
    column_info = []
    for column_name, column in columns.items():
        print(column_name, column)
        column_data_type = column['type']
        column_info.append(f"  - {column_name} ({column_data_type})")

    print(column_info)
    return table_name, column_info

def dbt_catalog_to_schema(catalog_path, output_path):
    with open(catalog_path, 'r') as f:
        catalog = json.load(f)

    schema_info = []

    for table_name, table_data in catalog['nodes'].items():
        columns = table_data['columns']
        _, column_info = extract_table_info(table_name, columns)
        schema_info.append(f"Table: {table_name}\n" + "\n".join(column_info))

    schema_text = "\n\n".join(schema_info)
    print(schema_text)

    with open(output_path, 'w') as f:
        f.write(schema_text)

if __name__ == "__main__":
    catalog_path = "timeball/target/catalog.json"
    output_path = "timeball/schema.txt"
    dbt_catalog_to_schema(catalog_path, output_path)