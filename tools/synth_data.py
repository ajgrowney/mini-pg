"""CLI tool for generating synthetic data for testing purposes
"""

import argparse
from datetime import datetime, timedelta
import json
import os
import random

def generate_value(constraints:dict, serial:int = None) -> any:
    if constraints["type"] == "int":
        return random.randint(constraints.get("min", 0), constraints.get("max", 100))
    elif constraints["type"] == "serial":
        return serial
    elif constraints["type"] == "float":
        return random.uniform(constraints.get("min", 0), constraints.get("max", 100))
    elif constraints["type"] == "str":
        return ''.join(random.choices(constraints.get("chars", 'abcdefghijklmnopqrstuvwxyz'), k=constraints.get("length", 10)))
    elif constraints["type"] == "name":
        return " ".join([random.choice(constraints["first"]), random.choice(constraints["last"])])
    elif constraints["type"] == "bool":
        return random.choice([True, False])
    elif constraints["type"] == "datetime":
        start = datetime.strptime(constraints.get("min", "2021-01-01"), "%Y-%m-%d")
        end = datetime.strptime(constraints.get("max", "2025-12-31"), "%Y-%m-%d")
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    elif constraints["type"] == "list":
        value_type = constraints.get("value_type", "int")
        return [
            generate_value({"type": value_type, **constraints.get("value_constraints", {})}) 
            for _ in range(constraints.get("length", 10))
        ]
    elif constraints["type"] == "dict":
        return {
            key: generate_value(value_constraints)
            for key, value_constraints in constraints.get("fields", {}).items()
        }
    else:
        raise ValueError(f"Unsupported type: {constraints['type']}")

def generate_record(constraints:dict, serial: int = None) -> dict:
    """Generate a record based on the constraints provided
    :param constraints: key = column name, value = list of constraints
    :return: generated record
    """
    rec = {}
    for col, col_constraints in constraints.items():
        # Generate a value based on the constraints
        rec[col] = generate_value(col_constraints, serial)
    return rec

def generate_data(table_columns_constraints:dict, num_records:int, output_file:str):
    """Generate synthetic data based on the constraints provided
    :param table_columns_constraints: key = column name, value = list of constraints
    :param num_records: number of records to generate
    :param output_file: output file to write the generated data
    """
    if output_file.endswith('.jsonl'):
        for i in range(num_records):
            # Generate a record based on the constraints
            rec = generate_record(table_columns_constraints, serial=i)
            # Append the record to the output file
            with open(output_file, 'a') as f:
                f.write(json.dumps(rec, default=str) + '\n')
    elif output_file.endswith('.csv'):
        # Write the header
        with open(output_file, 'w') as f:
            f.write(','.join(table_columns_constraints.keys()) + '\n')
        for i in range(num_records):
            # Generate a record based on the constraints
            rec = generate_record(table_columns_constraints, serial=i)
            # Append the record to the output file
            with open(output_file, 'a') as f:
                f.write(','.join(map(str, rec.values())) + '\n')
    elif output_file.endswith('.parquet'):
        import pandas as pd
        # Generate records based on the constraints
        records = [generate_record(table_columns_constraints, serial = i) for i in range(num_records)]
        # Convert the records to a DataFrame
        df = pd.DataFrame(records)
        # Write the DataFrame to the output file
        df.to_parquet(output_file)

def cli():
    parser = argparse.ArgumentParser(description='Generate synthetic data for testing purposes')
    parser.add_argument('--constraints', type=str, required=True, help='JSON file containing table columns constraints')
    parser.add_argument('--num-records', type=int, default=10, help='Number of records to generate')
    parser.add_argument('--output-file', type=str, required=True, help='Output file to write the generated data')
    args = parser.parse_args()

    # Load the table columns constraints
    with open(args.constraints) as f:
        constraints = json.load(f)
    # Generate synthetic data based on the constraints provided
    generate_data(constraints, args.num_records, args.output_file)

if __name__ == '__main__':
    cli()