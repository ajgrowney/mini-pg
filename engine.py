"""Mini database engine and helpers
to replicate a subset of Postgres functionality
"""
import json
import os
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function, Values, Comparison, TokenList
from sqlparse.tokens import Keyword, DML, Whitespace, Literal, Wildcard
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

class MiniPGEngine:
    """Core DB Engine class
    """
    def __init__(self, data_dir = "./data", config = {}):
        self.data_dir = data_dir
        # ---- Setup Global Data ----
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(f"{self.data_dir}/global", exist_ok=True) # cluster-wide tables (e.g. mpg_tables, mpg_sequences)
        os.makedirs(f"{self.data_dir}/mpg_stat", exist_ok=True) # permanent files for the statistics subsystem
        # per-database subdirectories
        os.makedirs(f"{self.data_dir}/json_db", exist_ok=True) 
        # ---- Initialize Global Data ----
        table_catalog = f"{self.data_dir}/global/mpg_tables.json"
        if not os.path.exists(table_catalog):
            with open(table_catalog, 'w') as table_file:
                json.dump({}, table_file)
        seq_catalog = f"{self.data_dir}/global/mpg_sequences.json"
        if not os.path.exists(seq_catalog):
            with open(seq_catalog, 'w') as seq_file:
                json.dump({}, seq_file)
        # ---- Setup Statistics Subsystem ----
        self.stats_manager = StatsManager({'data_dir': self.data_dir, 'max_workers': config.get('max_stats_workers', 4)})
        # ---- Setup Background Workers ----
        self.worker_pool = ProcessPoolExecutor(max_workers=int(config.get('max_bg_workers', 4)))
        # ---- Setup In-Memory Caches ----
        self._sequence_cache = {}
        self._seq_cache_flush_after = config.get('seq_cache_flush_after', 10)
        self._seq_cache_hits = 0
    
    def __enter__(self):
        print("Starting MiniPG Engine")
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        print("Shutting down MiniPG Engine")
        self.worker_pool.shutdown()
        # Flush Caches
        self._json_file_update_entries(f"{self.data_dir}/global/mpg_sequences.json", self._sequence_cache)

    def _json_file_update_entries(self, file_path, data_dict):
        with open(file_path, 'r') as file:
            data = json.load(file)
        with open(file_path, 'w') as file:
            data.update(data_dict)
            json.dump(data, file)

    def run_query(self, query) -> tuple[str, list]:
        """Run a query against the database
        :param query: user query to run
        :return: tuple of status message and op
        tional result set
        """
        query_tokens = sqlparse.parse(query)[0]
        query_type = query_tokens.get_type()
        if query_type == 'SELECT':
            select_plan = QueryPlanner._generate_select_plan(query_tokens)
            results = self._execute_select(select_plan)
            return (f"Query OK, {len(results)} rows returned", results)
        elif query_type == 'CREATE':
            ct_plan = QueryPlanner._generate_create_table_plan(query)
            print(f"[create_table] Plan: {ct_plan}")
            return self._execute_create_table(ct_plan)
        elif query_type == 'INSERT':
            insert_plan = QueryPlanner._generate_insert_plan(query_tokens)
            print(f"[insert] Plan: {insert_plan}")
            return self._execute_insert(insert_plan)
        else:
            return (f"Error: Unsupported query type: {query_type}", None)

    def get_sequence_nextval(self, sequence_name, flush_cache=False):
        """Get the next value from a sequence
        """
        if sequence_name not in self._sequence_cache:
            with open(f"{self.data_dir}/global/mpg_sequences.json", 'r') as seq_file:
                seq_catalog = json.load(seq_file)
            if sequence_name not in seq_catalog:
                return f"Error: Sequence '{sequence_name}' not found"
            self._sequence_cache[sequence_name] = seq_catalog[sequence_name]
        
        self._sequence_cache[sequence_name] += 1
        self._seq_cache_hits += 1
        if self._seq_cache_hits >= self._seq_cache_flush_after or flush_cache:
            self.worker_pool.submit(self._json_file_update_entries, f"{self.data_dir}/global/mpg_sequences.json", sequence_name, self._sequence_cache[sequence_name])
            self._seq_cache_hits = 0
        return self._sequence_cache[sequence_name]

    def _file_scan(self, table_path, table_info, order_by=None, column_prefix=None):
        """Get a generator to return rows from a table
        :param table_path: path to the table file
        :param table_info: table metadata
        :param order_by: ORDER BY clause
        :param column_prefix: prefix for column names
        :return: generator to yield rows from the table
        """
        if not order_by:
            sort_dir, sort_col = None, None
        else:
            sort_args = order_by.split()
            sort_col, sort_dir = sort_args[0], sort_args[1] if len(sort_args) > 1 else (sort_args[0], None)

        if not order_by or order_by == table_info['sort']:
            with open(table_path, 'r') as table_file:
                for line in table_file:
                    if column_prefix:
                        row = json.loads(line)
                        yield {f"{column_prefix}.{col}": row[col] for col in table_info['columns']}
                    else:
                        yield json.loads(line)
        else:
            # ---- Sort ----
            with open(table_path, 'r') as table_file:
                rows = [json.loads(line) for line in table_file]
            rows.sort(key=lambda x: x[sort_col], reverse=sort_dir == 'DESC')
            for row in rows:
                if column_prefix:
                    yield {f"{column_prefix}.{col}": row[col] for col in table_info['columns']}
                else:
                    yield row
            
    def _execute_insert(self, insert_plan):
        """
        """
        table_name = insert_plan['table']
        table_info = json.load(open(f"{self.data_dir}/global/mpg_tables.json", 'r')).get(table_name)
        if not table_info:
            return f"Error: Table '{table_name}' not found in catalog"
        table_path = f"{self.data_dir}/json_db/{table_name}.jsonl"
        with open(table_path, 'a') as table_file:
            for record in insert_plan['values']:
                record_id = self.get_sequence_nextval(f"{table_name}_id_seq")
                table_file.write(json.dumps(dict(zip(["id"] + insert_plan['columns'], (record_id, ) + record))) + '\n')
        # self.stats_manager.update_table_stats(table_name)
        return (f"Inserted {len(insert_plan['values'])} records into table '{table_name}'", [])

    def _execute_select(self, execution_plan):
        """Basic execution of a SELECT query
        1. Validate table and columns
        2. Load the 'from' table into memory
        3. Apply joins 
            a. Load join table into memory with columns as {join_table.column_name}
            b. Turn 'from' columns into {from_table.column_name}
            c. Perform join on 'from_table.column_name' = 'join_table.column_name'
            d. Add join columns to 'from' table
        4. Apply WHERE clause
        5. Apply ORDER BY
        6. Apply LIMIT
        7. Return results
        """
        # ---- Validate Table and Columns ----
        table_name = execution_plan['from']
        table_info = json.load(open(f"{self.data_dir}/global/mpg_tables.json", 'r')).get(table_name)
        if not table_info:
            return f"Error: Table '{table_name}' not found in catalog"
        select_cols = execution_plan['select']
        if select_cols != ["*"]:
            for col in select_cols:
                if col not in table_info['columns']:
                    return f"Error: Column '{col}' not found in table '{table_name}'"
        table_path = f"{self.data_dir}/json_db/{table_name}.jsonl"
        results = []
        # ---- Base Table Scan ----
        base_results = []
        column_prefix = table_name if len(execution_plan.get('joins', {})) > 0 else None
        for row in self._file_scan(table_path, table_info, execution_plan.get('order_by'), column_prefix):
            base_results.append(row)
        print(f"Base Results: {base_results[:10]}")

        # ---- Handle Joins ----
        if execution_plan.get('joins'):
            for join_table, join_info in execution_plan['joins'].items():
                join_table_info = json.load(open(f"{self.data_dir}/global/mpg_tables.json", 'r')).get(join_table)
                if not join_table_info:
                    return f"Error: Join Table '{join_table}' not found in catalog"
                join_table_path = f"{self.data_dir}/json_db/{join_table}.jsonl"
                join_results = []
                for row in self._file_scan(join_table_path, join_table_info, execution_plan.get('order_by'), join_table):
                    join_results.append(row)
                for row in base_results:
                    for join_row in join_results:
                        if row[join_info['on'].split()[0]] == join_row[join_info['on'].split()[2]]:
                            # print(f"Matched: {row}, {join_row}")
                            row.update(join_row)
        print(f"Base Results: {base_results[:10]}")
        # ---- Row Level Filtering ----
        results = []
        for row in base_results:
            if execution_plan["where"]:
                if not self._evaluate_where(row, execution_plan["where"]):
                    continue
            # ---- Projection ----
            if select_cols == ['*']:
                results.append(row)
            else:
                results.append({col: row[col] for col in select_cols})
            # ---- Limiting ----
            if execution_plan["limit"] and len(results) >= execution_plan["limit"]:
                break
        return results
    
    def _evaluate_where(self, row, where_clause):
        """Handle <, >, =, AND, OR, NOT operators
        Example: users.age = 23 AND users.name = 'Alice' OR users.city = 'New York'
        Returns: True or False
        """
        def evaluate_expression(expression):
            if ' AND ' in expression:
                return all(evaluate_expression(e.strip()) for e in expression.split(' AND '))
            if ' OR ' in expression:
                return any(evaluate_expression(e.strip()) for e in expression.split(' OR '))
            if expression.startswith('NOT '):
                return not evaluate_expression(expression[4:].strip())
            match = re.match(r'(.+?)\s*(=|<|>|<=|>=|!=)\s*(.+)', expression)
            if not match:
                raise ValueError(f"Invalid expression: {expression}")
            left, operator, right = match.groups()
            left_value = row.get(left)
            if right.startswith("'") and right.endswith("'"):
                right_value = right[1:-1]
            else:
                try:
                    right_value = float(right)
                except ValueError:
                    right_value = right
            if operator == '=':
                return left_value == right_value
            elif operator == '<':
                return left_value < right_value
            elif operator == '>':
                return left_value > right_value
            elif operator == '<=':
                return left_value <= right_value
            elif operator == '>=':
                return left_value >= right_value
            elif operator == '!=':
                return left_value != right_value
            else:
                raise ValueError(f"Unsupported operator: {operator}")

        return evaluate_expression(where_clause)

    def _execute_create_table(self, table_request):
        """
        """
        print(table_request)
        table_name = table_request['table']
        table_columns = { c.split()[0]: c.split()[1] for c  in table_request['columns'].split(",")}
        table_catalog = json.load(open(f"{self.data_dir}/global/mpg_tables.json", 'r'))
        if table_name in table_catalog:
            return f"Error: Table '{table_name}' already exists"
        table_catalog[table_name] = {
            'columns': table_columns,
            'sort': None
        }
        with open(f"{self.data_dir}/global/mpg_tables.json", 'w') as table_file:
            json.dump(table_catalog, table_file)
        table_path = f"{self.data_dir}/json_db/{table_name}.jsonl"
        with open(table_path, 'w') as table_file:
            pass
        self.stats_manager.update_table_stats(table_name)
        return f"Table '{table_name}' created successfully"

class QueryPlanner:
    """Build a query plan from a parsed SQL query
    """
    def __init__(self):
        pass

    @classmethod
    def _generate_select_plan(cls, parsed_tokens):
        """Extract a query plan from a SELECT query
        Example: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice' ORDER BY orders.created_at DESC LIMIT 10
        Output: {
            'select': ['*'],
            'from': 'users',
            'joins': { 'orders': { 'type': 'INNER', 'on': 'users.id = orders.user_id' } },
            'where': 'users.name = Alice',
            'group_by': None,
            'order_by': ['orders.created_at DESC'],
            'limit': 10
        }
        """
        parsed_query = {
            'command': 'SELECT',
            'select': [],
            'from': None,
            'joins': {},
            'where': None,
            'order_by': None,
            'group_by': None,
            'limit': None
        }
        state = 'START'
        join_type, join_table = None, None
        for token in parsed_tokens.tokens:
            if token.ttype is Whitespace:
                continue
            if state == 'START':
                if token.ttype is DML and token.value.upper() == 'SELECT':
                    state = 'SELECT'
            elif state == 'SELECT':
                if token.ttype is Keyword and token.value.upper() == 'FROM':
                    state = 'FROM'
                elif isinstance(token, IdentifierList):
                    parsed_query['select'] = [str(identifier) for identifier in token.get_identifiers()]
                elif isinstance(token, Identifier) or token.ttype is Wildcard:
                    parsed_query['select'].append(str(token))
            elif state == 'FROM':
                if token.ttype is Keyword or isinstance(token, Identifier):
                    state = 'JOIN_OR_WHERE'
                    parsed_query['from'] = str(token)
                else:
                    print(f"[{state}] Unresolved: {token} {type(token)}, {token.ttype}")
            elif state == 'JOIN_OR_WHERE':
                if token.ttype is Keyword and token.value.upper() in {'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'}:
                    state = 'JOIN'
                    join_type = token.value.upper()
                elif isinstance(token, Where):
                    state = 'WHERE'
                    parsed_query['where'] = str(token)[6:].strip()
                elif token.ttype is Keyword and token.value.upper() == 'ORDER BY':
                    state = 'ORDER_BY'
                elif token.ttype is Keyword and token.value.upper() == 'GROUP BY':
                    state = 'GROUP_BY'
                elif token.ttype is Keyword and token.value.upper() == 'LIMIT':
                    state = 'LIMIT'
                else:
                    print(f"[{state}] Unresolved: {token} {type(token)}, {token.ttype}")
            elif state == 'JOIN':
                if isinstance(token, Identifier):
                    join_table = str(token)
                    state = 'ON'
            elif state == 'ON':
                if isinstance(token, Comparison):
                    parsed_query['joins'][join_table] = { 'type': join_type, 'on': str(token) }
                    state = 'JOIN_OR_WHERE'
                    join_table, join_type = None, None
            elif state == 'WHERE':
                if isinstance(token, Where):
                    parsed_query['where'] = str(token)[6:].strip()
                    state = 'JOIN_OR_WHERE'
            elif state == 'ORDER_BY':
                if isinstance(token, IdentifierList):
                    parsed_query['order_by'] = [str(identifier) for identifier in token.get_identifiers()]
                elif isinstance(token, Identifier):
                    parsed_query['order_by'] = parsed_query['order_by'] + [str(token)] if parsed_query['order_by'] else [str(token)]
                state = 'JOIN_OR_WHERE'
            elif state == 'GROUP_BY':
                if isinstance(token, IdentifierList):
                    parsed_query['group_by'] = [str(identifier) for identifier in token.get_identifiers()]
                elif token.ttype is Keyword or isinstance(token, Identifier):
                    parsed_query['group_by'] = parsed_query['group_by'] + [str(token)] if parsed_query['group_by'] else [str(token)]
                state = 'JOIN_OR_WHERE'
            elif state == 'LIMIT':
                if token.ttype is Literal.Number.Integer:
                    parsed_query['limit'] = int(token.value)
                state = 'JOIN_OR_WHERE'
        print(f"Generated Plan: {parsed_query}")
        return parsed_query

    @classmethod
    def _generate_create_table_plan(cls, query):
        parsed_query = {
            'command': 'CREATE TABLE',
            'table': None,
            'columns': None
        }
        table_name_match = re.search(r'CREATE TABLE (.+?)\(', query, re.IGNORECASE)
        if not table_name_match:
            raise Exception("Error: Invalid CREATE TABLE query: Table name not found")
        parsed_query['table'] = table_name_match.group(1).strip()
        columns_match = re.search(r'\((.+)\)', query, re.IGNORECASE)
        if not columns_match:
            raise Exception("Error: Invalid CREATE TABLE query: Columns not found")
        parsed_query['columns'] = columns_match.group(1).strip()
        return parsed_query

    @classmethod
    def _generate_insert_plan(cls, parsed_tokens):
        """INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
        """
        parsed_query = {
            'command': 'INSERT',
            'table': None,
            'columns': None,
            'values': []
        }
        cur_keyword = None
        for token in parsed_tokens.tokens:
            if token.ttype is DML and token.value.upper() == 'INSERT':
                cur_keyword = 'table'
            elif token.ttype is Keyword:
                if token.value.upper() == 'INTO':
                    cur_keyword = 'table'
                elif token.value.upper() == 'VALUES':
                    cur_keyword = 'values'
            elif isinstance(token, Function) and cur_keyword == 'table':
                table, columns = str(token).split('(', 1)
                parsed_query['table'] = table.strip()
                parsed_query['columns'] = [c.strip() for c in columns[:-1].split(',')]
            elif isinstance(token, Values):
                parsed_query['values'] = cls._values_to_records(str(token)[6:].strip())
            elif isinstance(token, Identifier):
                parsed_query[cur_keyword] = str(token)
            elif isinstance(token, IdentifierList):
                if cur_keyword == 'columns':
                    parsed_query['columns'] = [str(identifier) for identifier in token.get_identifiers()]
                else:
                    print(f"Unresolved: {token} for {cur_keyword}")
            elif token.ttype is not Whitespace:
                print("Unresolved:", (token, type(token), token.ttype))
        return parsed_query

    @staticmethod
    def _values_to_records(values_str) -> list[tuple]:
        "(val1, 'val2', ...), ('val1', val2, ...), ..."
        new_data = []
        pattern = re.compile(r"\(([^)]+)\)")
        matches = pattern.findall(values_str)
        for match in matches:
            record = []
            for val in match.split(','):
                val = val.strip()
                # Handle cast syntax
                if '::' in val:
                    val, cast_type = val.split('::')
                    val = val.strip()
                    cast_type = cast_type.strip()
                else:
                    cast_type = None
                # Remove quotes for string values
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                # Convert to appropriate type
                if cast_type in ('text', 'varchar', 'char'):
                    record.append(str(val))
                elif cast_type in ('int', 'integer', 'bigint', 'smallint'):
                    record.append(int(val))
                elif cast_type in ('float', 'double precision', 'real'):
                    record.append(float(val))
                elif cast_type == 'boolean':
                    record.append(val.lower() == 'true')
                elif cast_type == 'text[]':
                    # Handle array of text
                    array_pattern = re.compile(r'\"([^\"]*)\"')
                    array_matches = array_pattern.findall(val)
                    record.append(array_matches)
                else:
                    # Default to string if no cast type is provided
                    try:
                        record.append(int(val))
                    except ValueError:
                        try:
                            record.append(float(val))
                        except ValueError:
                            record.append(val)
            new_data.append(tuple(record))
        return new_data

class StatsManager:
    """Statistics manager for MiniPG
    """
    def __init__(self, config:dict):
        self._config = config
        self._data_dir = config.get('data_dir', './data')
        self._max_workers = config.get('max_workers', 4)

    def update_table_stats(self, table_name):
        """Update the mini pg statistics for a table
        """
        with open(f"{self._data_dir}/global/mpg_tables.json", 'r') as table_file:
            table_catalog = json.load(table_file)[table_name]
        table_columns = table_catalog['columns']
        table_path = f"{self._data_dir}/json_db/{table_name}.jsonl"
        s = {
            "row_count": 0, 
            "column_stats": {
                col: {
                    'min': None,
                    'max': None,
                    'count': 0,
                    'mode': None,
                    'val_freq': {}
                } for col in table_columns
            }
        }
        with open(table_path, 'r') as table_file:
            for line in table_file:
                row = json.loads(line)
                s["row_count"] += 1
                for col in table_columns:
                    s["column_stats"][col]['min'] = min(s["column_stats"][col]['min'], row[col])
                    s["column_stats"][col]['max'] = max(s["column_stats"][col]['max'], row[col])
                    s["column_stats"][col]['count'] += 1
                    if row[col] not in s["column_stats"][col]['val_freq']:
                        s["column_stats"][col]['val_freq'][row[col]] = 0
                    s["column_stats"][col]['val_freq'][row[col]] += 1
        
        with open(f"{self._data_dir}/mpg_stat/{table_name}.json", 'w') as stat_file:
            json.dump(s, stat_file)

    def get_table_stats(self, table_name):
        with open(f"{self._data_dir}/mpg_stat/{table_name}.json", 'r') as stat_file:
            return json.load(stat_file)

    def update_all_table_stats(self):
        with open(f"{self._data_dir}/global/mpg_tables.json", 'r') as table_file:
            table_catalog = json.load(table_file)
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            table_stats = { executor.submit(self.update_table_stats, table_name): table_name for table_name in table_catalog }
        
        for future in as_completed(table_stats):
            table_name = table_stats[future]
            try:
                future.result()
                print(f"Updated stats for table {table_name}")
            except Exception as e:
                print(f"Error updating stats for table {table_name}: {e}")

def cli():
    import readline
    import sys
    with MiniPGEngine() as engine:
        if len(sys.argv) > 1:
            print(f"Running query: {sys.argv[1]}")
            print(engine.run_query(sys.argv[1]))
        else:
            histfile = os.path.join(os.path.expanduser("~"), ".mpg_history")
            try:
                readline.read_history_file(histfile)
            except FileNotFoundError:
                pass
            readline.set_history_length(100)
            user_query = input("Enter a query [or exit (\q)]: ")
            while user_query != '\q':
                msg, res = (engine.run_query(user_query))
                for r in res:
                    print(json.dumps(r))
                print(msg)
                user_query = input("Enter a query [or exit (\q)]: ")
            readline.write_history_file(histfile)

if __name__ == '__main__':
    cli()
    # query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice' ORDER BY orders.created_at DESC LIMIT 10"
    # parsed_query = sqlparse.parse(query)[0]
    # query_type = parsed_query.get_type()