import json
import os
import pytest
import sqlparse

from engine import QueryPlanner, MiniPGEngine

def get_tokens(query):
    return sqlparse.parse(query)[0]

class TestGenerateSelectPlan:
    def test_generate_select_plan_basic(self):
        query = "SELECT * FROM table"
        expected_plan = {
            'command': 'SELECT',
            'select': ['*'],
            'from': 'table',
            'joins': {},
            'where': None,
            'order_by': None,
            'group_by': None,
            'limit': None
        }
        assert QueryPlanner._generate_select_plan(get_tokens(query)) == expected_plan

    def test_generate_select_plan_with_where(self):
        query = "SELECT * FROM table WHERE table.id = 1"
        expected_plan = {
            'command': 'SELECT',
            'select': ['*'],
            'from': 'table',
            'joins': {},
            'where': 'table.id = 1',
            'order_by': None,
            'group_by': None,
            'limit': None
        }
        assert QueryPlanner._generate_select_plan(get_tokens(query)) == expected_plan

    def test_generate_select_plan_with_joins(self):
        query = "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
        expected_plan = {
            'command': 'SELECT',
            'select': ['*'],
            'from': 'table1',
            'joins': {
                'table2': {
                    'type': 'JOIN',
                    'on': 'table1.id = table2.id'
                }
            },
            'where': None,
            'order_by': None,
            'group_by': None,
            'limit': None
        }
        assert QueryPlanner._generate_select_plan(get_tokens(query)) == expected_plan

    def test_generate_select_plan_with_order_by(self):
        query = "SELECT * FROM table ORDER BY id DESC"
        expected_plan = {
            'command': 'SELECT',
            'select': ['*'],
            'from': 'table',
            'joins': {},
            'where': None,
            'order_by': [
                'id DESC'
            ],
            'group_by': None,
            'limit': None
        }
        assert QueryPlanner._generate_select_plan(get_tokens(query)) == expected_plan

    def test_generate_select_plan_with_group_by(self):
        query = "SELECT column, COUNT(*) FROM table GROUP BY column"
        expected_plan = {
            'command': 'SELECT',
            'select': ['column', 'COUNT(*)'],
            'from': 'table',
            'joins': {},
            'where': None,
            'order_by': None,
            'group_by': ['column'],
            'limit': None
        }
        assert QueryPlanner._generate_select_plan(get_tokens(query)) == expected_plan

@pytest.fixture(scope='session')
def test_data_dir():
    os.makedirs('./tests/data/global', exist_ok=False)
    # Add mgp_tables.json file
    with open("./tests/data/global/mpg_tables.json", "w+") as f:
        json.dump({}, f)
    os.makedirs('./tests/data/json_db', exist_ok=False)
    os.makedirs('./tests/data/mgp_stat', exist_ok=False)
    yield "./tests/data"
    os.system("rm -rf ./tests/data")


@pytest.fixture(scope='session')
def test_small_user_table(test_data_dir):
    """Write a small user table to disk for testing
    """
    records = [
        {"id": 1, "name": "John", "age": 50},
        {"id": 2, "name": "Jane", "age": 51}
    ]
    table_name = "users"
    # Add metadata for engine
    table_info = {
        "columns": ["id", "name", "age"],
        "sort": "id ASC"
    }
    with open(f"{test_data_dir}/global/mpg_tables.json", "r") as f:
        d = json.load(f)
    d[table_name] = table_info
    with open(f"{test_data_dir}/global/mpg_tables.json", "w") as f:
        json.dump(d, f)

    # Write data file
    with open(f"{test_data_dir}/json_db/users.jsonl", 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')
    yield table_name, records
    os.remove(f"{test_data_dir}/json_db/users.jsonl")

@pytest.fixture(scope='session')
def test_engine(test_data_dir):
    engine = MiniPGEngine(test_data_dir)
    yield engine
    del engine

class TestMiniPGEngine:
    
    def test_execute_select_basic(self, test_engine, test_small_user_table):

        query = "SELECT * FROM users"
        expected_result = [
            {'id': 1, 'name': 'John', 'age': 50},
            {'id': 2, 'name': 'Jane', 'age': 51}
        ]
    
        msg, result = test_engine.run_query(query)
        assert result == expected_result
    
    def test_execute_select_with_where(self, test_engine, test_small_user_table):
        query = "SELECT * FROM users WHERE id = 1"
        expected_result = [{'id': 1, 'name': 'John', 'age': 50}]
    
        msg, result = test_engine.run_query(query)
        assert result == expected_result
    
    def test_execute_select_with_order_by(self, test_engine, test_small_user_table):
        query = "SELECT * FROM users ORDER BY age DESC"
        expected_result = [
            {'id': 2, 'name': 'Jane', 'age': 51},
            {'id': 1, 'name': 'John', 'age': 50}
        ]
    
        msg, result = test_engine.run_query(query)
        assert result == expected_result
    
    def test_execute_select_with_group_by(self, test_engine, test_small_user_table):
        query = "SELECT name, COUNT(*) FROM users GROUP BY name"
        expected_result = [
            {'name': 'John', 'COUNT(*)': 1},
            {'name': 'Jane', 'COUNT(*)': 1}
        ]
    
        msg, result = test_engine.run_query(query)
        assert msg == "Query OK, 2 rows returned"
        assert result == expected_result

    def test_execute_select_with_limit(self, test_engine, test_small_user_table):
        query = "SELECT * FROM users LIMIT 1"
        expected_result = [{'id': 1, 'name': 'John', 'age': 50}]
    
        msg, result = test_engine.run_query(query)
        assert msg == "Query OK, 1 rows returned"
        assert result == expected_result