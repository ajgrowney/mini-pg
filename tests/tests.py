from engine import QueryPlanner
import sqlparse

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