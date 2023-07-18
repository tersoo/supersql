from unittest import TestCase

from supersql.table import Table


class TestTable(TestCase):
    def test_table_name(self):
        table = Table('abc')
        self.assertEqual(str(table), 'abc')
    
    def test_table_name_access(self):
        table = Table('foo')
        self.assertEqual(table.__tn__, 'foo')
    
    def test_table_coercion(self):
        table = Table('bar')
        self.assertRaises(ValueError, table.COERCE, 0)
    
    def test_table_attribute_access(self):
        table = Table('foobar')
        self.assertEqual(table.username, 'username')
        self.assertEqual(table.what_is_that_field, 'what_is_that_field')
    
    def test_table_alias(self):
        table = Table('baz').AS('b')
        self.assertEqual(str(table), 'baz AS b')
    
    def test_table_alias_field(self):
        table = Table('boo').AS('b')
        self.assertEqual(str(table.username), 'b.username')
