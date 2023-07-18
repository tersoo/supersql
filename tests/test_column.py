from unittest import TestCase

from supersql import Column
from supersql.column import SYM


class TestField(TestCase):
    def setUp(self) -> None:
        self.f = Column('id')

    def test_field(self):
        field = Column('name')
        self.assertEqual(str(field), 'name')
    
    def test_field_eq(self):
        field = Column('id')
        self.assertIsInstance(field == 5, Column)
        self.assertEqual(field._sql, f'id = {SYM}')
        self.assertEqual(field._arg, 5)
    
    def test_field_ge(self):
        self.f >= 100
        self.assertEqual(f'id >= {SYM}', self.f._sql)
        self.assertEqual(self.f._arg, 100)
    
    def test_field_gt(self):
        self.f > 100
        self.assertEqual(f'id > {SYM}', self.f._sql())
        self.assertEqual(self.f._arg, 100)
    
    def test_field_mul(self):
        self.f * 100
        self.assertEqual(f'id * {SYM}', self.f._sql())
        self.assertEqual(self.f._arg, 100)

    def test_field_le(self):
        self.f <= 100
        self.assertEqual(f'id <= {SYM}', self.f._sql)
        self.assertEqual(self.f._arg, 100)
    
    def test_field_lt(self):
        self.f < 100
        self.assertEqual(f'id < {SYM}', self.f._sql)
        self.assertEqual(self.f._arg, 100)
    
    def test_field_mod_like(self):
        self.f % 'abc'
        self.assertEqual(f'id LIKE {SYM}', self.f._sql)
        self.assertEqual(self.f._arg, '%abc%')
    
    def test_field_ne(self):
        self.f != 100
        self.assertEqual(f'id <> {SYM}', self.f._sql)
        self.assertEqual(self.f._arg, 100)
    
    def test_field_coercion(self):
        f = Column('name')
        self.assertRaises(ValueError, f.COERCE, {})
    
    def test_field_quotes(self):
        self.assertEqual(Column.QUOTE(5), 5)
        self.assertEqual(Column.QUOTE('mavis'), "'mavis'")
    
    def test_field_LIKE(self):
        f = Column('name')

        f.LIKE('yimu', mask='%%')
        self.assertEqual(f._arg, '%yimu%')

        f.LIKE('yimu', mask='-%')
        self.assertEqual(f._arg, 'yimu%')

        f.LIKE('yimu', mask='%-')
        self.assertEqual(f._arg, '%yimu')

        self.assertRaises(ValueError, f.LIKE, 'abc', '')
    
    def test_field_AS(self):
        f = Column('foo').AS('other')
        self.assertEqual(str(f), 'foo AS other')
