"""
Monty Python: Restaurant Sketch

Wife: It's nice here, isn't it?
Man: Oh, very good restaurant, three stars you know.
Wife: Really?
Man: Mmm...

Waiter: Good evening, sir! Good evening, madam! And may I say what a
    pleasure it is to see you here again, sir!

Man: Oh thank you. Well  re you are dear. Have a look there, anything
    you like. The boeuf en croute is fantastic.

Waiter: Oh if I may suggest, sir ... the pheasant à la reine, the sauce
    is one of the chefs most famous creations.

Man: Em... that sounds good. Anyway just have a look... take your time. Oh, er
    by the way - got a bit of a dirty fork, could you ... er·.. get me another one?

Waiter: I beg your pardon.

Man: Oh it's nothing ... er, I've got a fork a little bit dirty. Could
    you get me another one? Thank you.

Waiter: Oh ... sir, 1 do apologize.
Man: Oh, no need to apologize, it doesn't worry me.
Waiter: Oh no, no, no, I do apologize. I will fetch the head waiter immediatement.
Man: Oh, there's no need to do that!
Waiter: Oh, no no... I'm sure the head waiter, he will want to apologize to you
    himself. I will fetch him at once.

Wife: Well, you certainly get good service here.
Man: They really look after you... yes.
"""


SYM = '--?--'


class Column():
    def __init__(self, col: str, table = None):
        self._parameterize = True
        self._name = col
        self._table = table
        self._sql = None
        self._alias = None
        self._arg = None

    def __eq__(self, v: any) -> 'Column':
        if isinstance(v, Column):
            # if comparing column to column then return a string
            return f'{self} = {v}'
        self._sql = f'{self} = {SYM}'
        self._arg = v
        return self

    def __ge__(self, v: any) -> 'Column':
        self._sql = f'{self} >= {SYM}'
        self._arg = v
        return self

    def __gt__(self, v: any) -> 'Column':
        return self.x('>', v)

    def __le__(self, v: any) -> 'Column':
        self._sql = f'{self} <= {SYM}'
        self._arg = v
        return self

    def __lt__(self, v: any) -> 'Column':
        self._sql = f'{self} < {SYM}'
        self._arg = v
        return self

    def __mod__(self, v: any) -> 'Column':
        self._sql = f'{self} LIKE {SYM}'
        self._arg = f'%{v}%'
        return self
    
    def __mul__(self, v: any) -> 'Column':
        return self.x('*', v)

    def __ne__(self, v: any) -> 'Column':
        self._sql = f'{self} <> {SYM}'
        self._arg = v
        return self

    def __neg__(self) -> 'Column':
        self._sql = f'{self} DESC'
        return self

    def __pos__(self) -> 'Column':
        self._sql = f'{self} ASC'
        return self

    def __str__(self) -> str:
        table_alias = self._table.__alias__ if self._table else None
        prefix = f'{table_alias}.' if table_alias else ''
        if self._alias:
            return f'{prefix}{self._name} AS {self._alias}'
        return f'{prefix}{self._name}'
    
    @property
    def parameterize(self) -> bool:
        return self._parameterize
    
    @parameterize.setter
    def parameterize(self, value: bool):
        self._parameterize = value

    def AS(self, alias: str):
        self._alias = alias
        return self

    def LIKE(self, v, mask = '%%'):
        """
        This is to control the position of the '%' operator, something that
        column % val is too limited to do
        """
        if mask not in ['%%', '%-', '-%', '%_', '_%']:
            raise ValueError('invalid LIKE mask provided')
        self._sql = f'{self._name} LIKE {SYM}'
        val = f'{v}'
        if mask.startswith('%'):
            val = f'%{val}'
        if mask.endswith('%'):
            val = f'{val}%'
        self._arg = val
        return self

    @staticmethod
    def COERCE(f: 'Column'):
        if not isinstance(f, (str, Column, int, float, bool)):
            raise ValueError(f'''SELECT command accepts only <type, 'Column'> and Basic Data Types (int, float, bool..)''')
        return str(f)

    def QUOTE(param, stringify=False):
        if isinstance(param, str):
            return f"'{param}'"
        return param
    
    def x(self, symbol, v: any) -> 'Column':
        def _():
            return f'{self} {symbol} {SYM}' if self.parameterize else f'{self} {symbol} {v}'
        self._sql = _
        self._arg = v
        return self
