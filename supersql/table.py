"""
Head Waiter: Excuse me monsieur and madame. (examines the fork) It's filthy,
    Gaston ... find out who washed this up, and give them their cards immediately.

Man: Oh, no, no.

Head Waiter: Better still, we can't afford to take any chances, sack
    the entire washing-up staff.

Man: No, look I don't want to make any trouble.

Head Waiter: Oh, no please, no trouble. It's quite right that you
    should point these kind of things out. Gaston, tell the manager what has happened immediately! (The Waiter runs off)

Man: Oh, no I don't want to cause any fuss.

Head Waiter: Please, it's no fuss. I quite simply wish to ensure
    that nothing interferes with your complete enjoyment of the meal.

Man: Oh I'm sure it won't, it was only a dirty fork.

Head Waiter: I know. And I'm sorry, bitterly sorry, but I know that... no
    apologies I can make can alter the fact that in our restaurant you have
    been given a dirty, filthy, smelly piece of cutlery...

Man: It wasn't smelly.

Head Waiter: It was smelly, and obscene and disgusting and I hate it, I hate
    it ,.. nasty, grubby, dirty, mingy, scrubby little
    fork. Oh ... oh . . . oh . . . 
    (runs off in a passion as the manager comes to the table)
"""


from .column import Column


class Table(object):
    def __init__(self, table: str):
        self.__tn__ = table
        self.__alias__ = None

    def __getattr__(self, name: str) -> Column:
        return Column(name, self)
    
    def __str__(self) -> str:
        if self.__alias__:
            return f'{self.__tn__} AS {self.__alias__}'
        return self.__tn__

    def AS(self, alias: str):
        self.__alias__ = alias
        return self

    @staticmethod
    def COERCE(f: 'Table'):
        if not isinstance(f, (str, Table)):
            raise ValueError(f'''SELECT command accepts only {type('str')} and <type, 'Table'>''')
        return str(f)
