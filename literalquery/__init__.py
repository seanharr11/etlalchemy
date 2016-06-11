from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType

# python2/3 compatible.
PY3 = str is not bytes
text = str if PY3 else unicode
int_type = int if PY3 else (int, long)
str_type = str if PY3 else (str, unicode)

class StringLiteral(String):
    """Teach SA how to literalize various things."""
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int_type):
                return text(value)
            if not isinstance(value, str_type):
                value = text(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(DefaultDialect):
    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
    }


def literalquery(statement):
    """NOTE: This is entirely insecure. DO NOT execute the resulting strings."""
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        statement = statement.statement
    return statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True},
    ).string
