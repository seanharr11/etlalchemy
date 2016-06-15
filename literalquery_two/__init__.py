import decimal
import datetime


def render_literal_value_global(value, dialect, type_):
    """Render the value of a bind parameter as a quoted literal.

    This is used for statement sections that do not accept bind paramters
    on the target driver/database.

    This should be implemented by subclasses using the quoting services
    of the DBAPI.

    """
    if isinstance(value, basestring):
        value = value.replace("'", "''")
        return "'%s'" % value
    elif value is None:
        return "NULL"
    elif isinstance(value, (float, int, long)):
        return "%s" % value
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, datetime.datetime):
        if dialect.name.lower() == "mysql":
            return "STR_TO_DATE('%s','%%Y-%%m-%%d %%H:%%M:%%S')" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "postgresql":
            return "to_date('%s', 'YYYY-MM-DD HH24:MI:SS')" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "mssql":
            return "'%s'" % value.strftime("%Y%m%d %H:%M:%S %p")
        elif dialect.name.lower() == "sqlite":
            return "'%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. Implement it here!" % dialect.name)            
    elif isinstance(value, datetime.date):
        if dialect.name.lower() == "mysql":
            return "STR_TO_DATE('%s','%%Y-%%m-%%d')" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "postgresql":
            return "to_date('%s', 'YYYY-MM-DD')" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "mssql":
            return "'%s'" % value.strftime("%Y%m%d")
        elif dialect.name.lower() == "sqlite":
            return "'%s'" % value.strftime("%Y-%m-%d")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. Implement it here!" % dialect.name)            

    else:
        raise NotImplementedError(
                    "Don't know how to literal-quote value %r" % value)            

def printquery(statement, bind=None, table_name=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                    statement._mapper_zero_or_none()
            )
        statement = statement.statement
    elif bind is None:
        bind = statement.bind 

    dialect = bind.dialect
    compiler = statement._compiler(dialect)
    class LiteralCompiler(compiler.__class__):
        def visit_bindparam(
                self, bindparam, within_columns_clause=False, 
                literal_binds=False, **kwargs
        ):
            return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
            )
        def render_literal_value(self, value, type_):
            return render_literal_value_global(value, dialect, type_)

    compiler = LiteralCompiler(dialect, statement)
    
    stmt = compiler.process(statement) + ";\n"
    if dialect.name.lower() == "mssql":
        return "SET IDENTITY_INSERT {0} ON ".format(table_name) + stmt

    return stmt
