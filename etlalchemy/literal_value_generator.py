import decimal
import datetime


def _generate_literal_value_for_csv(value, dialect):
    dialect_name = dialect.name.lower()
    
    if isinstance(value, basestring):
        value = value.replace("'", "''")
        if dialect_name in ['sqlite', 'mssql', 'postgresql']:
            # No support for 'quote' enclosed strings
            return "%s" % value
        else:
            return "'%s'" % value
    elif value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "%s" % int(value)
    elif isinstance(value, (float, int, long)):
        return "%s" % value
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, datetime.datetime):
        if dialect_name == "mysql":
            return "%s" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" %\
                value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "postgresql":
            return "\"%s\"" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "mssql":
            return "'%s'" % value.strftime("%m/%d/%Y %H:%M:%S.%p")
        elif dialect_name == "sqlite":
            return "%s" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. " +
                    "Implement it here!" % dialect.name)
    elif isinstance(value, datetime.date):
        if dialect_name == "mysql":
            return "%s" % value.strftime("%Y-%m-%d")
        elif dialect_name == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" % value.strftime("%Y-%m-%d")
        elif dialect_name == "postgresql":
            return "\"%s\"" % value.strftime("%Y-%m-%d")
        elif dialect_name == "mssql":
            return "'%s'" % value.strftime("%m/%d/%Y")
        elif dialect_name == "sqlite":
            return "%s" % value.strftime("%Y-%m-%d")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'." +
                    "Implement it here!" % dialect.name)
    
    else:
        raise NotImplementedError(
                    "Don't know how to literal-quote value %r" % value)


def _generate_literal_value(value, dialect):
    dialect_name = dialect.name.lower()
    if isinstance(value, basestring):
        value = value.replace("'", "''")
        return "'%s'" % value
    elif value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "%s" % int(value)
    elif isinstance(value, (float, int, long)):
        return "%s" % value
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, datetime.datetime):
        if dialect_name == "mysql":
            return "STR_TO_DATE('%s','%%Y-%%m-%%d %%H:%%M:%%S')" %\
                value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" %\
                value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "postgresql":
            return "to_date('%s', 'YYYY-MM-DD HH24:MI:SS')" %\
                value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "mssql":
            return "'%s'" % value.strftime("%Y%m%d %H:%M:%S %p")
        elif dialect_name == "sqlite":
            return "'%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. " +
                    "Implement it here!" % dialect.name)
    elif isinstance(value, datetime.date):
        if dialect_name == "mysql":
            return "STR_TO_DATE('%s','%%Y-%%m-%%d')" %\
                value.strftime("%Y-%m-%d")
        elif dialect_name == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" %\
                value.strftime("%Y-%m-%d")
        elif dialect_name == "postgresql":
            return "to_date('%s', 'YYYY-MM-DD')" %\
                value.strftime("%Y-%m-%d")
        elif dialect_name == "mssql":
            return "'%s'" % value.strftime("%Y%m%d")
        elif dialect_name == "sqlite":
            return "'%s'" % value.strftime("%Y-%m-%d")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. " +
                    "Implement it here!" % dialect.name)

    else:
        raise NotImplementedError(
            "Don't know how to literal-quote value %r" % value)


def dump_to_oracle_insert_statements(fp, engine, table, raw_rows, columns):
    ##################################
    # No Bulk Insert available in Oracle
    ##################################
    # TODO: Investigate "sqlldr" CLI utility to handle this load...
    lines = []
    lines.append("INSERT INTO {0} (".format(table) +
                 ",".join(columns) +
                 ")\n")
    num_rows = len(raw_rows)
    dialect = engine.dialect
    for i in range(0, num_rows):
        if i == num_rows-1:
            # Last row...
            lines.append("SELECT " +
                         ",".join(map(lambda c: _generate_literal_value(
                             c, dialect), raw_rows[i])) +
                         " FROM DUAL\n")
        else:
            lines.append("SELECT " +
                         ",".join(map(lambda c: _generate_literal_value(
                             c, dialect), raw_rows[i])) +
                         " FROM DUAL UNION ALL\n")
    fp.write(''.join(lines))


# Supported by [MySQL, Postgresql, sqlite, SQL server (non-Azure) ]
def dump_to_csv(fp, table_name, columns, raw_rows, dialect):
    lines = []
    separator = ","
    # Determine the separator based on Target DB 
    if dialect.name.lower() in ["postgresql", "sqlite"]:
        separator = "|"
    elif dialect.name.lower() in ["mssql"]:
        separator = "|,"
        
    for r in raw_rows:
        for col in r:
            fp.write(_generate_literal_value_for_csv(col, dialect) + separator)
        fp.write("\n")
            

def generate_literal_value(value, dialect, type_):
    """Render the value of a bind parameter as a quoted literal.

    This is used for statement sections that do not accept bind paramters
    on the target driver/database.

    This should be implemented by subclasses using the quoting services
    of the DBAPI.

    """
    return _generate_literal_value(value, dialect)


def dump_to_sql_statement(statement, fp, bind=None, table_name=None):
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
            return generate_literal_value(value, dialect, type_)

    compiler = LiteralCompiler(dialect, statement)

    stmt = compiler.process(statement) + ";\n"
    if dialect.name.lower() == "mssql":
        stmt = "SET IDENTITY_INSERT {0} ON ".format(table_name) + stmt

    fp.write(stmt)
