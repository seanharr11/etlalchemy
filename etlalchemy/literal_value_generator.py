import shutil
import decimal
import datetime
# Find the best implementation available on this platform
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

def _generate_literal_value_for_csv(value, dialect):
    dialect_name = dialect.name.lower()
    
    if isinstance(value, basestring):
        if dialect_name in ['sqlite', 'mssql']:
            # No support for 'quote' enclosed strings
            return "%s" % value
        else:
            value = value.replace('"', '""')
            return "\"%s\"" % value
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
            return '%02d-%02d-%02d %02d:%02d:%02d' %\
                    (value.year,value.month,value.day,value.hour,value.minute,value.second)
        elif dialect_name == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" %\
                    ('%02d-%02d-%02d %02d:%02d:%02d' %\
                        (value.year,value.month,value.day,value.hour,value.minute,value.second))
                #value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "postgresql":
            return '%02d-%02d-%02d %02d:%02d:%02d' %\
                    (value.year,value.month,value.day,value.hour,value.minute,value.second)
            #return '%Y-%m-%d %H:%M:%S'.format(value)
            #return "\"%s\"" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "mssql":
            #return "'%s'" % value.strftime("%m/%d/%Y %H:%M:%S.%p")
            return '%02d%02d%02d %02d:%02d:%02d.0' %\
                    (value.year,value.month,value.day,value.hour,value.minute,value.second)

        elif dialect_name == "sqlite":
            #return "%s" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return '%02d-%02d-%02d %02d:%02d:%02d.0' %\
                    (value.year,value.month,value.day,value.hour,value.minute,value.second)
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. " +
                    "Implement it here!" % dialect.name)
    elif isinstance(value, datetime.date):
        if dialect_name == "mysql":
            return '%02d-%02d-%02d' %\
                    (value.year,value.month,value.day)
        elif dialect_name == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" %\
                    ('%02d-%02d-%02d' % (value.year,value.month,value.day))
        elif dialect_name == "postgresql":
            return '%02d-%02d-%02d' %\
                    (value.year,value.month,value.day)
        elif dialect_name == "mssql":
            return "'%02d/%02d/%02d'" %\
                    (value.year,value.month,value.day)
        elif dialect_name == "sqlite":
            return "%02d-%02d-%02d" %\
                    (value.year,value.month,value.day)
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
        #if dialect_name == "mysql":
        #    return "STR_TO_DATE('%s','%%Y-%%m-%%d %%H:%%M:%%S')" %\
        #        value.strftime("%Y-%m-%d %H:%M:%S")
        if dialect_name == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" %\
                    ('%02d-%02d-%02d %02d:%02d:%02d' %\
                        (value.year,value.month,value.day,value.hour,value.minute,value.second))
        #elif dialect_name == "postgresql":
        #    return "to_date('%s', 'YYYY-MM-DD HH24:MI:SS')" %\
        #        value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect_name == "mssql":
            #return "'%s'" % value.strftime("%Y%m%d %H:%M:%S %p")
            return "'%02d%02d%02d %02d:%02d:%02d 0'" %\
                    (value.year,value.month,value.day,value.hour,value.minute,value.second)
        #elif dialect_name == "sqlite":
        #    return "'%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. " +
                    "Implement it here!" % dialect.name)
    elif isinstance(value, datetime.date):
        #if dialect_name == "mysql":
        #    return "STR_TO_DATE('%s','%%Y-%%m-%%d')" %\
        #        value.strftime("%Y-%m-%d")
        if dialect_name == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" %\
                ('%02d-%02d-%02d' % (value.year,value.month,value.day))
        #elif dialect_name == "postgresql":
        #    return "to_date('%s', 'YYYY-MM-DD')" %\
        #        value.strftime("%Y-%m-%d")
        elif dialect_name == "mssql":
            return "'%02d%02d%02d'" % (value.year,value.month,value.day)
        #elif dialect_name == "sqlite":
        #    return "'%s'" % value.strftime("%Y-%m-%d")
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
    if dialect.name.lower() in ["sqlite"]:
        separator = "|"
    elif dialect.name.lower() in ["mssql"]:
        separator = "|,"
        
    num_cols = len(raw_rows[0])
    num_rows = len(raw_rows)
    out = StringIO()
    for i in range(0, num_rows):
        for j in range(0, num_cols - 1):
            out.write(_generate_literal_value_for_csv(raw_rows[i][j], dialect))
            out.write(separator)
        # Print the last column w/o the separator
        out.write(_generate_literal_value_for_csv(raw_rows[i][num_cols - 1], dialect) + "\n")
    out.seek(0)
    fp.write(out.getvalue())
            

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
