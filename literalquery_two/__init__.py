import decimal
import datetime

def _stringify_as_literal_value_for_csv(value, dialect):
    if isinstance(value, basestring):
        value = value.replace("'", "''")
        if dialect.name.lower() in ['sqlite', 'mssql', 'postgresql']:
            # No support for 'quote' enclosed strings
            return "%s" % value
        else:    
            return "'%s'" % value
    elif value is None:
        return "NULL"
    elif isinstance(value, (float, int, long)):
        return "%s" % value
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, datetime.datetime):
        if dialect.name.lower() == "mysql":
            return "%s" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "oracle":
            return "TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "postgresql":
            return "\"%s\"" % value.strftime("%Y-%m-%d %H:%M:%S")
        elif dialect.name.lower() == "mssql":
            return "'%s'" % value.strftime("%m/%d/%Y %H:%M:%S.%p")
        elif dialect.name.lower() == "sqlite":
            return "%s" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. Implement it here!" % dialect.name)            
    elif isinstance(value, datetime.date):
        if dialect.name.lower() == "mysql":
            return "%s" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "oracle":
            return "TO_DATE('%s', 'YYYY-MM-DD')" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "postgresql":
            return "\"%s\"" % value.strftime("%Y-%m-%d")
        elif dialect.name.lower() == "mssql":
            return "'%s'" % value.strftime("%m/%d/%Y")
            #return "'%s'" % value.strftime("%Y%m%d")
        elif dialect.name.lower() == "sqlite":
            return "%s" % value.strftime("%Y-%m-%d")
        else:
            raise NotImplementedError(
                    "No support for engine with dialect '%s'. Implement it here!" % dialect.name)            

    else:
        raise NotImplementedError(
                    "Don't know how to literal-quote value %r" % value)            


def _stringify_as_literal_value(value, dialect):
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

def dump_oracle_insert_statements(fp, engine, table, raw_rows, columns):        
   ##################################
   ### No Bulk Insert available in Oracle
   ##################################
   #TODO: Investigate "sqlldr" CLI utility to handle this load...
   
   with open("{0}.sql".format(table), "a+") as fp:
       buffr = ("INSERT INTO {0} (".format(table) +\
               ",".join(columns) +\
                ")\n")
       num_rows = len(raw_rows)
       dialect = engine.dialect
       for i in range(0, num_rows):
           if i == num_rows-1:
               # Last row...
               buffr += "SELECT " + ",".join(map(lambda c: _stringify_as_literal_value(c, dialect), raw_rows[i])) + \
                       " FROM DUAL\n"
           else:
               buffr += "SELECT " + ",".join(map(lambda c: _stringify_as_literal_value(c, dialect), raw_rows[i])) + \
                       " FROM DUAL UNION ALL\n"
       fp.write(buffr)
        

# Supported by [MySQL, Postgresql, sqlite, SQL server (non-Azure) ]
def dump_to_csv(fp, table_name, columns, raw_rows, dialect):
    buffr = ""
    if dialect.name.lower() == "postgresql":
        for r in raw_rows:
            buffr += "|".join(map(lambda c: _stringify_as_literal_value_for_csv(c, dialect), r))+ "\n"
    elif dialect.name.lower() in ["mssql", "sqlite"]:
        for r in raw_rows:
            buffr += "|,".join(map(lambda c: _stringify_as_literal_value_for_csv(c, dialect), r))+ "\n"
    else:
        for r in raw_rows:
            buffr += ",".join(map(lambda c: _stringify_as_literal_value_for_csv(c, dialect), r))+ "\n"
    fp.write(buffr)
    
def render_literal_value_global(value, dialect, type_):
    """Render the value of a bind parameter as a quoted literal.

    This is used for statement sections that do not accept bind paramters
    on the target driver/database.

    This should be implemented by subclasses using the quoting services
    of the DBAPI.

    """
    return _stringify_as_literal_value(value, dialect)

def dump_sql_statement(statement, fp, bind=None, table_name=None):
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

    fp.write(stmt)
