#########################
# This is a TODO!!! Not complete at all..
#########################
class DataTransformer():
    def __init__(self, name, search_re, lam, table, column):
        self.name = name
        self.search_re = re.compile(search_re)
        self.lam = lam
        self.table = table
        self.column = column
    def convert(self, className, obj):
        if className == 'int':
            return int(obj)
        elif className == 'float':
            return float(obj)
        elif className == 'str':
            return str(obj)
        elif className == 'Decimal':
            return Decimal(obj)
        else:
            raise UnknownType("Class Name '" + className + "' is currently unhandled...")
    def clean(self, entry):
        ################################
        ## Should always return an object of the same type it got
        ###############################
        className = entry.__class__.__name__
        if className not in ['int', 'str', 'float', 'Decimal']:
            raise UnknownType("Class Name '" + className + "' is currently unhandled...")
            return None
        matched = re.match(self.search_re, str(entry))
        c = self.lam(matched.groups())
        c = self.convert(className, c)
        assert(c.__class__.__name__ == className)
        return c
    def marshal(self):
        return dill.dumps(self)
      

class TableDataTransformer():
    def __init__(self, table):
        try:
           self.primary_key_column = inspect(table).primary_key.columns.keys()[0]
        except IndexError:
           self.primary_key_column = None
        self.table = table
        self.cleaners = {}
    ###############################
    ### Cleaners is a dict of 'column' -> cleaner object
    ###############################
    def loadTransformers(self, cleaners):
        # Put the right <cleaner> at the right column INDEX...
        self.cleaners = cleaners
    ###############################
    ### Returns a list of CLEANED rows (dictionaries)
    ### ... ONLY includes cleanred rows, not ENTIRE rows (see clean_for_insert)
    ##############################
    ### [
    ###  {
    ###   "pk": ('primary_key_col': 123),
    ###   "row": 
    ###      {'agt_id': 123, 'agt_name': 'John Doe', ... 'last_field_cleaned': 'foobar'}, 
    ###  }, 
    ###  {
    ###   "pk": ('primary_key_col': 123),
    ###   "row": 
    ###      {'agt_id': 123, 'agt_name': 'John Doe', ... 'last_field_cleaned': 'foobar'}, 
    ###  },   
    ### ]
    ##############################
    def clean_for_update(self, engine):
        selectable = engine.execute(self.table.select())
        rows = selectable.fetchall()
        cleaned_rows = {}     
        for row in rows:
            pk_column = None
            # Include 'pk' for 'where' clause in UPDATE
            if self.primary_key_column == None:
                raise NotImplemented("Currently, a table w/o a primary key cannot be updated...skipping for now...(i.e. association tables)") 
            else:
                pk_column = self.primary_key_column
            cleaned_row = { "pk": (pk_column, row[self.primary_key_column]),
                            "row" : {} }
            for (col, value) in row.items():
                cleaner = self.cleaners.get(col)
                if not cleaner:
                    ####################
                    ## We don't care about
                    ## rows we aren't cleaning..
                    ####################
                    continue
                cleaned_val = cleaner.clean(value)
                cleaned_row['row'][col] = cleaned_val
            cleaned_rows.append(cleaned_row)
        return cleaned_rows

    def clean(self, rows): #Clean in place rows...
        change_count_dict = {} #Map column name, to # changes
        for row in rows:
            for (col, value) in row.items():
                cleaners = self.cleaners.get(col)
                if cleaners != None:
                    for c in cleaners:
                        cleaned_val = c.clean(value)
                        if cleaned_val != value:
                            row[col] = cleaned_val
                            if change_count_dict.get(col):
                                change_count_dict[col] += 1
                            else:
                                change_count_dict[col] = 1
        for col in change_count_dict.keys():
            self.logger.info("Column '" + col + "' was cleaned in '" + str(change_count_dict[col]) + "' rows.")
    
    def clean_for_insert(self, engine, clean=True):
        selectable = engine.execute(self.table.select())
        rows = selectable.fetchall()
        insertable_rows = []
        count = 0
        for row in rows:
            count += 1
            if count % 1000 == 0:
                pass #print "Loaded "+ str(count) + " rows..."
            new_row = {}
            for (col, value) in row.items():
                if clean:
                    cleaners = self.cleaners.get(col)
                    if cleaners != None: #clean it
                       changes = []
                       for c in cleaners:
                          cleaned_val = c.clean(value)
                          if cleaned_val != value:
                              changes.append(cleaned_val)
                              new_row[col] = cleaned_val
                       if len(changes) > 0:
                           self.logger.warning("'" + str(len(changes)) + "' changes made to '" + str(value) + "\nChanges: " + str(changes))
                    else: #leave it alone
                       new_row[col] = value
                else:
                    new_row[col] = value
            insertable_rows.append(new_row)
        return insertable_rows


