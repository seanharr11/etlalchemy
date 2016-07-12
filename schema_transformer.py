import logging
import csv
import sqlalchemy

class SchemaTransformer():

    class TableTransformation():
        def __init__(self, stRow):
            self.delete = stRow['Delete'].lower() in ["true", "1"]
            self.oldTable = stRow['Table Name']
            self.newTable = stRow['New Table Name']

        def __str__(self):
            return "({0}) ".format(self.action) + self.oldTable + " => " + self.newTable
    
    class ColumnTransformation():
        def __init__(self, stRow):
            self.delete = stRow['Delete'].lower() in ["true", "1"]
            self.oldTable = stRow['Table Name']
            self.oldColumn = stRow['Column Name']
            self.newColumn = stRow['New Column Name']
            self.newType = stRow['New Column Type']
        def new_type(self):
            return getattr(sqlalchemy.types, self.newType)
        def __str__(self):
            return self.oldTable + "." + self.oldColumn
    
    
    def __init__(self, column_transform_file, table_transform_file, global_renamed_col_suffixes={}):    
        self.logger = logging.getLogger("schema-transformer")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s (%(levelname)s) - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.columnTransformations = {}
        self.tableTransformations = {}
        self.failed_transformations = set([])
        self.logger.propagate = False
        self.global_renamed_col_suffixes = global_renamed_col_suffixes
        #Load column mappings
        if column_transform_file:
            with open(column_transform_file, "rU") as fp:
                dr = csv.DictReader(fp)
                for row in dr:
                    st = self.ColumnTransformation(row)
                    if self.columnTransformations.get(st.oldTable) == None:
                        self.columnTransformations[st.oldTable] = {}
                    self.columnTransformations[st.oldTable][st.oldColumn] = st
        #Load table mappings
        if table_transform_file:
            with open(table_transform_file, "rU") as fp:
                dr = csv.DictReader(fp)
                for row in dr:
                    st = self.TableTransformation(row)
                    self.tableTransformations[st.oldTable] = st
    
    # Returns False if deleted...
    def transform_table(self, table):
        thisTableTT = self.tableTransformations.get(table.name.lower())
        # Update table name
        if thisTableTT:
            if thisTableTT.delete ==  True:
                return False
            if thisTableTT.newTable not in ["", None]:
                self.logger.info(" ----> Renaming table '{0}' to '{1}'".format(table.name, thisTableTT.newTable))
                table.name = thisTableTT.newTable
                return True
        return True
    # Returns 'True' if an action is defined for the column... 
    def transform_column(self, C, tablename, columns):
        # Find Column...
        thisTableST = self.columnTransformations.get(tablename)
        initialColumnName = C.name
        actionApplied = False
        idx = columns.index(C.name)
       
        if thisTableST:
            st = thisTableST.get(C.name)
            if st:
                if st.delete == True:
                    # Remove the column from the list of columns...
                    del columns[idx]
                    actionApplied = True
                else:
                    # Rename the column if a "New Column Name" is specificed
                    if st.newColumn not in ["", None]:
                        self.logger.info(" ----> Renaming column '{0}' => '{1}'".format(C.name, st.newColumn))
                        C.name = st.newColumn
                        columns[idx] = C.name
                        actionApplied = True
                    # Change the type of the column if a "New Column Type" is specified
                    if st.newType not in ["", None]:
                        old_type = C.type.__class__.__name__
                        try:
                            C.type = st.new_type()
                        except Exception as e:
                            self.logger.critical("**Couldn't change column type of '{0}' to '{1}'**".format(C.name, st.newType))
                            self.logger.critical(e)
                            raise e
                    else:
                        self.logger.warning("Schema transformation defined for column '{0}', but no action was taken...".format(C.name))
        
        if actionApplied == False:
            # Then the column had no 'action' applied to it...
            for k in self.global_renamed_col_suffixes.keys():
                # Check if column name ends with specfiic suffix
                if initialColumnName.lower().endswith(k.lower()):
                    self.logger.info(" ---> Renaming column '{0}' to GLOBAL default '{1}' because it contains '{2}'"\
                            .format(initialColumnName.lower(), initialColumnName.replace(k, self.global_renamed_col_suffixes[k]), k.lower()))
                    C.name = initialColumnName.replace(k, self.global_renamed_col_suffixes[k])
                    columns[idx] = C.name
        return columns

    def transform_rows(self, rows, columns, tablename):
        thisTableST = self.columnTransformations.get(tablename)
        if thisTableST == None:
            return 
        for r in rows:
            for col in columns:
                st = thisTableST.get(col)
                idx = columns.index(col)
                if st and st.action.lower() in ["delete", "rename"]:
                    # Then there is a transformation defined for this column...
                    try:
                        if st.action.lower() == "rename":
                            if thisTableST.get(st.newColumn):
                                if thisTableST[st.newColumn].action.lower() == "delete":
                                    # Then this column will be deleted later! We must avoid this!
                                    del thisTableST[st.newColumn]
                        elif st.action.lower() == "delete":
                            del r[idx]
                    except KeyError as e:     
                        raise e 
