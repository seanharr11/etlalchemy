import logging
import csv

class SchemaTransformer():

    class TableTransformation():
        def __init__(self, stRow):
            self.action = stRow['Action'].lower()
            self.oldTable = stRow['Old Table']
            self.newTable = stRow['New Table']

        def __str__(self):
            return "({0}) ".format(self.action) + self.oldTable + " => " + self.newTable
    
    class ColumnTransformation():
        def __init__(self, stRow):
            self.action = stRow['Action'].lower()
            self.oldTable = stRow['Old Table']
            self.oldColumn = stRow['Old Column']
            self.newColumn = stRow['New Column']
            self.typeOverride = stRow['Type Override']
        def newType(self):
            if self.typeOverride.lower() == "datetime":
                return Datetime()
            elif self.typeOverride.lower() == "date":
                return Date()
            else:
                return None

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

        with open(column_transform_file, "rU") as fp:
            dr = csv.DictReader(fp)
            for row in dr:
                st = self.ColumnTransformation(row)
                if self.columnTransformations.get(st.oldTable) == None:
                    self.columnTransformations[st.oldTable] = {}
                self.columnTransformations[st.oldTable][st.oldColumn] = st
        #Load table mappings
        with open(table_transform_file, "rU") as fp:
            dr = csv.DictReader(fp)
            for row in dr:
                st = self.TableTransformation(row)
                self.tableTransformations[st.oldTable] = st
    def transform_table(self, table):
        thisTableTT = self.tableTransformations.get(table.name.lower())
        # Update table name
        if thisTableTT:
            if thisTableTT.action.lower() == "rename":
                self.logger.info(" ----> Renaming table '{0}' to '{1}'".format(table.name, thisTableTT.newTable))
                table.name = thisTableTT.newTable
                return True
            elif thisTableTT.action.lower() == "delete":
                return None
        return False


    # Returns 'True' if an action is defined for the column... 
    def transform_column(self, C, tablename, columns):
        # Find Column...
        thisTableST = self.columnTransformations.get(tablename)
        initialColumnName = C.name
        actionApplied = False
        idx = columns.index(C.name)
       
        if thisTableST:
            st = thisTableST.get(C.name)
            if st and st.action.lower() in ["delete", "rename"]:
                if st.action.lower() == "delete":
                    # Remove the column from the list of columns...
                    del columns[idx]
                    actionApplied = True
                elif st.action.lower() == "rename":
                    self.logger.info(" ----> Renaming column '{0}' => '{1}'".format(C.name, st.newColumn))
                    C.name = st.newColumn
                    columns[idx] = C.name
                    actionApplied = True
            else:
                if st:
                    self.logger.warning(" ----> Action '{0}' not yet implemented, ignoring...".format(st.action))
        
        if actionApplied == False:
            # Then the column had no 'action' applied to it...
            for k in self.global_renamed_col_suffixes.keys():
                # Check if column name ends with specfiic suffix
                if initialColumnName.lower().endswith(k.lower()):
                    self.logger.info(" ---> Renaming column '{0}' to GLOBAL default '{1}' because it contains '{2}'".format(initialColumnName.lower(), self.global_renamed_col_suffixes[k], k.lower()))
                    C.name = self.global_renamed_col_suffixes[k]
                    columns[idx] = C.name
        ############################
        ### Now update the type ###
        ############################
        if thisTableST and thisTableST.get(initialColumnName):
            st = thisTableST.get(initialColumnName)
            if st.newType():
                C.type = st.newType()
    
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
