import logging
import csv
import sqlalchemy


class SchemaTransformer():

    class TableTransformation():
        def __init__(self, stRow):
            self.delete = stRow['Delete'].lower() in ["true", "1"]
            self.old_table = stRow['Table Name']
            self.new_table = stRow['New Table Name']

        def __str__(self):
            return "({0} -> {1}...Delete = {2})".\
                format(self.old_table, self.new_table, str(self.delete))

    class ColumnTransformation():
        def __init__(self, stRow):
            self.delete = stRow['Delete'].lower() in ["true", "1"]
            self.old_table = stRow['Table Name']
            self.old_column = stRow['Column Name']
            self.new_column = stRow['New Column Name']
            self.new_type = stRow['New Column Type']

        def _new_type(self):
            return getattr(sqlalchemy.types, self.new_type)

        def __str__(self):
            return self.old_table + "." + self.old_column
    def schedule_deletion_of_column(self, col, table):
        st = self.ColumnTransformation({
            'Delete': "true",
            'Table Name': table,
            'Column Name': col,
            'New Column Name': '',
            'New Column Type': ''
        })
        self.logger.info("Scheduling '{0}' to be deleted due to column being empty".format(col))
        if not self.column_transformations.get(st.old_table):
            # No column transformations exist for the table
            self.column_transformations[st.old_table] = {}
            self.column_transformations[st.old_table][st.old_column] = st
        elif self.column_transformations[st.old_table].get(st.old_column):
            # There ALREADY EXISTS a transformation on this column, UPDATE IT
            self.column_transformations[st.old_table][st.old_column].delete = True
            self.column_transformations[st.old_table][st.old_column] = st
        else:
            # Transformations exist on the table, not nothing on the column
            self.column_transformations[st.old_table][st.old_column] = st

    def __init__(self, column_transform_file,
                 table_transform_file, global_renamed_col_suffixes={}):
        self.logger = logging.getLogger("schema-transformer")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s (%(levelname)s) - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.column_transformations = {}
        self.table_transformations = {}
        self.failed_transformations = set([])
        self.logger.propagate = False
        self.global_renamed_col_suffixes = global_renamed_col_suffixes
        # Load column mappings
        if column_transform_file:
            with open(column_transform_file, "rU") as fp:
                dr = csv.DictReader(fp)
                for row in dr:
                    st = self.ColumnTransformation(row)
                    if not self.column_transformations.get(st.old_table):
                        self.column_transformations[st.old_table] = {}
                    self.column_transformations[st.old_table][st.old_column] = st
        # Load table mappings
        if table_transform_file:
            with open(table_transform_file, "rU") as fp:
                dr = csv.DictReader(fp)
                for row in dr:
                    st = self.TableTransformation(row)
                    self.table_transformations[st.old_table] = st
    
    # Returns False if deleted...
    def transform_table(self, table):
        thisTableTT = self.table_transformations.get(table.name.lower())
        # Update table name
        if thisTableTT:
            if thisTableTT.delete:
                return False
            if thisTableTT.new_table not in ["", None]:
                self.logger.info(
                    " ----> Renaming table '{0}' to '{1}'"
                    .format(table.name, thisTableTT.new_table))
                table.name = thisTableTT.new_table
                return True
        return True
    # Returns 'True' if an action is defined for the column...

    def transform_column(self, C, tablename, columns):
        # Find Column...
        this_table_st = self.column_transformations.get(tablename)
        initial_column_name = C.name
        action_applied = False
        idx = columns.index(C.name)

        if this_table_st:
            st = this_table_st.get(C.name)
            if st:
                if st.delete:
                    # Remove the column from the list of columns...
                    del columns[idx]
                    action_applied = True
                else:
                    # Rename the column if a "New Column Name" is specificed
                    if st.new_column not in ["", None]:
                        self.logger.info(
                            " ----> Renaming column '{0}' => '{1}'"
                            .format(C.name, st.new_column))
                        C.name = st.new_column
                        columns[idx] = C.name
                        action_applied = True
                    # Change the type of the column if a
                    # "New Column Type" is specified
                    if st.new_type not in ["", None]:
                        old_type = C.type.__class__.__name__
                        try:
                            C.type = st._new_type()
                        except Exception as e:
                            self.logger.critical(
                                "** Couldn't change column type of " +
                                "'{0}' to '{1}'**".
                                format(C.name, st.new_type))
                            self.logger.critical(e)
                            raise e
                    else:
                        self.logger.warning(
                            "Schema transformation defined for " +
                            "column '{0}', but no action was " +
                            "taken...".format(C.name))

        if not action_applied:
            # Then the column had no 'action' applied to it...
            for k in self.global_renamed_col_suffixes.keys():
                # Check if column name ends with specfiic suffix
                if initial_column_name.lower().endswith(k.lower()):
                    self.logger.info(
                        " ---> Renaming column '{0}' to GLOBAL " +
                        " default '{1}' because it contains '{2}'"
                        .format(initial_column_name.lower(),
                                initial_column_name.replace(
                                    k, self.global_renamed_col_suffixes[k]),
                                k.lower()))
                    C.name = initial_column_name.replace(
                            k, self.global_renamed_col_suffixes[k])
                    columns[idx] = C.name
        return columns

    def transform_rows(self, rows, columns, tablename):
        this_table_st = self.column_transformations.get(tablename)
        bool_dict = {
                'Y': True,
                'N': False,
                1: True,
                0: False,
                '1': True,
                '0': False,
                'y': True,
                'n': False,
        }
        if this_table_st is None:
            return
        column_transformers = []
        for c in columns:
            if this_table_st.get(c):
                column_transformers.append(this_table_st.get(c))
            else:
                column_transformers.append(None)
        number_columns = len(columns)
        for r in rows:
            for i in range(number_columns-1, -1 ,-1):
                column_transformer = column_transformers[i]
                if column_transformer:
                    # Then there is a transformation defined for this column...
                    if column_transformer.delete:
                        del r[i]
                    elif st.new_type in [None, ""]:
                        continue
                    # Handle type conversions here...
                    elif st.new_type == "Integer":
                            r[idx] = int(r[idx])
                    elif st.new_type in ["String", "Text"]:
                        r[idx] = str(r[idx])
                    elif st.new_type in ["Float", "Decimal"]:
                        r[idx] = float(r[idx])
                    elif st.new_type == "Boolean":
                        r[idx] = bool_dict[r[idx]]
                
  
