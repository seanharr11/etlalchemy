from etlalchemy.schema_transformer import SchemaTransformer

col_hdrs = ['Column Name','Table Name',
            'New Column Name','New Column Type','Delete']
col_sample_data = [
    col_hdrs,
    ['middle_name','employees','','','True'],
    ['fired','employees','','Boolean','False'],
    ['birth_date','employees','dob','',''],
    ['salary','jobs','payrate','','False'],
        ]
def setup_column_transform_file(tmpdir, data=[]):
    f = tmpdir.join("sample_column_mappings.csv")
    file_data = []
    for row in data:
        file_data.append(','.join(row))
    file_data_str = '\n'.join(file_data)
    f.write(file_data_str)
    # f.write_text?
    assert f.read() == file_data_str
    return str(f) # filename

tbl_hdrs = ['Table Name','New Table Name','Delete']
tbl_sample_data = [
    tbl_hdrs,
    ['table_to_rename','new_table_name','False'],
    ['table_to_delete','','True'],
    ['departments','dept','False'],
        ]
def setup_table_transform_file(tmpdir, data=[]):
    f = tmpdir.join("sample_table_mappings.csv")
    file_data = []
    for row in data:
        file_data.append(','.join(row))
    file_data_str = '\n'.join(file_data)
    f.write(file_data_str)
    # f.write_text?
    assert f.read() == file_data_str
    return str(f) # filename

def mock_dictreader(headers, data):
    return dict(zip(headers, data))


def test_init_args_empty():
    trans = SchemaTransformer(column_transform_file=None, table_transform_file=None)
    assert trans is not None
    assert trans.global_renamed_col_suffixes == {}

def test_init_global_only():
    test_col_suffixes = {'org': 'chg'}
    trans = SchemaTransformer(column_transform_file=None,
            table_transform_file=None,
            global_renamed_col_suffixes=test_col_suffixes)
    assert trans is not None
    assert trans.global_renamed_col_suffixes == test_col_suffixes

def test_column_transformation_delete():
    """Test the allowed values for delete in column transformation file"""
    test_cases = {
        # Delete Value: the expected result
        'True': True, # The first 3 are the only ones true based on code
        'true': True,
        '1': True,
        'Y': False,   # ! should this be true?
        'yes': False, # ! should this be true?
        'delete': False, # ! should this be true?
        '': False,
        '0': False,
        'False': False,
        'false': False,
        'unknown': False,
    }
    row = mock_dictreader(col_hdrs, ['middle_name','employees','','','True'])
    for k in test_cases:
        row['Delete'] = k
        c = SchemaTransformer.ColumnTransformation(row)
        assert c
        assert c.old_column == 'middle_name'
        assert c.old_table == 'employees'
        assert c.new_column == ''
        assert c.new_type == ''
        assert c.delete == test_cases[k]

def test_column_transformation_rename():
    row = mock_dictreader(col_hdrs, ['birth_date','employees','dob','',''])
    c = SchemaTransformer.ColumnTransformation(row)
    assert c
    assert c.old_column == 'birth_date'
    assert c.old_table == 'employees'
    assert c.new_column == 'dob' # <=== The actual test
    assert c.new_type == ''
    assert c.delete == False

    row['New Column Name']=''
    c = SchemaTransformer.ColumnTransformation(row)
    assert c
    assert c.old_column == 'birth_date'
    assert c.old_table == 'employees'
    assert c.new_column == '' # <==== Should be blank
    assert c.new_type == ''
    assert c.delete == False

def test_column_transformation_tables():
    row = mock_dictreader(col_hdrs, ['fired','employees','','Boolean','False'])
    c = SchemaTransformer.ColumnTransformation(row)
    assert c
    assert c.old_table == 'employees'
    assert str(c) == 'employees.fired'
    row = mock_dictreader(col_hdrs, ['salary','jobs','payrate','','False'])
    c = SchemaTransformer.ColumnTransformation(row)
    assert c
    assert c.old_table == 'jobs'
    assert str(c) == 'jobs.salary'

def test_column_transformation_type():
    row = mock_dictreader(col_hdrs, ['fired','employees','','Boolean','False'])
    c = SchemaTransformer.ColumnTransformation(row)
    assert c
    assert c.new_type == 'Boolean'

def test_table_transformation_rename():
    row = mock_dictreader(tbl_hdrs, ['departments','dept','False'])
    t = SchemaTransformer.TableTransformation(row)
    assert t.old_table == 'departments'
    assert t.new_table == 'dept'
    assert t.delete == False

def test_table_transformation_delete():
    """Test the allowed values for delete in table transformation file"""
    test_cases = {
        # Delete Value: the expected result
        'True': True, # The first 3 are the only ones true based on code
        'true': True,
        '1': True,
        'Y': False,   # ! should this be true?
        'yes': False, # ! should this be true?
        'delete': False, # ! should this be true?
        '': False,
        '0': False,
        'False': False,
        'false': False,
        'unknown': False,
    }
    row = mock_dictreader(tbl_hdrs, ['table_to_delete','new_name','True'])
    for k in test_cases:
        row['Delete'] = k
        t = SchemaTransformer.TableTransformation(row)
        assert t
        assert t.old_table == 'table_to_delete'
        assert t.new_table == 'new_name' # ! should this be removed?
        assert t.delete == test_cases[k]

def test_needsfiles(tmpdir):
    """Make sure we can create, save and remove temporary files"""
    f = tmpdir.join("testfile.txt")
    f.write("can write")
    assert len(tmpdir.listdir()) == 1
    assert f.read() == "can write"
    f.remove()
    assert len(tmpdir.listdir()) == 0

def test_init_column_transform_file_empty(tmpdir):
    col_map = setup_column_transform_file(tmpdir)
    trans = SchemaTransformer(column_transform_file=col_map,
            table_transform_file=None)
    assert trans is not None
    assert len(trans.column_transformations) == 0

def test_init_column_transform_file(tmpdir):
    col_map = setup_column_transform_file(tmpdir, data=col_sample_data)
    trans = SchemaTransformer(column_transform_file=col_map,
            table_transform_file=None)
    assert trans is not None
    assert len(trans.table_transformations) == 0
    assert len(trans.column_transformations) > 0
    # Get unique tables from sample data, slice out the header row
    unique_tables = set([c[1] for c in [row for row in col_sample_data[1:]]])
    assert len(trans.column_transformations) == len(unique_tables)

def test_init_table_transform_file(tmpdir):
    tbl_map = setup_table_transform_file(tmpdir, data=tbl_sample_data)
    trans = SchemaTransformer(column_transform_file=None,
            table_transform_file=tbl_map)
    assert trans is not None
    assert len(trans.column_transformations) == 0
    # Get unique tables from sample data, slice out the header row
    unique_tables = set([t[0] for t in [row for row in tbl_sample_data[1:]]])
    assert len(trans.table_transformations) == len(unique_tables)

def test_schedule_deletion_of_column(tmpdir):
    col_map = setup_column_transform_file(tmpdir, data=col_sample_data)
    trans = SchemaTransformer(column_transform_file=col_map,
            table_transform_file=None)
    # Get unique tables from sample data, slice out the header row
    unique_tables = set([c[1] for c in [row for row in col_sample_data[1:]]])
    total_tables = len(unique_tables)

    ### Remove a column in new table (compared to sample data)
    assert trans.column_transformations.get('dept') is None
    trans.schedule_deletion_of_column('manager','dept')
    assert trans.column_transformations.get('dept') is not None
    assert trans.column_transformations['dept'].get('manager') is not None
    assert trans.column_transformations['dept'].get('manager').delete
    # Confirm list has been added to
    total_tables += 1
    assert len(trans.column_transformations) == total_tables

    ### Remove a column known in a different table
    trans.schedule_deletion_of_column('birth_date', 'bosses')
    # Make sure it didn't change employees.birth_date
    assert trans.column_transformations['employees'].get('birth_date').delete == False
    assert trans.column_transformations['bosses'].get('birth_date').delete
    total_tables += 1
    assert len(trans.column_transformations) == total_tables

    ### Remove a known column in known table (in sample data)
    # Birth_date already has transformation to dob, but isn't to be deleted
    assert trans.column_transformations['employees'].get('birth_date') is not None
    assert trans.column_transformations['employees'].get('birth_date').delete == False
    num_cols = len(trans.column_transformations['employees'])
    trans.schedule_deletion_of_column('birth_date','employees')
    # Confirm it changed to deleting it
    assert trans.column_transformations['employees'].get('birth_date').delete
    # make sure it did not change the list of employees transformations
    assert len(trans.column_transformations['employees']) == num_cols

    ### Remove a new column in known table (in sample data)
    num_cols = len(trans.column_transformations['employees'])
    trans.schedule_deletion_of_column('title','employees')
    assert trans.column_transformations['employees'].get('title').delete
    # make sure it added to the employees transformations
    assert len(trans.column_transformations['employees']) == num_cols + 1
    # make sure it din't change how many tables
    assert len(trans.column_transformations) == total_tables

def test_transform_table(tmpdir):
    # TODO implement tests
    # I think it would be preferable for transform_table to
    # return the altered SQLAlchemy Table object instead of having a
    # strange side effect of renaming. It could return None for delete
    assert 0

def test_transform_column(tmpdir):
    # TODO implement tests
    assert 0

def test_transform_rows(tmpdir):
    # TODO implement tests
    assert 0
