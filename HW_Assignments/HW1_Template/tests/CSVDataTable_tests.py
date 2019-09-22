# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable, DataTableException, DuplicatePrimaryKeyException
import logging
import os
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("Data/Baseball")


template_example = {
    'birthYear': '1981',
    'bats': 'R',
}

def run_tests():
    csv_table = t_load()
    csv_table_p_key = t_load_p_key()

    ## FIND BY TEMPLATE TESTS ##
    test_find_by_template_good(csv_table)
    test_find_by_template_field_list_good(csv_table)
    test_find_by_template_bad(csv_table)
    test_find_by_template_empty(csv_table)

    ## FIND BY PRIMARY KEY TESTS ##
    test_find_by_primary_key_good(csv_table_p_key)
    test_find_by_primary_key_does_not_exist(csv_table_p_key)
    test_find_by_primary_key_field_list_good(csv_table_p_key)
    test_find_by_primary_key_bad_type(csv_table_p_key)
    test_find_by_primary_key_bad_key_fields(csv_table_p_key)

    ## DELETE BY KEY TESTS ##
    test_delete_by_key_good(csv_table_p_key)
    test_delete_by_key_does_not_exist(csv_table_p_key)

    ## DELETE BY TEMPLATE TESTS ##
    test_delete_by_template_good(csv_table)
    test_delete_by_template_empty(csv_table)

    ## RELOAD TABLES AFTER EDITING THEM ##
    csv_table = t_load()
    csv_table_p_key = t_load_p_key()

    ## UPDATE BY KEY TESTS ##
    test_update_by_key_good(csv_table_p_key)
    test_update_by_key_bad_type(csv_table_p_key)
    test_update_by_key_does_not_exist(csv_table_p_key)
    test_update_by_key_duplicate(csv_table_p_key)

    ## UPDATE BY TEMPLATE TESTS ##
    test_update_by_template_good(csv_table_p_key)
    test_update_by_template_does_not_exist(csv_table_p_key)
    test_update_by_template_duplicate(csv_table_p_key)

    ## INSERT TESTS ##
    test_insert_good(csv_table_p_key)
    test_insert_bad_record(csv_table_p_key)
    test_insert_duplicate_primary_key(csv_table_p_key)


def t_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    return csv_tbl

def t_load_p_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ['playerID'])

    return csv_tbl


def test_find_by_template_good(csv_tbl):

    result = csv_tbl.find_by_template(template_example)

    try:
        assert(len(result) > 0)
        for r in result:
            assert(csv_tbl.matches_template(r, template_example))
        print('TEST PASSED: find_by_template_good')
    except AssertionError:
        print('TEST FAILED: find_by_template_good')



def test_find_by_template_field_list_good(csv_tbl):
    field_list = [
        'playerID',
        'nameLast',
        'debut'
    ]

    result = csv_tbl.find_by_template(template_example, field_list)

    try:
        assert(len(result) > 0)
        for r in result:
            assert(len(r) == 3)
            for field in field_list:
                assert(field in r)
        print('TEST PASSED: find_by_template_field_list_good')
    except AssertionError as e:
        print('TEST FAILED: find_by_template_field_list_good')
        raise

def test_find_by_template_empty(csv_tbl):
    template = {
        'fieldDoesNotExist': '1234',
    }

    result = csv_tbl.find_by_template(template)

    try:
        assert(len(result) == 0)
        print('TEST PASSED: find_by_template_empty')
    except AssertionError:
        print('TEST FAILED: find_by_template_empty')


def test_find_by_template_bad(csv_tbl):
    template = ['this is an invalid type for template'] 

    try:
        result = csv_tbl.find_by_template(template)
        print('TEST FAILED: find_by_template_bad')
    except:
        print('TEST PASSED: find_by_template_bad')

def test_find_by_primary_key_good(csv_table_p_key):
    key_fields = ['zupofr01']

    try:
        result = csv_table_p_key.find_by_primary_key(key_fields)
        assert(result)
        assert(result['playerID'] == key_fields[0])
        print('TEST PASSED: find_by_primary_key_good')
    except:
        print('TEST FAILED: find_by_primary_key_good')


def test_find_by_primary_key_field_list_good(csv_table_p_key):
    key_fields = ['zupofr01']
    field_list = [
        'playerID',
        'birthDay',
        'deathYear'
    ]

    try:
        result = csv_table_p_key.find_by_primary_key(key_fields, field_list)
        assert(result)
        assert(result['playerID'] == key_fields[0])
        assert(len(result) == 3)
        for field in field_list:
            assert(field in result)
        print('TEST PASSED: find_by_primary_key_field_list_good')
    except:
        print('TEST FAILED: find_by_primary_key_field_list_good')

def test_find_by_primary_key_does_not_exist(csv_table_p_key):
    key_fields = ['ThisPlayerIDDoesNotExist']

    try:
        result = csv_table_p_key.find_by_primary_key(key_fields)
        assert(not result)
        print('TEST PASSED: find_by_primary_key_does_not_exist')
    except:
        print('TEST FAILED: find_by_primary_key_does_not_exist')


def test_find_by_primary_key_bad_type(csv_table_p_key):
    # This function tests when two or more rows have the same primary key
    key_fields = 'INVALID TYPE'
    try:
        result = csv_table_p_key.find_by_primary_key(key_fields)
        print('TEST FAILED: find_by_primary_key_bad_type')
    except DataTableException:
        print('TEST PASSED: find_by_primary_key_bad_type')

def test_find_by_primary_key_bad_key_fields(csv_table_p_key):
    # This function tests when two or more rows have the same primary key
    key_fields = ['too many', 'fields', 'should only have 1 field for p_key'] 

    try:
        result = csv_table_p_key.find_by_primary_key(key_fields)
        print('TEST FAILED: find_by_primary_key_bad_key_fields')
    except DataTableException:
        print('TEST PASSED: find_by_primary_key_bad_key_fields')

def test_delete_by_key_good(csv_table_p_key):
    key_fields = ['zupofr01']

    try:
        assert(csv_table_p_key.find_by_primary_key(key_fields))
        assert(csv_table_p_key.delete_by_key(key_fields) == 1)
        assert(csv_table_p_key.find_by_primary_key(key_fields) == None)
        print('TEST PASSED: delete_by_key_good')
    except:
        print('TEST FAILED: delete_by_key_good')


def test_delete_by_key_does_not_exist(csv_table_p_key):
    key_fields = ['ThisPlayerIDDoesNotExist']

    try:
        assert(csv_table_p_key.delete_by_key(key_fields) == 0)
        print('TEST PASSED: delete_by_key_does_not_exist')
    except:
        print('TEST FAILED: delete_by_key_does_not_exist')

def test_delete_by_template_good(csv_table):
    rows_to_delete_expected = csv_table.find_by_template(template_example)

    try:
        assert(csv_table.delete_by_template(template_example) == len(rows_to_delete_expected))
        assert(len(csv_table.find_by_template(template_example)) == 0)
        print('TEST PASSED: delete_by_template_good')
    except:
        print('TEST FAILED: delete_by_template_good')

def test_delete_by_template_empty(csv_table):
    template_empty = {
        'InvalidField': '1234'
    }

    try:
        result = csv_table.delete_by_template(template_empty)
        assert(result == 0)
        print('TEST PASSED: delete_by_template_empty')
    except:
        print('TEST FAILED: delete_by_template_empty')

def test_update_by_key_good(csv_table_p_key):
    key_fields = ['zupofr01']
    new_values = {
        'nameFirst': 'Max',
        'nameLast': 'Amsterdam'
    }

    try:
        assert(csv_table_p_key.update_by_key(key_fields, new_values) == 1)
        updated_row = csv_table_p_key.find_by_primary_key(key_fields)
        for field, new_value in new_values.items():
            if field in updated_row:
                assert(updated_row[field] == new_value)
        print('TEST PASSED: update_by_key_good')
    except Exception as e:
        print('TEST FAILED: update_by_key_good')

def test_update_by_key_bad_type(csv_table_p_key):
    key_fields = ['zupofr01']
    new_values = ['This is the wrong type'] 
    try:
        assert(csv_table_p_key.update_by_key(key_fields, new_values) == 1)
        print('TEST FAILED: update_by_key_bad_type')
    except DataTableException:
        print('TEST PASSED: update_by_key_bad_type')

def test_update_by_key_does_not_exist(csv_table_p_key):
    key_fields = ['This player id does not exist']
    new_values = {
        'nameFirst': 'Max',
        'nameLast': 'Amsterdam'
    }

    try:
        assert(csv_table_p_key.update_by_key(key_fields, new_values) == 0)
        print('TEST PASSED: update_by_key_does_not_exist')
    except:
        print('TEST FAILED: update_by_key_does_not_exist')

def test_update_by_key_duplicate(csv_table_p_key):
    key_fields = ['zupofr01']
    new_values = {
        'playerID': 'aaronha01'
    }

    try:
        csv_table_p_key.update_by_key(key_fields, new_values)
        print('TEST FAILED: update_by_key_duplicate')
    except DuplicatePrimaryKeyException:
        print('TEST PASSED: update_by_key_duplicate')


def test_update_by_template_good(csv_table_p_key):
    new_values = {
        'nameFirst': 'Max',
        'nameLast': 'Amsterdam'
    }

    rows_to_update_expected = csv_table_p_key.find_by_template(template_example)

    # Getting indecies of the rows to update, so that after they are updated can 
    # check to make sure they were updated accordingly 
    indecies = []
    rows = csv_table_p_key.get_rows()
    for row in rows_to_update_expected:
        indecies.append(rows.index(row))

    try:
        assert(csv_table_p_key.update_by_template(template_example, new_values) == len(rows_to_update_expected))

        rows = csv_table_p_key.get_rows()
        for index in indecies:
            curr_row = rows[index]
            for field, new_value in new_values.items():
                if field in curr_row:
                    assert(curr_row[field] == new_value)
        print('TEST PASSED: update_by_template_good')
    except Exception as e:
        print('TEST FAILED: update_by_template_good')

def test_update_by_template_does_not_exist(csv_table_p_key):
    template = {
        'Field does not exist': '1234'
    }
    new_values = {
        'nameFirst': 'Max',
        'nameLast': 'Amsterdam'
    }

    try:
        assert(csv_table_p_key.update_by_template(template, new_values) == 0)
        print('TEST PASSED: update_by_template_does_not_exist')
    except:
        print('TEST FAILED: update_by_template_does_not_exist')

def test_update_by_template_duplicate(csv_table_p_key):
    template = {
        'playerID': 'zupofr01'
    }
    new_values = {
        'playerID': 'aaronha01'
    }

    try:
        csv_table_p_key.update_by_template(template, new_values)
        print('TEST FAILED: update_by_key_duplicate')
    except DuplicatePrimaryKeyException:
        print('TEST PASSED: update_by_key_duplicate') 

def test_insert_good(csv_table_p_key):
    new_record = {
        "playerID": "abcdef001",
        "birthYear": "1998",
        "birthMonth": "9",
        "birthDay": "1",
        "birthCountry": "USA",
        "birthState": "PA",
        "birthCity": "Philadelphia",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Max",
        "nameLast": "Amsterdam",
        "nameGiven": "Max Amsterdam",
        "weight": "150",
        "height": "71",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01"
    }
    key_fields = ['abcdef001']

    try:
        assert(csv_table_p_key.find_by_primary_key(key_fields) == None)
        csv_table_p_key.insert(new_record)
        assert(csv_table_p_key.find_by_primary_key(key_fields))
        print('TEST PASSED: insert_good')
    except Exception as e:
        print('TEST FAILED: insert_good')

def test_insert_bad_record(csv_table_p_key):
    new_record = {
        'this': 'is',
        'invalid': 'record',
        'that': 'should',
        'result': 'in',
        'an': 'error'
    }

    try:
        csv_table_p_key.insert(new_record)
        print('TEST FAILED: insert_bad_record')
    except DataTableException:
        print('TEST PASSED: insert_bad_record')

def test_insert_duplicate_primary_key(csv_table_p_key):
    new_record_duplicate_p_key = {
        "playerID": "aardsda01",
        "birthYear": "1998",
        "birthMonth": "9",
        "birthDay": "1",
        "birthCountry": "USA",
        "birthState": "PA",
        "birthCity": "Philadelphia",
        "deathYear": "",
        "deathMonth": "",
        "deathDay": "",
        "deathCountry": "",
        "deathState": "",
        "deathCity": "",
        "nameFirst": "Max",
        "nameLast": "Amsterdam",
        "nameGiven": "Max Amsterdam",
        "weight": "150",
        "height": "71",
        "bats": "R",
        "throws": "R",
        "debut": "2004-04-06",
        "finalGame": "2015-08-23",
        "retroID": "aardd001",
        "bbrefID": "aardsda01"
    }

    try:
        csv_table_p_key.insert(new_record_duplicate_p_key)
        print('TEST FAILED: insert_duplicate_primary_key')
    except DuplicatePrimaryKeyException:
        print('TEST PASSED: insert_duplicate_primary_key')




        

    



run_tests()