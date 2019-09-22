# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.RDBDataTable import RDBDataTable
from pymysql import IntegrityError
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


def run_tests():
    rdb_table = create_RDBDataTable_instance()

    ## FIND BY TEMPLATE TESTS ##
    test_find_by_template_good(rdb_table)
    test_find_by_template_field_list_good(rdb_table)
    test_find_by_template_empty(rdb_table)

    ## FIND BY PRIMARY KEY TESTS ##
    test_find_by_primary_key_good(rdb_table)
    test_find_by_primary_key_field_list_good(rdb_table)
    test_find_by_primary_key_does_not_exist(rdb_table)
    test_find_by_primary_key_bad_field_list(rdb_table)

    ## DELETE BY PRIMARY KEY TESTS ##
    test_delete_by_key_good(rdb_table)
    test_delete_by_key_does_not_exist(rdb_table)

    ## DELETE BY TEMPLATE TESTS ##
    test_delete_by_template_good(rdb_table)
    test_delete_by_key_does_not_exist(rdb_table)

    ## UPDATE BY PRIMARY KEY TESTS ##
    test_update_by_key_good(rdb_table)
    test_update_by_key_does_not_exist(rdb_table)
    test_update_by_key_duplicate(rdb_table)

    ## UPDATE BY TEMPLATE TESTS ##
    test_update_by_template_good(rdb_table)
    test_update_by_template_does_not_exist(rdb_table)
    test_update_by_template_duplicate(rdb_table)

    ## INSERT TESTS ##
    test_insert_good(rdb_table)
    test_insert_duplicate_primary_key(rdb_table)

def create_RDBDataTable_instance():
    connect_info = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'password123',
        'db': 'lahman_db' 
    }

    key_columns = [
        'playerID',
        'yearID',
        'stint',
        'teamID'
    ]

    rdb_table = RDBDataTable('Batting', connect_info = connect_info, key_columns = key_columns)
    return rdb_table

def test_find_by_template_good(rdb_table):
    test_row = rdb_table.get_row()
    template = {
        'teamID': 'NYN',
        'SB': '1'
    } 

    result = rdb_table.find_by_template(template)
    try:
        assert(result)
        for row in result:
            assert(row['teamID'] == 'NYN')
            assert(row['SB'] == '1')
        print('TEST PASSED: find_by_template_good')
    except:
        print('TEST FAILED: find_by_template_good')

def test_find_by_template_field_list_good(rdb_table):
    test_row = rdb_table.get_row()
    template = {
        'teamID': 'NYN',
        'SB': '1'
    } 
    field_list = ['teamID', 'SB', 'playerID', 'G', 'AB', 'R']

    result = rdb_table.find_by_template(template, field_list)
    try:
        assert(result)
        for row in result:
            assert(row['teamID'] == 'NYN')
            assert(row['SB'] == '1')
            assert(len(row) == len(field_list))
            for field in field_list:
                assert field in row
        print('TEST PASSED: find_by_template_field_list_good')
    except:
        print('TEST FAILED: find_by_template_field_list_good')

def test_find_by_template_empty(rdb_table):
    test_row = rdb_table.get_row()
    template = {
        'teamID': 'DOESNOTEXISTTEAM',
        'SB': '1'
    } 

    result = rdb_table.find_by_template(template)
    try:
        assert(len(result) == 0)
        print('TEST PASSED: find_by_template_empty')
    except:
        print('TEST FAILED: find_by_template_empty')

def test_find_by_primary_key_good(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]

    result = rdb_table.find_by_primary_key(key_fields)
    try:
        assert(result)
        assert(result == test_row)
        print('TEST PASSED: find_by_primary_key_good')
    except:
        print('TEST FAILED: find_by_primary_key_good')


def test_find_by_primary_key_field_list_good(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]
    field_list = ['playerID', 'G', 'AB', 'R']

    result = rdb_table.find_by_primary_key(key_fields, field_list)
    try:
        assert(result)
        assert(len(result) == len(field_list))
        for field in field_list:
            assert(field in result)
        print('TEST PASSED: find_by_primary_key_field_list_good')
    except:
        print('TEST FAILED: find_by_primary_key_field_list_good')

def test_find_by_primary_key_does_not_exist(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = ['non-existent player id', test_row['yearID'], test_row['stint'], test_row['teamID']]

    result = rdb_table.find_by_primary_key(key_fields)
    try:
        assert(result == None)
        print('TEST PASSED: find_by_primary_key_does_not_exist')
    except:
        print('TEST FAILED: find_by_primary_key_does_not_exist')


def test_find_by_primary_key_bad_field_list(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]
    field_list = ['doesnotexist', 'a;jlsdkfj', '00', 'a;lkj']

    result = rdb_table.find_by_primary_key(key_fields, field_list)
    try:
        assert(result == None)
        print('TEST PASSED: find_by_primary_key_bad_field_list')
    except:
        print('TEST FAILED: find_by_primary_key_bad_field_list')

def test_delete_by_key_good(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]

    try:
        assert(rdb_table.delete_by_key(key_fields) == 1)
        assert(rdb_table.find_by_primary_key(key_fields) == None)
        print('TEST PASSED: delete_by_key_good')
    except:
        print('TEST FAILED: delete_by_key_good')


def test_delete_by_key_does_not_exist(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = ['DOESNOTEXIST ', test_row['yearID'], test_row['stint'], test_row['teamID']]

    try:
        assert(rdb_table.delete_by_key(key_fields) == 0)
        print('TEST PASSED: delete_by_key_does_not_exist')
    except:
        print('TEST FAILED: delete_by_key_does_not_exist')

def test_delete_by_template_good(rdb_table):
    template = {
        'teamID': 'NYN',
        'SB': '2'
    } 

    try:
        expected_rows = rdb_table.find_by_template(template)
        assert(rdb_table.delete_by_template(template) == len(expected_rows))
        assert(len(rdb_table.find_by_template(template)) == 0)
        print('TEST PASSED: delete_by_template_good')
    except:
        print('TEST FAILED: delete_by_template_good')

def test_delete_by_template_does_not_exist(rdb_table):
    template = {
        'teamID': 'TEAMDOESNOTEXIST',
        'SB': '1'
    } 

    try:
        assert(rdb_table.delete_by_template(template) == 0)
        print('TEST PASSED: delete_by_template_does_not_exist')
    except:
        print('TEST FAILED: delete_by_template_does_not_exist')


def test_update_by_key_good(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]
    new_values = {
        'G': '1000',
        'H': '10000'
    }

    try:
        assert(rdb_table.update_by_key(key_fields, new_values) == 1)
        updated_row = rdb_table.find_by_primary_key(key_fields)
        assert(updated_row['G'] == '1000')
        assert(updated_row['H'] == '10000')
        print('TEST PASSED: update_by_key_good')
    except:
        print('TEST FAILED: update_by_key_good')

def test_update_by_key_does_not_exist(rdb_table):
    test_row = rdb_table.get_row()
    key_fields = ['DOESNOTEXIST', test_row['yearID'], test_row['stint'], test_row['teamID']]
    new_values = {
        'G': '1000',
        'H': '10000'
    }

    try:
        assert(rdb_table.update_by_key(key_fields, new_values) == 0)
        print('TEST PASSED: update_by_key_does_not_exist')
    except:
        print('TEST FAILED: update_by_key_does_not_exist')

def test_update_by_key_duplicate(rdb_table):
    test_row = rdb_table.get_row()
    test_other_row = test_row 
    while test_row == test_other_row:
        test_other_row = rdb_table.get_row()
    key_fields = [test_row['playerID'], test_row['yearID'], test_row['stint'], test_row['teamID']]
    new_values = {
        'playerID': test_other_row['playerID'],
        'yearID' : test_other_row['yearID'],
        'stint' : test_other_row['stint'],
        'teamID' : test_other_row['teamID']
    }

    try:
        rdb_table.update_by_key(key_fields, new_values)
        print('TEST FAILED: update_by_key_duplicate')
    except IntegrityError:
        print('TEST PASSED: update_by_key_duplicate')

def test_update_by_template_good(rdb_table):
    template = {
        'teamID': 'NYN',
        'SB': '1'
    } 
    new_values = {
        'G': '1000',
        'H': '10000'
    }

    try:
        expected_rows = rdb_table.find_by_template(template)
        assert(rdb_table.update_by_template(template, new_values) == len(expected_rows))
        updated_rows = rdb_table.find_by_template(template)
        for row in updated_rows:
            assert(row['G'] == '1000')
            assert(row['H'] == '10000')
        print('TEST PASSED: update_by_template_good')
    except:
        print('TEST FAILED: update_by_template_good')

def test_update_by_template_does_not_exist(rdb_table):
    template = {
        'teamID': 'DOESNOTEXIST',
        'SB': '1'
    } 
    new_values = {
        'G': '1000',
        'H': '10000'
    }

    try:
        assert(rdb_table.update_by_template(template, new_values) == 0)
        print('TEST PASSED: update_by_template_does_not_exist')
    except:
        print('TEST FAILED: update_by_template_does_not_exist')

def test_update_by_template_duplicate(rdb_table):
    template = {
        'teamID': 'NYN',
        'H': '1'
    } 
    new_values = {
        'playerID': '1234',
        'yearID': '2012',
        'stint': 1,
        'teamID': 'NYN'
    }

    try:
        rdb_table.update_by_template(template, new_values)
        print('TEST FAILED: update_by_template_duplicate')
    except IntegrityError:
        print('TEST PASSED: update_by_template_duplicate')

def test_insert_good(rdb_table):
    new_record = {
		"playerID" : "1234567",
		"yearID" : "2000",
		"stint" : "1",
		"teamID" : "NYN",
		"lgID" : "NL",
		"G" : "79",
		"AB" : "157",
		"R" : "22",
		"H" : "34",
		"2B" : "7",
		"3B" : "1",
		"HR" : "6",
		"RBI" : "12",
		"SB" : "1",
		"CS" : "1",
		"BB" : "14",
		"SO" : "51",
		"IBB" : "2",
		"HBP" : "1",
		"SH" : "0",
		"SF" : "1",
		"GIDP" : "2"
	}

    rdb_table.insert(new_record)

    try:
        key_fields = [new_record['playerID'], new_record['yearID'], new_record['stint'], new_record['teamID']]
        assert(rdb_table.find_by_primary_key(key_fields) == new_record)
        print('TEST PASSED: insert_good')
    except:
        print('TEST FAILED: insert_good')


def test_insert_duplicate_primary_key(rdb_table):
    duplicate_record = {
		"playerID" : "abbotku01",
		"yearID" : "2000",
		"stint" : "1",
		"teamID" : "NYN",
		"lgID" : "NL",
		"G" : "79",
		"AB" : "157",
		"R" : "22",
		"H" : "34",
		"2B" : "7",
		"3B" : "1",
		"HR" : "6",
		"RBI" : "12",
		"SB" : "1",
		"CS" : "1",
		"BB" : "14",
		"SO" : "51",
		"IBB" : "2",
		"HBP" : "1",
		"SH" : "0",
		"SF" : "1",
		"GIDP" : "2"
	}

    try:
        rdb_table.insert(duplicate_record)
        print('TEST FAILED: insert_duplicate_primary_key')
    except IntegrityError:
        print('TEST PASSED: insert_duplicate_primary_key')



run_tests()