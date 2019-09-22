from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def get_fields_for_row(self, row, field_list):
        if not field_list:
            return row
        else:
            d = dict()
            for field in field_list:
                if field in row:
                    d[field] = row[field]
            return d

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if type(key_fields) != list:
            raise DataTableException('Key Fields must be a list')

        result = []
        key_columns = self._data['key_columns']
        
        if not key_columns:
            raise DataTableException('This table does not have a primary key')

        if len(key_fields) != len(key_columns):
            raise DataTableException('The number of key fields must match the numebr of keys that make up the primary key')

        for r in self._rows:
            matches = True
            for i in range(len(key_columns)):
                if r[key_columns[i]] != key_fields[i]:
                    matches = False
                    break
            if matches:
                result.append(r)

        if len(result) == 0:
            return None
        if len(result) > 1:
            raise DataTableException('Data is not valid. A primary key should only match one row exactly.')
        else:
            return self.get_fields_for_row(result[0], field_list)



    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        if type(template) != dict:
            raise DataTableException('Template must be a dictionary.')

        result = list()

        for r in self._rows:
            if self.matches_template(r, template):
                result.append(self.get_fields_for_row(r, field_list))

        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        row_to_delete = self.find_by_primary_key(key_fields)

        if not row_to_delete:
            return 0 

        self._rows.remove(row_to_delete)
        return 1


    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        rows_to_delete = self.find_by_template(template)

        num_deleted = 0
        for row in rows_to_delete:
            self._rows.remove(row)
            num_deleted += 1
        return num_deleted

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if type(new_values) != dict:
            raise DataTableException('new_values must be a dictionary')


        # The actual updating
        row_to_update = self.find_by_primary_key(key_fields)
        if not row_to_update:
            return 0 

        row_index = self._rows.index(row_to_update)
        curr_row_copy = self._rows[row_index].copy()
        for field, value in new_values.items():
            if field in curr_row_copy:
                curr_row_copy[field] = value
        if self.update_creates_duplicate_primary_key(new_values, curr_row_copy):
            raise DuplicatePrimaryKeyException
        else:
            self._rows[row_index] = curr_row_copy
        return 1

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        rows_to_update = self.find_by_template(template)

        if type(new_values) != dict:
            raise DataTableException('new_values must be a dictionary')

        num_updated = 0
        for row in rows_to_update:
            row_index = self._rows.index(row)
            curr_row_copy = self._rows[row_index].copy()
            for field, value in new_values.items():
                if field in curr_row_copy:
                    curr_row_copy[field] = value
            if self.update_creates_duplicate_primary_key(new_values, curr_row_copy):
                raise DuplicatePrimaryKeyException
            else:
                self._rows[row_index] = curr_row_copy
            num_updated += 1

        return num_updated

    def update_creates_duplicate_primary_key(self, new_values, row):
        key_columns = self._data['key_columns']
        if not key_columns:
            # No primary key in this table
            return False
        key_fields = list()

        updated_a_key = False 
        for key_column in key_columns:
            if key_column in new_values:
                updated_a_key = True
                break

        # Only check if the update involved changing a key
        if updated_a_key:
            for key_column in key_columns:
                key_fields.append(row[key_column])

            if self.find_by_primary_key(key_fields):
                return True

        return False

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        # Checks to make sure the new record has all the necessary fields, and no more
        if not self.matches_columns(new_record):
            raise DataTableException('new_record must contain the exact fields of the table.')

        if self.insert_creates_duplicate_primary_key(new_record):
            raise DuplicatePrimaryKeyException('This insert would lead to a duplicate primary key')

        self._rows.append(new_record)


    def insert_creates_duplicate_primary_key(self, row):
        key_columns = self._data['key_columns']
        if key_columns is None:
            return False

        key_fields = list()

        for key_column in key_columns:
            key_fields.append(row[key_column])

        if self.find_by_primary_key(key_fields):
            return True
        return False


    def matches_columns(self, row):
        correct_row = self._rows[0]
        for field in correct_row.keys():
            if field not in row:
                return False
        return len(row) == len(correct_row)  # Makes sure the row doesn't have extra columns


    def get_rows(self):
        return self._rows

class DataTableException(Exception):
    pass

class DuplicatePrimaryKeyException(Exception):
    pass