from src.BaseDataTable import BaseDataTable
import src.dbutils as dbutils
import pymysql
import json
import pandas as pd
import random

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        cnx = dbutils.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = dbutils.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['"row_count'] = row_count

        return row_count

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_columns = self._data['key_columns']
        if len(key_columns) != len(key_fields):
            raise ValueError('key_fields must have the same number of keys as the key_columns')

        table = self._data['table_name']
        where_clause = list() 
        for i in range(len(key_fields)):
            where_clause.append("{} = '{}'".format(key_columns[i], key_fields[i]))
        where_clause = " AND ".join(where_clause)
        if field_list:
            fields = ", ".join(field_list)
            stmt = "SELECT {fields} FROM {table} WHERE {where_clause}".format(fields=fields, table=table, where_clause=where_clause)
        else:
            stmt = "SELECT * FROM {table} WHERE {where_clause}".format(table=table, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx)
        except:
            return None
        if num_rows == 1:
            return data[0]
        return None

            

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
            raise TypeError('template must be a dictionary')
        if field_list and type(field_list) != list:
            raise TypeError('field_list must be a list')
        table = self._data['table_name']
        where_clause = list()
        for field, value in template.items():
            where_clause.append("{} = '{}'".format(field, value))
        where_clause = " AND ".join(where_clause)
        if field_list:
            fields = ", ".join(field_list)
            stmt = "SELECT {fields} FROM {table} WHERE {where_clause}".format(fields=fields, table=table, where_clause=where_clause)
        else:
            stmt = "SELECT * FROM {table} WHERE {where_clause}".format(table=table, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx)
        except:
            return None
        return data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        table = self._data['table_name']
        where_clause = list()
        key_columns = self._data['key_columns']
        for i in range(len(key_fields)):
            where_clause.append("{} = '{}'".format(key_columns[i], key_fields[i]))
        where_clause = " AND ".join(where_clause)

        stmt = "DELETE FROM {table} WHERE {where_clause}".format(table=table, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx, commit=False)
        except:
            return None
        return num_rows 

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        table = self._data['table_name']
        where_clause = list()
        key_columns = self._data['key_columns']
        for field, value in template.items():
            where_clause.append("{} = '{}'".format(field, value))
        where_clause = " AND ".join(where_clause)

        stmt = "DELETE FROM {table} WHERE {where_clause}".format(table=table, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx, commit=False)
        except:
            return None
        return num_rows 

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if type(new_values) != dict:
            raise TypeError('new_values must be a dictionary')
        if type(key_fields) != list:
            raise TypeError('key_fields must be a list')

        table = self._data['table_name']
        set_clause = list()
        for field, value in new_values.items():
            set_clause.append("{} = '{}'".format(field, value))
        set_clause = ', '.join(set_clause)
        where_clause = list()
        key_columns = self._data['key_columns']
        for i in range(len(key_fields)):
            where_clause.append("{} = '{}'".format(key_columns[i], key_fields[i]))
        where_clause = " AND ".join(where_clause)

        stmt = "UPDATE {table} SET {set_clause} WHERE {where_clause}".format(table=table, set_clause=set_clause, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx, commit=False)
        except Exception as e:
            raise(e)
        return num_rows 

    def update_by_template(self, template, new_values):
        """
        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        if type(new_values) != dict or type(template) != dict:
            raise TypeError('template and new_values must be dictionaries')
        table = self._data['table_name']
        set_clause = list()
        for field, value in new_values.items():
            set_clause.append("{} = '{}'".format(field, value))
        set_clause = ', '.join(set_clause)
        where_clause = list()
        key_columns = self._data['key_columns']
        for field, value in template.items():
            where_clause.append("{} = '{}'".format(field, value))
        where_clause = " AND ".join(where_clause)

        stmt = "UPDATE {table} SET {set_clause} WHERE {where_clause}".format(table=table, set_clause=set_clause, where_clause=where_clause)
        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx, commit=False)
        except Exception as e:
            raise(e) 
        return num_rows 

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if not self.matches_columns(new_record):
            raise ValueError('new_record must have all the necessary columns for insertion')

        table = self._data['table_name']
        fields = list()
        values = list()
        for field, value in new_record.items():
            fields.append(field)
            values.append("'{}'".format(value))
        fields = ', '.join(fields)
        values = ', '.join(values)

        stmt = "INSERT INTO {table} ({field_list}) VALUES ({value_list})".format(table=table, field_list=fields, value_list=values)

        try:
            num_rows, data = dbutils.run_q(stmt, conn=self._cnx, commit=False)
        except Exception as e:
            raise(e) 

    def matches_columns(self, row):
        ex_row = self.get_row()
        for field in ex_row.keys():
            if field not in row:
                return False
        return len(row) == len(ex_row)

    def get_row(self):
        stmt = 'SELECT * FROM {} WHERE playerID = {}'.format(self._data['table_name'], "'abadfe01'")
        result, data = dbutils.run_q(stmt, conn=self._cnx)
        index = random.randint(0, result-1)
        return data[index] 




