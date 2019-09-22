# W4111_F19_HW1

## Part 1: CSVDataTable
This part of the homework involved reading a local csv file and writing functions to perform basic selects, edits, and deletes on it. I used the People table for the testing for this part, but the methods I wrote work with any table. Below are the methods that I implemented and tested for the CSVDataTable file:
* find_by_primary_key
    * This method works by looping through all rows in the csv, searching for items that match the given key fields for the primary key. If no matching rows are found, 0 is returned. If exactly one matching row is found, that row (or, if a field list was given, only the specified fields from that row) are returned. If more than 1 matching row was found, we have an integrity error in our data, and an Exception is thrown.
* find_by_template
    * This method utilizes the matches_template method, checking for every row that matches the given template. It then returns a list of those rows. 
* delete_by_key
    * Delete by key uses the find_by_primary_key method to find the row to delete. If it is found, it removes the row from the csv and returns 0. Otherwise, nothing is removed and 0 is returned.
* delete_by_template
    * Utilizes the matches_template function to delete any row that returns true. Keeps a count of the returned rows and returns that count at the end
* update_by_key
    * Uses the find_by_primary_key method to find the correct row to update. It then makes a copy of that row, and makes the update changes. Then, before actually updating the csv, it checks if this updated row would result in a primary key conflict with any existing row in the csv. It checks by going through every key in key_columns for the updated row, and making sure that those exact key_fields do not already exist. If it results in collision, an error is thrown. Otherwise, it is updated accordingly.
* update_by_template
    * This method uses matches_template to find the correct row to update. Like update_by_key, it makes a copy of the row and checks the primary key constraints before actually updating the row in the csv file. This method also throws an error if the update would result in a collision. Otherwise, it is updated accordingly.
* insert
    * This method first checks to make sure the given record to insert upholds the relational database model of the existing table. This means that the given new_record must contain every field that the table has, no more and no less. After checking the fields, the method then checks to make sure that the given row's primary key does not already exist in the csv. If it passes both of those checks, the row in successfully inserted into the csv.

Note: The save function was not implemented, as the professor said that the method was not required in a Piazza post. However, the above methods are still thouroughly tested.

Testing Procedures:
All of the tests for this part are in the file: CSVDataTable_tests.py. In order to run the entire test suite, simply run the file itself. It will run all of the tests (23 of them!). For every test, a message of either "TEST PASSED" or "TEST FAILED" will be printed, along with the respective method.

## Part 2: RDBDataTable
This part of the homework involved connecting to a local database and writing methods to alter the elements in the database. I used the Batting table as the main table for my tests for this part, but the methods work with any table. Below are the methods I implemented and tested for Part 2:
* find_by_primary_key
    * This method involved taking the given key values and executing a select statement on the database and returning the resulting row. If exactly one row was found, it was returned. Otherwise, None was returned.
* find_by_template
    * Very similar in structure to find_by_primary_key, since finding by a key versus finding by a template is the exact same structure in SQL. It is simply a SELECT WHERE statement. Note this method can return an empty list if no matching rows are found to the template, and can also return None if an error occurs, like if the given template column identifiers are incorrect.
* delete_by_key
    * Does not use find_by_primary_key like Part 1 did. Similar structure to creating the statement, but uses the DELETE WHERE statement instead. Returns the number of deleted rows, or, on error, returns None
* delete_by_template
    * Uses the DELETE WHERE statement. Returns the number of deleted rows, or None on error.
* update_by_key
    * Uses the UPDATE SET WHERE statement. Note: if the update were to result in primary key collision, an error (IntegrityError) is raised. Otherwise, the total number of rows updated is returned
* update_by_template
    * Uses the UPDATE SET WHERE statemenet. As above, an IntegrityError is thrown if the update were to result in a collision. Otherwise, the total number of rows updated is returned.
* insert
    * Uses the INSERT INTO VALUES statement. This method can only accept one record at a time, cannot insert multiple with one call. This method check to make sure the inputted record has the correct columns to uphold the RDB model. If the columns match, the SQL statement is build and executed. If a collsion were to occur on this INSERT, an IntegrityError is raised.

Testing Procedures:
All of the tests for this part are in the file: RDBDataTable_tests.py. In order to run the entire test suite, simply run the file itself. It will run all of the tests (19 of them!). For every test, a message of either "TEST PASSED" or "TEST FAILED" will be printed, along with the respective method.



Note: dbutils.py, __init__, __str__, and get_row_count was all code I got from Piazza that the instructor posted. Also, I was using the Batting table for testing for this part. I used the following as the primary key for Batting: playerID, teamID, yearID, stint.

Overall Notes:
The primary key constraints are upheld throughout this homework. Errors are thrown if the data is already bad (several rows for one primary key). They are also thrown if updates or inserts are attempted that would result in primary key collision. This upholds the constraints of the table. Additionally, on insertion and updating, it is made sure that only the columns that already exist in the table, no more and no less, can be changed and added. This also upholds the relational database model. 
