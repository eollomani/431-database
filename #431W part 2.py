#431W part 2

import psycopg2
from psycopg2 import extras

#var that keeps track of errors
error = None

def connect_database():
    #Connects to the database.
    return psycopg2.connect(dbname="endri", user="endri", password="endri", host="localhost", port = "5432")

#function that gets the columns from every table
def get_columns(user_input_tables):
    #column_dict = {'master' : ['bioid', 'firstname', 'lastname', 'position'], ''}
    if user_input_tables == 'master':
        #master columns
        columns = ['bioid', 'firstname', 'lastname', 'position']
    elif user_input_tables == 'player':
        #player columns
        columns = ['playerid', 'year', 'gamesplayer', 'points', 'minutes']
    elif user_input_tables == 'team':
        #team columns
        columns = ['name', 'year', 'teamid', 'divisionid', 'conferenceid']
    elif user_input_tables == 'postseason':
        #postseason columns
        columns = ['year', 'round', 'teamidwinner', 'teamidloser']
    elif user_input_tables == 'draft':
        #draft columns
        columns = ['firstname', 'lastname', 'year', 'school', 'teamid', 'round', 'pick', 'overallselection']
    elif user_input_tables == 'coachaward':
        #coachaward columns
        columns = ['year', 'coachid', 'award', 'teamid']
    elif user_input_tables == 'playeraward':
        #playeraward columns
        columns = ['year', 'playerid', 'award']
    elif user_input_tables == 'halloffame':
        #playerawards
        columns = ['name', 'year', 'category']
    elif user_input_tables == 'allstargame':
        #allstargame columns
        columns = ['year', 'playerid', 'conferenceid', 'firstname', 'lastname']
    #returns the columns from the table
    return columns

def possible_tables(user_input_table):
    #this database has these tables: master, player, team, postseason, draft, coachaward, playeraward, halloffame, allstargame
    #list of tables in the database
    possible_tables = ['master', 'player', 'team', 'postseason', 'draft', 'coachaward', 'playeraward', 'halloffame', 'allstargame']
    #if what the user provided is a valid table
    if user_input_table in possible_tables:
        #get the columns for that table
        columns = get_columns(user_input_table)
        return columns
    else:
        return []

#works!!
def insert_data(conn):
    #table_name, columns, values_placeholder, values_str, values = user_inputs()
    table_name = input("Enter the table name: ")
    #gets the columns from the table input
    columns = possible_tables(table_name)
    #if the column arry is empty the table is empty with no attrubutes
    if(columns == []):
        error = 1
        return "Error there is no data in the table"
    else:
        values = []
        #prompts the user to fill the value with every attribute in the table
        for attribute in columns:
            value = input(f"Enter the value for {attribute}: ")
            values.append(value)
    #joins the values and columns together for the sql command. 
    list_of_values = ', '.join(['%s'] * len(values))
    list_of_columns = ', '.join(columns)
    #the sql command that is used in the database
    command = f"INSERT INTO {table_name} ({list_of_columns}) VALUES ({list_of_values});"
    conn.cursor().execute(command, values)
    conn.commit()

#function that prints out every column in a numbered order
def provide_column_options(columns):
    #for loop that does this
    for index, c in enumerate(columns, start=1):
        print(f'{index}. {c}')
    
#works!!
def delete_data(conn):
    #table
    table_name = input("Enter the table name: ")
    #columns
    columns_options = possible_tables(table_name)
    provide_column_options(columns_options)
    #conditions
    column = input("Choose a column from the options above for the condition: ")
    value = input(f"Enter the value for {column}: ")
    #the sql command that is used in the database
    command = f"DELETE FROM {table_name} WHERE {column} = {value};"
    conn.cursor().execute(command, value)
    conn.commit()
    
    
#works !!
def update_data(conn):
    #table
    table_name = input("Enter the table name: ")
    #provides a list of all of the columns
    columns_options = possible_tables(table_name)
    provide_column_options(columns_options)
    column = input("Choose the column from the options above you want to update: ")
    column_value = input(f"Enter the value you want to change {column} to: ")
    condition = input(f"Enter the condition column: ")
    #condition value for the condition column
    condition_value = input(f"Enter the value for {condition}: ")
    #sql command used in the database
    query = f"UPDATE {table_name} SET {column} = {column_value} WHERE {condition} = {condition_value};"
    values = (column_value, condition_value)
    conn.cursor().execute(query, values)
    conn.commit()
    
#works !!
def search_data(conn):
    #table
    table_name = input("Enter the table name: ")
    #provides a list of columns from the table input
    columns_options = possible_tables(table_name)
    provide_column_options(columns_options)
    column = input("Enter the column: ")
    #condition
    condition = input("Choose a column from the options above to be the conditional column: ")
    #value for the condition column
    value = input(f"Enter the value for {condition}: ") 

    #sql command that is used in the database
    command = f"SELECT {column} FROM {table_name} WHERE {condition} = {value};"
    connection = conn.cursor()
    connection.execute(command, (value))
    #results from the sql command
    results = connection.fetchall()
    try:
        if results is not None:
            #prints everything from the results of the sql command
            for r in results:
                print(r)
    #expection if error occurs
    except Exception as ex:
        print(f"An error occurred: {ex}")
    finally:
        connection.close()

#works !!
def aggregate_functions(conn):
    #table input
    table_name = input("Enter the table name: ")
    #provides a list of columns in the table
    columns_options = possible_tables(table_name)
    provide_column_options(columns_options)
    column = input("Choose a column from the options above to be the conditional column: ")
    #array that stores all of the valid functions
    possible_functions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']
    #prints out all of the functions
    provide_column_options(possible_functions)
    #user input for functions
    aggregate_function = input("Choose one of the aggregate functions above: ")
    
    #if the function is valid
    if aggregate_function in possible_functions:
        #sql command
        command = f"SELECT {aggregate_function}({column}) FROM {table_name};"
        try:
            connection = conn.cursor()
            connection.execute(command)
            #results from the sql command
            returns = connection.fetchone()
            if returns is not None:
                #prints the first value in results
                if returns[0] is not None:
                    #print
                    print(f"{aggregate_function}({column}) is {returns[0]}")
        except Exception as expect:
            error = 5
            #if an error occurs
            print(f"An error occurred: {expect}") 
        finally:
            connection.close() 
    #if the user input for the function is not valid
    else:
        print("Invalid aggregate function use one of these: SUM, AVG, COUNT, MIN, MAX")

#works!!
def sorting(conn):
    #user input table
    table_name = input("Enter the table name: ")
    #all of the columns in the table
    columns_options = possible_tables(table_name)
    provide_column_options(columns_options)
    column = input("Choose one of the columns from above to sort: ")
    #the possible sorting orders
    possible_orders = ['ASC', 'DESC']
    #prints all of the possible sorting orders
    provide_column_options(possible_orders)
    order = input("Choose one of the orders to sort: ")
    #sql command for the database
    command = f"SELECT * FROM {table_name} ORDER BY {column} {order};"
    
    #if the sorting order the user inputted is valid
    if order in possible_orders:
        try:
            connection = conn.cursor()
            connection.execute(command)
            #stores what is returned from the sql command
            returns = connection.fetchall()
            if returns is not None:
                #prints the return
                for r in returns:
                    print(r)
        except Exception as expect:
            error = 6
            #if operation fails
            print(f"An error occurred: {expect}") 
        finally:
            connection.close()
    #if the user used a sorting order that is not valid 
    else:
        print("Invalid order use one of these: ASC or DESC")
        
#works!!
def joins(conn):
    #first table used in the join command
    first_table_name = input("Enter the first table: ")
    #second thable used in the join command
    second_table_name = input("Enter the second table: ")
    #list of all possibble join functions
    possible_joins = ['INNER', 'LEFT', 'RIGHT', 'FULL']
    #prints out all of the join possibilities
    provide_column_options(possible_joins)
    #user input join
    join = input(f'Choose one of the joins above to join the {first_table_name} table with the {second_table_name} table: ')
    #all the columns in the first table
    first_table_columns = possible_tables(first_table_name)
    provide_column_options(first_table_columns)
    #user input for attr
    attr1 = input(f"Enter the attribute for the {first_table_name} table: ")
    #all columns in the second table
    second_table_columns = possible_tables(second_table_name)
    provide_column_options(second_table_columns)
    attr2 = input(f"Enter the attribute for the {second_table_name} table: ")
    
    #if user input join is valid
    if join in possible_joins:
        #adds the join part
        join_type = join + " JOIN"
        #sql command used in database
        command = f"SELECT * FROM {first_table_name} {join_type} {second_table_name} ON {first_table_name}.{attr1} = {second_table_name}.{attr2};"
        try:
            connection = conn.cursor()
            connection.execute(command) 
            #stores the return from the command
            returns = connection.fetchall()
            if returns is not None:
                #prints everything in the joined table
                for r in returns:
                    print(r)
        except Exception as expect:
            error = 7
            #if an error ocuurs
            print(f"An error occurred: {expect}") 
        finally:
            connection.close() 
    #not a valid user input join
    else:
        print("Invalid join operation use one of these: INNER, LEFT, RIGHT, FULL.")
    

def grouping(conn):
    #user input tbale
    table_name = input("Enter the table name: ")
    #list of columns in the table
    columns = possible_tables(table_name)
    provide_column_options(columns)
    #count
    count_column = input("Enter the column: ")
    #column that the data will be grouped by
    group_column = input("Enter the column to group by: ")
    
    #sql command for the database
    command = f"SELECT {group_column}, COUNT({count_column}) FROM {table_name} GROUP BY {group_column};"

    try:
        connection = conn.cursor() 
        connection.execute(command)  
        #stores the return from the command
        returns = connection.fetchall()
        if returns:
            #prints the grouped data
            for r in returns:
                print(r)
    except Exception as expect:
        error = 8
        #error occured
        print(f"An error occurred: {expect}")  
    finally:
        connection.close()
    
def subqueries(conn):
    #table name in orginal query
    table_name = input("Enter the table name: ")
    #columns in orginial query and list them
    columns = possible_tables(table_name)
    provide_column_options(columns)
    column = input("Enter the column: ")
    #condition for orginial query
    condition_column = input("Enter the condtion column: ")
    #value for the condition
    condition_value = input(f"Enter the condition value for {condition_column}: ")
    
    #table name for the subquery
    sub_table_name = input("Enter the table name for the subquery: ")
    #columns in the subquery and list them
    sub_columns = possible_tables(sub_table_name)
    provide_column_options(sub_columns)
    sub_column = input("Enter the column for the subquery: ")
    #condtion for subquery
    sub_condition_column = input("Enter the condition column for the subquery: ")
    #value for the condition for the subquery
    sub_condition_value = input(f"Enter the condition value for {sub_condition_column}: ")
    
    #sql command for the databse
    command = f"SELECT {column} FROM {table_name} WHERE {condition_column} IN (SELECT {sub_column} FROM {sub_table_name} WHERE {sub_condition_column} = {sub_condition_value});"
    
    try:
        connection = conn.cursor()
        connection.execute(command, (sub_condition_value,)) 
        #return from the command
        returns = connection.fetchall()  
        if returns is not None:
            #returned the subquery row by row
            for r in returns:
                print(r)
    except Exception as expect:
        error = 9
        #error occured
        print(f"An error occurred: {expect}") 
    finally:
        connection.close() 

#for transactions
def asking_for_queries(conn):
     #Mini CLI for transactions
     while True:
            print("""
Select an option:
Welcome to the Database CLI Interface!
By: Endri Ollomani

1. Insert Data
2. Delete Data
3. Update Data
4. Search Data
5. Aggregate Functions
6. Sorting
7. Joins
8. Grouping
9. Subqueries
10. Quit

Enter your choice (1-10):
""")
            #user input values for the transactions
            what_user_picked = input("> ")
            if what_user_picked == '10':
                #quit
                break
            elif what_user_picked == '1':
                #insert
                insert_data(conn)
            elif what_user_picked == '2':
                #delete
                delete_data(conn)
            elif what_user_picked == '3':
                #update
                update_data(conn)
            elif what_user_picked == '4':
                #search
                search_data(conn)
            elif what_user_picked == '5':
                #aggregate functions
                aggregate_functions(conn)
            elif what_user_picked == '6':
                #sort
                sorting(conn)
            elif what_user_picked == '7':
                #joins
                joins(conn)
            elif what_user_picked == '8':
                #grouping
                grouping(conn)
            elif what_user_picked == '9':
                #subqueries
                subqueries(conn)
            else:
                print("Pick a number 1-12")
    
        
            
def transactions(conn):
    #the number of queries for the entire transaction
    number_of_queries = int(input("How many queries do you have? "))
    #variable that will store the current number of queries done
    n = 0
    #so changes aren't made automatically and will only be made automatically if all of ther queries work
    conn.autocommit = False
    try:
        #while loop so that the number of queries needed to be done can be done
        while(n!= number_of_queries):
            #mini CLI 
            asking_for_queries(conn)
            conn.commit()
            #updated the current number of queries done
            n = n+1
    except:
        error = 10
        #error found 
        print("error")
        conn.rollback()
            
    finally:
        conn.autocommit = True
 
    
def error_handling(conn, error_code):
    if(error_code == 1):
        #insert
        print('Error found at INSERT_DATA')
    elif(error_code == 2):
        #delete
        print('Error found at DELETE_DATA')
    elif(error_code == 3):
        #update
        print('Error found at UPDATE_DATA')
    elif(error_code == 4):
        #search
        print('Error found at SEARCH_DATA')
    elif(error_code == 5):
        #aggregate function
        print('Error found at AGGREGATE_FUNCTIONS')
    elif(error_code == 6):
        #sorting
        print('Error found at SORTING')
    elif(error_code == 7):
        #joins
        print('Error found at JOINS')
    elif(error_code == 8):
        #grouping
        print('Error found at GROUPING')
    elif(error_code == 9):
        #subqueries
        print('Error found at SUBQUERIES')
    elif(error_code == 10):
        #transactions
        print('Error found at TRANSACTIONS')
    else:
        #go to go!
        print('No errors found')

def main():
    #connects to database
    conn = connect_database()
    try:
        #while loop so that the CLI keeps on until the user quits
        while True:
            print("""
Select an option:
Welcome to the Database CLI Interface!
By: Endri Ollomani

1. Insert Data
2. Delete Data
3. Update Data
4. Search Data
5. Aggregate Functions
6. Sorting
7. Joins
8. Grouping
9. Subqueries
10. Transactions
11. Error Handling
12. Quit

Enter your choice (1-12):
""")
            what_user_picked = input("> ")
            if what_user_picked == '12':
                #quit
                break
            elif what_user_picked == '1':
                #insert
                insert_data(conn)
            elif what_user_picked == '2':
                #delete
                delete_data(conn)
            elif what_user_picked == '3':
                #update
                update_data(conn)
            elif what_user_picked == '4':
                #search
                search_data(conn)
            elif what_user_picked == '5':
                #aggregate function
                aggregate_functions(conn)
            elif what_user_picked == '6':
                #sorting
                sorting(conn)
            elif what_user_picked == '7':
                #join
                joins(conn)
            elif what_user_picked == '8':
                #grouping
                grouping(conn)
            elif what_user_picked == '9':
                #subqueries
                subqueries(conn)
            elif what_user_picked == '10':
                #transactions
                transactions(conn)
            elif what_user_picked == '11':
                #error handling
                error_handling(conn, error)
            else:
                print("Please pick a number 1-12. If you are trying to quit press 12!")
    except Exception as expect:
        print(f"An error occurred: {expect}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()


