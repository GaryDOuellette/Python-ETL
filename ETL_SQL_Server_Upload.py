# Import Python libraries
import pypyodbc as odbc
import pandas as pd
import os
import shutil
import datetime

"""
Create and initiate appendable log file
"""

# Create Log file
f = open('ETL_LogFile.txt', 'a')
log_datetime = datetime.datetime.now()
f.write('---------------------------------------\n')
f.write(f'Logfile: {log_datetime}\n')
f.write('\n')

# Folders
data_folder = '.\\UPLOAD'
arch_folder = '.\\ARCHIVE'

"""
Import Data files and concatenate all data into one DataFrame
""" 
workbooks = os.listdir(data_folder)

# Create list of .csv files
csv_files = [file for file in workbooks if file.endswith('.csv')]

# Create list to store dataframes
df_list = []

for csv in csv_files:
    csv_path = os.path.join(data_folder, csv)
    try:
        # read file using UTF-8 encoding
        df = pd.read_csv(csv_path)
        df_list.append(df)
    except UnicodeDecodeError:
        try:
            # Try UTF-16 encoding with tab separator if UTF-8 fails
            df = pd.read_csv(csv_path, sep='\t', encoding='utf-16')
            df_list.append(df)
        except Exception as e:
            print(f'Could not read file {csv} because of error: {e}')
    except Exception as e:
        f.write(f'Could not read file {csv} because of error: {e}\n')
        print(f'Could not read file {csv} because of error: {e}')
    
# Concatenate all data into on DataFrame
con_df = pd.concat(df_list, ignore_index=True)

"""
Data clean up - 
"""

# Specify columns to import
columns = ['FIRST_NAME', 'LAST_NAME', 'AGE']

# Create new dataframe with only the needed columns
df_data = con_df[columns]

records = df_data.values.tolist()

"""
SQL Server connection
"""
# Replace with actual driver name; if unsure search under driver tab using ODBC Data Source Administrator
DRIVER = 'SQL Server'
f.write(f'DRIVER: {DRIVER}\n')

# Replace with actual server name; run: SELECT @@ServerName
SERVER_NAME = 'LAPTOP-XXXXXXXX\SQLEXPRESS'
f.write(f'SERVER_NAME: {SERVER_NAME}\n')

# Replace with actual database name
DATABASE_NAME = 'SQL_TESTING'
f.write(f'DATABASE_NAME: {DATABASE_NAME}\n')

# Create connection string- Reference found at www.connectionstrings.com
def connection_string(driver, server_name, database_name):
    conn_string = f"""
    DRIVER={{{driver}}};
    SERVER={server_name};
    DATABASE={database_name};
    Trust_Connection=yes; 
"""
    return conn_string
    
"""
Create database connection instance
"""   

try:
    conn=odbc.connect(connection_string(DRIVER,SERVER_NAME,DATABASE_NAME)) 
except odbc.DatabaseError as e:
    print('Database Error:')
    print(str(e.value[1]))
    f.write(f'Database Error: {str(e.value[1])}\n')  
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))
    f.write(f'Connection Error: {str(e.value[1])}\n')   

"""
Create a cursor connection
""" 

# Insert statement
sql_insert = """
    INSERT INTO temp_Persons
    values (?,?,?)
    """
# Stored procedure statement
sql_sproc = """
    EXEC sp_persons_upload 
    """
    
try:
    cursor=conn.cursor()
    # Insert records
    cursor.executemany(sql_insert, records)
    cursor.commit();
    f.write('Database upload complete\n')
    print('Upload complete\n')
    cursor.execute(sql_sproc)
    cursor.commit();
    print('Stored Procedure Executed\n')
    
except Exception as e:
    cursor.rollback()
    f.write(f'Exception: {e}\n')
    print(str(e[1]))
    
finally:
    f.write('Stored Procedure Executed\n')
    print('Upload Complete and Stored Procedure Executed\n')
    cursor.close()
    conn.close()    

"""
Move files to ARCHIVE and delete from UPLOAD folder
"""

try:
    for workbook in workbooks:
        f.write(f'Archiving file: {workbook}\n')
        shutil.copy2(os.path.join(data_folder,workbook), arch_folder)
        os.remove(os.path.join(data_folder,workbook))
    
except:
    f.write(f'Files Not Successfully Copied to Archive\n')
    print('Files not copied or deleted')
    exit()
    
finally:
    f.write('All files successfully archived\n')
    f.write('\n')
    f.write('---------------------------------------\n')
    f.write('\n')   
    print('Files copied and deleted')
    f.close()

