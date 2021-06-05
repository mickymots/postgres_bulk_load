import pandas as pd
import os

from datetime import datetime, timedelta
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import logging



def convert_dtype(x):
    if not x:
        return ''
    try:
        return str(x)   
    except:        
        return ''


def get_headers(df):
    print(df.columns)
    coloumn_of_interest = ["ID", "First_Name_01", "Last_Name_01", "City", "State", "ZIP", "Email", "Email_02" , "Email_03","Phone","CellPhone","Ind_Date_Of_Birth_Year"]

    headers = list(filter(lambda col: col in df, coloumn_of_interest))
    print(headers)
    return headers


def data_loader(dir_name, file_name):
    ts_start = datetime.now()

    print(f'loader called on file {file_name}')
    
    try:

        # df = pd.read_csv(f'./data/{file_name}', 
        df = pd.read_csv(f'{dir_name}/data/{file_name}', 

        header=0, 
        # index_col='ID',
        low_memory=False,
        error_bad_lines = False
        # converters={'HH_ID': convert_dtype, 'ID': convert_dtype, 'First_Name_01': convert_dtype, 'alphafirstname_sort': convert_dtype, 'Phonetic_First_Name': convert_dtype, 'Last_Name_01': convert_dtype, 'alphalastname_sort': convert_dtype, 'Phonetic_Last_Name': convert_dtype, 'Address': convert_dtype, 'alphaaddress_sort': convert_dtype, 'City': convert_dtype, 'CITY_PHRASE': convert_dtype, 'alphacity_sort': convert_dtype, 'Cities': convert_dtype, 'State': convert_dtype, 'alphastate_sort': convert_dtype, 'ZIP': convert_dtype, 'ZIP4': convert_dtype, 'Carrier_Route': convert_dtype, 'Delivery_Point': convert_dtype, 'Mail_Score_Code': convert_dtype, 'Geo_Level_Code': convert_dtype, 'Latitude': convert_dtype, 'Longitude': convert_dtype, 'Time_Zone_Code': convert_dtype, 'County_Code': convert_dtype, 'County_Description': convert_dtype, 'CBSA_Code': convert_dtype, 'CBSA_Description': convert_dtype, 'Scrubbed_Phoneable_Flag': convert_dtype, 'Ind_Gender_Code': convert_dtype, 'Ind_Household_Rank_Code': convert_dtype, 'Ind_Ethnic_Code': convert_dtype, 'Ind_Political_Party_Code': convert_dtype, 'Home_Value_Code': convert_dtype, 'Home_Value_Description': convert_dtype, 'Home_Median_Value_Code': convert_dtype, 'Home_Median_Value_Description': convert_dtype, 'Median_Income_Code': convert_dtype, 'Median_Income_Description': convert_dtype, 'Income_Code': convert_dtype, 'Income_Description': convert_dtype, 'Donor_Capacity_Code': convert_dtype, 'Delivery_Point_CheckDigit': convert_dtype, 'Address_Number': convert_dtype, 'Street_Name': convert_dtype, 'Street_Suffix': convert_dtype, 'State_City': convert_dtype, 'Address_ID': convert_dtype, 'PO_Flag': convert_dtype, 'Mailable_Flag': convert_dtype, 'Location_Unique_Flag': convert_dtype, 'ProductionDate': convert_dtype, 'Lat_Long': convert_dtype, 'Geo_Lat_Long': convert_dtype, 'Marketing': convert_dtype, 'Mailable': convert_dtype, 'Phoneable': convert_dtype, 'Mailable_Phoneable': convert_dtype, 'ZIP9': convert_dtype, 'Zip11': convert_dtype, 'Zip4Exists': convert_dtype, 'Address_Master': convert_dtype, 'LS_Green_Living_Flag': convert_dtype, '_version_': convert_dtype, 'Lat_Long_0_coordinate': convert_dtype, 'Lat_Long_1_coordinate': convert_dtype, 'Pre_Direction': convert_dtype, 'Home_Dwelling_Type_Code': convert_dtype, 'Secondary_Name': convert_dtype, 'Secondary_Number': convert_dtype, 'Post_Direction': convert_dtype, 'Number_of_Bedrooms': convert_dtype, 'Number_of_Bathrooms': convert_dtype, 'Home_Property_Type_Code_02': convert_dtype, 'Home_Square_Footage': convert_dtype, 'Home_Square_Footage_Code': convert_dtype, 'Email_Present_Flag': convert_dtype, 'Email': convert_dtype, 'Email_02': convert_dtype, 'Email_01_MD5': convert_dtype, 'Email_02_MD5': convert_dtype, 'Email_03': convert_dtype,'Phone': convert_dtype,'CellPhone': convert_dtype,'Ind_Date_Of_Birth_Year': convert_dtype}
        )

        # print(df.columns)
        headers = get_headers(df)
        df =  df[headers]
        # print(df)

        database_load(df, file_name, headers)
        
        ts_end = datetime.now()

        logging.info(f'{file_name} Processing Took %s seconds {ts_end - ts_start}')   
    except Exception as e:
        print(e)
    finally:
        cleanup(dir_name, file_name)


import tarfile
def cleanup(dir_name, file_name):
    os.rename(f'{dir_name}/data/{file_name}', f'{dir_name}/processed/{file_name}') 
    



def database_load(df, file_name, headers):

    # Write DF to disk 
    df.to_csv(f'/home/amit/processed/{file_name}' , index=False, header=False, na_rep="") 


    # #INPUT CONNECTION STRING HERE
    conn_string = "postgresql://pgadmin:pgadmin1234@localhost/people"

    col_string = ",".join(headers)

    sql = f'''
    COPY copy_test_1 ({col_string})
    FROM '/home/amit/processed/{file_name}' 
    DELIMITER ',' CSV;
    '''

    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()
    

    start_time = datetime.now()
    
    
    cur.execute(sql)
    pg_conn.commit()
    cur.close()
    print("COPY duration: {} seconds".format(datetime.now() - start_time))




if __name__ == '__main__':

    DATA_FILE = 'ac_0.csv'

    data_loader("./data",DATA_FILE)
    