import pandas as pd
import os
import glob

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
    logging.debug(df.columns)
    coloumn_of_interest = ["ID", "First_Name_01", "Last_Name_01", "City", "State", "ZIP", "Email", "Email_02" , "Email_03","Phone","CellPhone","Ind_Date_Of_Birth_Year"]

    headers = list(filter(lambda col: col in df, coloumn_of_interest))
    logging.debug(headers)
    return headers




def data_loader(file_name):
    ts_start = datetime.now()

    logging.debug(f'loader called on file {file_name}')
    
    try:

        files = glob.glob("./data/*.csv")

        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, 
            names=["ID", "First_Name_01", "Last_Name_01", "City", "State", "ZIP", "Email", "Email_02" , "Email_03","Phone","CellPhone","Ind_Date_Of_Birth_Year"],
            dtype={'HH_ID': int, 'ID': int, 'First_Name_01': object, 'alphafirstname_sort': object, 'Phonetic_First_Name': object, 'Last_Name_01': object, 'alphalastname_sort': object, 'Phonetic_Last_Name': object, 'Address': object, 'alphaaddress_sort': object, 'City': object, 'CITY_PHRASE': object, 'alphacity_sort': object, 'Cities': object, 'State': object, 'alphastate_sort': object, 'ZIP': object, 'ZIP4': object, 'Carrier_Route': object, 'Delivery_Point': object, 'Mail_Score_Code': object, 'Geo_Level_Code': object, 'Latitude': object, 'Longitude': object, 'Time_Zone_Code': object, 'County_Code': object, 'County_Description': object, 'CBSA_Code': object, 'CBSA_Description': object, 'Scrubbed_Phoneable_Flag': object, 'Ind_Gender_Code': object, 'Ind_Household_Rank_Code': object, 'Ind_Ethnic_Code': object, 'Ind_Political_Party_Code': object, 'Home_Value_Code': object, 'Home_Value_Description': object, 'Home_Median_Value_Code': object, 'Home_Median_Value_Description': object, 'Median_Income_Code': object, 'Median_Income_Description': object, 'Income_Code': object, 'Income_Description': object, 'Donor_Capacity_Code': object, 'Delivery_Point_CheckDigit': object, 'Address_Number': object, 'Street_Name': object, 'Street_Suffix': object, 'State_City': object, 'Address_ID': object, 'PO_Flag': object, 'Mailable_Flag': object, 'Location_Unique_Flag': object, 'ProductionDate': object, 'Lat_Long': object, 'Geo_Lat_Long': object, 'Marketing': object, 'Mailable': object, 'Phoneable': object, 'Mailable_Phoneable': object, 'ZIP9': object, 'Zip11': object, 'Zip4Exists': object, 'Address_Master': object, 'LS_Green_Living_Flag': object, '_version_': object, 'Lat_Long_0_coordinate': object, 'Lat_Long_1_coordinate': object, 'Pre_Direction': object, 'Home_Dwelling_Type_Code': object, 'Secondary_Name': object, 'Secondary_Number': object, 'Post_Direction': object, 'Number_of_Bedrooms': object, 'Number_of_Bathrooms': object, 'Home_Property_Type_Code_02': object, 'Home_Square_Footage': object, 'Home_Square_Footage_Code': object, 'Email_Present_Flag': object, 'Email': object, 'Email_02': object, 'Email_01_MD5': object, 'Email_02_MD5': object}
            # converters={'HH_ID': convert_dtype, 'ID': int, 'First_Name_01': convert_dtype, 'alphafirstname_sort': convert_dtype, 'Phonetic_First_Name': convert_dtype, 'Last_Name_01': convert_dtype, 'alphalastname_sort': convert_dtype, 'Phonetic_Last_Name': convert_dtype, 'Address': convert_dtype, 'alphaaddress_sort': convert_dtype, 'City': convert_dtype, 'CITY_PHRASE': convert_dtype, 'alphacity_sort': convert_dtype, 'Cities': convert_dtype, 'State': convert_dtype, 'alphastate_sort': convert_dtype, 'ZIP': convert_dtype, 'ZIP4': convert_dtype, 'Carrier_Route': convert_dtype, 'Delivery_Point': convert_dtype, 'Mail_Score_Code': convert_dtype, 'Geo_Level_Code': convert_dtype, 'Latitude': convert_dtype, 'Longitude': convert_dtype, 'Time_Zone_Code': convert_dtype, 'County_Code': convert_dtype, 'County_Description': convert_dtype, 'CBSA_Code': convert_dtype, 'CBSA_Description': convert_dtype, 'Scrubbed_Phoneable_Flag': convert_dtype, 'Ind_Gender_Code': convert_dtype, 'Ind_Household_Rank_Code': convert_dtype, 'Ind_Ethnic_Code': convert_dtype, 'Ind_Political_Party_Code': convert_dtype, 'Home_Value_Code': convert_dtype, 'Home_Value_Description': convert_dtype, 'Home_Median_Value_Code': convert_dtype, 'Home_Median_Value_Description': convert_dtype, 'Median_Income_Code': convert_dtype, 'Median_Income_Description': convert_dtype, 'Income_Code': convert_dtype, 'Income_Description': convert_dtype, 'Donor_Capacity_Code': convert_dtype, 'Delivery_Point_CheckDigit': convert_dtype, 'Address_Number': convert_dtype, 'Street_Name': convert_dtype, 'Street_Suffix': convert_dtype, 'State_City': convert_dtype, 'Address_ID': convert_dtype, 'PO_Flag': convert_dtype, 'Mailable_Flag': convert_dtype, 'Location_Unique_Flag': convert_dtype, 'ProductionDate': convert_dtype, 'Lat_Long': convert_dtype, 'Geo_Lat_Long': convert_dtype, 'Marketing': convert_dtype, 'Mailable': convert_dtype, 'Phoneable': convert_dtype, 'Mailable_Phoneable': convert_dtype, 'ZIP9': convert_dtype, 'Zip11': convert_dtype, 'Zip4Exists': convert_dtype, 'Address_Master': convert_dtype, 'LS_Green_Living_Flag': convert_dtype, '_version_': convert_dtype, 'Lat_Long_0_coordinate': convert_dtype, 'Lat_Long_1_coordinate': convert_dtype, 'Pre_Direction': convert_dtype, 'Home_Dwelling_Type_Code': convert_dtype, 'Secondary_Name': convert_dtype, 'Secondary_Number': convert_dtype, 'Post_Direction': convert_dtype, 'Number_of_Bedrooms': convert_dtype, 'Number_of_Bathrooms': convert_dtype, 'Home_Property_Type_Code_02': convert_dtype, 'Home_Square_Footage': convert_dtype, 'Home_Square_Footage_Code': convert_dtype, 'Email_Present_Flag': convert_dtype, 'Email': convert_dtype, 'Email_02': convert_dtype, 'Email_01_MD5': convert_dtype, 'Email_02_MD5': convert_dtype, 'Email_03': convert_dtype,'Phone': convert_dtype,'CellPhone': convert_dtype,'Ind_Date_Of_Birth_Year': convert_dtype}
            )
            df = df.append(csv)

        print(df)

        df.to_csv("./data/tmp/all_in_one.csv" , index=False, header=True, na_rep="") 
        # # df = pd.read_csv(f'./data/{file_name}', 
        # df = pd.read_csv(file_name, 

        # header=0, 
        # # index_col='ID',
        # low_memory=False,
        # error_bad_lines = False
        # # converters={'HH_ID': convert_dtype, 'ID': convert_dtype, 'First_Name_01': convert_dtype, 'alphafirstname_sort': convert_dtype, 'Phonetic_First_Name': convert_dtype, 'Last_Name_01': convert_dtype, 'alphalastname_sort': convert_dtype, 'Phonetic_Last_Name': convert_dtype, 'Address': convert_dtype, 'alphaaddress_sort': convert_dtype, 'City': convert_dtype, 'CITY_PHRASE': convert_dtype, 'alphacity_sort': convert_dtype, 'Cities': convert_dtype, 'State': convert_dtype, 'alphastate_sort': convert_dtype, 'ZIP': convert_dtype, 'ZIP4': convert_dtype, 'Carrier_Route': convert_dtype, 'Delivery_Point': convert_dtype, 'Mail_Score_Code': convert_dtype, 'Geo_Level_Code': convert_dtype, 'Latitude': convert_dtype, 'Longitude': convert_dtype, 'Time_Zone_Code': convert_dtype, 'County_Code': convert_dtype, 'County_Description': convert_dtype, 'CBSA_Code': convert_dtype, 'CBSA_Description': convert_dtype, 'Scrubbed_Phoneable_Flag': convert_dtype, 'Ind_Gender_Code': convert_dtype, 'Ind_Household_Rank_Code': convert_dtype, 'Ind_Ethnic_Code': convert_dtype, 'Ind_Political_Party_Code': convert_dtype, 'Home_Value_Code': convert_dtype, 'Home_Value_Description': convert_dtype, 'Home_Median_Value_Code': convert_dtype, 'Home_Median_Value_Description': convert_dtype, 'Median_Income_Code': convert_dtype, 'Median_Income_Description': convert_dtype, 'Income_Code': convert_dtype, 'Income_Description': convert_dtype, 'Donor_Capacity_Code': convert_dtype, 'Delivery_Point_CheckDigit': convert_dtype, 'Address_Number': convert_dtype, 'Street_Name': convert_dtype, 'Street_Suffix': convert_dtype, 'State_City': convert_dtype, 'Address_ID': convert_dtype, 'PO_Flag': convert_dtype, 'Mailable_Flag': convert_dtype, 'Location_Unique_Flag': convert_dtype, 'ProductionDate': convert_dtype, 'Lat_Long': convert_dtype, 'Geo_Lat_Long': convert_dtype, 'Marketing': convert_dtype, 'Mailable': convert_dtype, 'Phoneable': convert_dtype, 'Mailable_Phoneable': convert_dtype, 'ZIP9': convert_dtype, 'Zip11': convert_dtype, 'Zip4Exists': convert_dtype, 'Address_Master': convert_dtype, 'LS_Green_Living_Flag': convert_dtype, '_version_': convert_dtype, 'Lat_Long_0_coordinate': convert_dtype, 'Lat_Long_1_coordinate': convert_dtype, 'Pre_Direction': convert_dtype, 'Home_Dwelling_Type_Code': convert_dtype, 'Secondary_Name': convert_dtype, 'Secondary_Number': convert_dtype, 'Post_Direction': convert_dtype, 'Number_of_Bedrooms': convert_dtype, 'Number_of_Bathrooms': convert_dtype, 'Home_Property_Type_Code_02': convert_dtype, 'Home_Square_Footage': convert_dtype, 'Home_Square_Footage_Code': convert_dtype, 'Email_Present_Flag': convert_dtype, 'Email': convert_dtype, 'Email_02': convert_dtype, 'Email_01_MD5': convert_dtype, 'Email_02_MD5': convert_dtype, 'Email_03': convert_dtype,'Phone': convert_dtype,'CellPhone': convert_dtype,'Ind_Date_Of_Birth_Year': convert_dtype}
        # )

        # # print(df.columns)
        # headers = get_headers(df)
        # df =  df[headers]
        # # print(df)

        database_load('all_in_one.csv')
        # cleanup(file_name)
        ts_end = datetime.now()

        logging.info(f'{file_name} Processing Took %s seconds {ts_end - ts_start}')   
    except Exception as e:
        logging.error(f"Error for file {file_name}")
        logging.error(e)
    


def cleanup(file_name):
    dir = os.path.dirname(file_name)
    file_base_name = os.path.basename(file_name)
    archive_name = dir + "/archive/" + file_base_name
    os.rename(file_name, archive_name ) 
    



def database_load(file_name):
    
    # #INPUT CONNECTION STRING HERE
    conn_string = "postgresql://pgadmin:amit4488@localhost/people"

    
    sql = f'''
    COPY copy_test_1 
    FROM '/var/lib/postgresql/tmp_data/{file_name}' 
    DELIMITER ',' CSV;
    '''

    table_create_sql = '''
    CREATE TABLE IF NOT EXISTS copy_test_1 ( ID       bigint,
                                           First_Name_01   object, 
                                           Last_Name_01 object, 
                                           City object, 
                                           State object, 
                                           ZIP int, 
                                           Email  object, 
                                           Email_02 object,
										   Email_03 object default 'None',
										   Phone float default 0.0, 
										  CellPhone float default 0, 
										  Ind_Date_Of_Birth_Year float default 0.0
                                  )
    '''

    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()
    cur.execute(table_create_sql)

    start_time = datetime.now()
    
    
    cur.execute(sql)
    pg_conn.commit()
    cur.close()
    logging.debug("COPY duration: {} seconds".format(datetime.now() - start_time))




if __name__ == '__main__':

    

    data_loader("./data")
    