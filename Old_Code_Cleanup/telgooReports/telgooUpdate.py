from sqlalchemy import create_engine, MetaData, Table, insert, select, update, bindparam
import csv
import os
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import load_workbook
import time
from dateutil.parser import parse
import re


priority_fields = ['ENROLLMENT_ID', 'DISCONNECT_REASON', 'DEVICE_ID', 'ESN', 'MDN', 'MAKE_MODEL', 'PRODUCT_TYPE', 'NLAD_SUBSCRIBER_ID', 'ACCOUNT_STATUS', 'PLAN', 'AGENT_NAME']

# Before making any updates that arent made from a file generated today and therefore 'yesterday' needs to be changed to an earlier date, make sure to change 'yesterday' variable in which_rows_need_updating_or_inserting!!!!!!!!!


def large_csv_to_csv(report_path):
    with open(report_path) as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        medicaid_index = fields.index('MEDICAID ID')
        del fields[medicaid_index]
        new_name = path_to_csv_reports + '/' + 'updated_csv.csv'
        with open(new_name, 'w', encoding='utf-8') as writercsv:
            csvwriter = csv.writer(writercsv)
            csvwriter.writerow(fields)
            for row_num, row in enumerate(csvreader):
                if row_num % 100000 == 0:
                    print(row_num)
                del row[medicaid_index]
                csvwriter.writerow(row)


def checking_column_data(current_file):

    columns = customerInfoTelgoo.c.keys()
    with open(current_file, encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        no_spaces = [field.replace(' ', '_') for field in fields]
        no_spaces[no_spaces.index('CONSENT_FORM_AVAILABE(Y/N)')] = 'CONSENT_FORM_AVAILABE'
        no_spaces[no_spaces.index('ID_PROOF_AVAILABE(Y/N)')] = 'ID_PROOF_AVAILABE'
        # Remove hashtag field
        no_spaces.remove(no_spaces[0])
    for column in columns:
        if column not in no_spaces:
            print(columns)
            print(no_spaces)
            return False
    for column in no_spaces:
        if column not in columns:
            print(columns)
            print(no_spaces)
            return False
    return True

def csv_to_sql_sqlalchemy(path_to_csv):
    list_of_enroll_id_types = ['AMAX', 'WMAX', 'EMAX1', 'EMAX2', 'EMAX3', 'EMAX4', 'EMAX5', 'EMAX6', 'EMAX7', 'EMAX8', 'EMAX9']

    telgoo_contacts = []
    
    with engine.connect() as conn:
        for type_of_enroll in list_of_enroll_id_types:
            print(type_of_enroll)
            telgoo_contacts_partial = conn.execute(select(customerInfoTelgoo.c['ENROLLMENT_ID', 'DISCONNECT_REASON', 'DEVICE_ID', 'ESN', 'MDN', 'MAKE_MODEL', 'PRODUCT_TYPE', 'NLAD_SUBSCRIBER_ID', 'ACCOUNT_STATUS', 'PLAN', 'AGENT_NAME'])
                                        .where(customerInfoTelgoo.c['ENROLLMENT_ID'].like(f'{type_of_enroll}%'))).all()
            telgoo_contacts += telgoo_contacts_partial


    print(len(telgoo_contacts))
    dict_telgoo_contacts = {}
    enroll_index = priority_fields.index('ENROLLMENT_ID')
    for contact_num, contact in enumerate(telgoo_contacts):
        if contact_num % 100000 == 0:
            print(contact_num)
        dict_telgoo_contacts[contact[enroll_index]] = contact
    
    with open(path_to_csv, encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        fields = [field.replace(' ', '_') for field in fields]
        fields_indexes = [fields.index(x) for x in priority_fields]
        with open('comparison.csv', 'w', newline='', encoding='utf-8') as co_csv:
            print('compare file opened to write')
            csvwriter = csv.writer(co_csv)
            for row in csvreader:
                if len(row) == len(fields):
                    # This try block had to be put in to account for one row that comes in faulty, only 57 columns because of an unread comma
                    try:
                        csvwriter.writerow(row[x] for x in fields_indexes)
                    except:
                        print(row)
                else:
                    print(f'This row does not have the same amount of columns and is being removed:\n{row}')

                    
    print('starting function "which_rows_need_updating_or_inserting"')
    contacts_for_inserting, contacts_for_updating = which_rows_need_updating_or_inserting(dict_telgoo_contacts)
        
    # creating a csv reader object
    with open(path_to_csv, encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        # extracting field names through first row
        fields = next(csvreader)
        fields = [field.replace(' ', '_') for field in fields]
        fields[fields.index('CONSENT_FORM_AVAILABE(Y/N)')] = 'CONSENT_FORM_AVAILABE'
        fields[fields.index('ID_PROOF_AVAILABE(Y/N)')] = 'ID_PROOF_AVAILABE'

        # Remove the hashtag field
        del fields[0]
        print(f'This is the csv fields variable after removing the hashtag: {fields}')
        fields_dict = {}
        for field_num, field in enumerate(fields):
            fields_dict[field] = field_num
        # del fields_dict[list(fields_dict.keys())[0]]
        enroll_id_index = fields_dict['ENROLLMENT_ID']
        whole_rows_for_insert = []
        whole_rows_for_update = []

        print('Going through contact order ids that need inserting or updating')
        for row_num, row in enumerate(csvreader):
            # Remove the hastag field
            del row[0]
            if row_num % 100000 == 0:
                print(f'Row number for inserting and updating: {row_num}')
            if row[enroll_id_index] in contacts_for_inserting:
                if len(row) == len(fields_dict.keys()):
                    dict_test = {}
                    for col, index in fields_dict.items():
                        try:
                            if col == 'CREATED_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'ORDER_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'ACTIVATION_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DOB':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'BQP_BIRTH_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DEACTIVATION_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DEVICE_REIMBURSEMENT__DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'SSN':
                                if row[index]:
                                    temp_data = row[index]
                                    if len(temp_data) > 2:
                                        if temp_data[-2] == '.':
                                            temp_data = temp_data[:-2]
                                    digits = len(temp_data)
                                    if digits == 6:
                                        dict_test[col] = temp_data[1:-1]
                                    elif digits == 4:
                                        dict_test[col] = temp_data
                                    elif digits == 3:
                                        dict_test[col] = f'0{temp_data}'
                                    elif digits == 2:
                                        dict_test[col] = f'00{temp_data}'
                                    elif digits == 1:
                                        dict_test[col] = f'000{temp_data}'
                                    else:
                                        dict_test[col] = temp_data
                                else:
                                    dict_test[col] = ''
                            else:
                                dict_test[col] = row[index]
                        except:
                            dict_test[col] = ''
                    whole_rows_for_insert.append(dict_test)
                    contacts_for_inserting.remove(row[enroll_id_index])
                else:
                    print(f'Row length did not equal field length: {row}')
            elif row[enroll_id_index] in contacts_for_updating:
                if len(row) == len(fields):
                    dict_test = {}
                    for col, index in fields_dict.items():
                        try:
                            if col == 'ENROLLMENT_ID':
                                dict_test['b_ENROLLMENT_ID'] = row[index]
                            elif col == 'CREATED_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'ORDER_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'ACTIVATION_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DOB':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'BQP_BIRTH_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DEACTIVATION_DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'DEVICE_REIMBURSEMENT__DATE':
                                if row[index]:
                                    dict_test[col] = parse(row[index]).timestamp()
                                else:
                                    dict_test[col] = row[index]
                            elif col == 'SSN':
                                if row[index]:
                                    temp_data = row[index]
                                    if len(temp_data) > 2:
                                        if temp_data[-2] == '.':
                                            temp_data = temp_data[:-2]
                                    digits = len(temp_data)
                                    if digits == 6:
                                        dict_test[col] = temp_data[1:-1]
                                    elif digits == 4:
                                        dict_test[col] = temp_data
                                    elif digits == 3:
                                        dict_test[col] = f'0{temp_data}'
                                    elif digits == 2:
                                        dict_test[col] = f'00{temp_data}'
                                    elif digits == 1:
                                        dict_test[col] = f'000{temp_data}'
                                    else:
                                        dict_test[col] = temp_data
                                else:
                                    dict_test[col] = ''
                            else:
                                dict_test[col] = row[index]
                        except:
                            print(row)
                    whole_rows_for_update.append(dict_test)
                    contacts_for_updating.remove(row[enroll_id_index])
                else:
                    print(f'Row length did not equal field length: {row}')
            length_of_rows_insert = len(whole_rows_for_insert)
            length_of_rows_update = len(whole_rows_for_update)
            if length_of_rows_insert % 500 == 0 and length_of_rows_insert > 0:
                db_connect_and_insert(whole_rows_for_insert)
                whole_rows_for_insert = []
                print('500 rows submitted to db for inserting')
            if length_of_rows_update % 500 == 0 and length_of_rows_update > 0:
                db_connect_and_update(whole_rows_for_update)
                whole_rows_for_update = []
                print('500 rows submitted to db for updating')
        if len(whole_rows_for_insert) > 0:
            db_connect_and_insert(whole_rows_for_insert)
            whole_rows_for_insert = []
            print('Remainder of contacts for inserting have been submitted to db')
        print('DB inserting complete')
        if len(whole_rows_for_update) > 0:
            db_connect_and_update(whole_rows_for_update)
            whole_rows_for_update = []
            print('Remainder of contacts for updating have been submitted to db')
        print('DB updating complete')
    
def which_rows_need_updating_or_inserting(dict_telgoo_contacts: dict):
    need_update_by_enrollment_id = []
    need_insert_by_enrollment_id = []
    old_telgoo_list_of_enrollment_Ids = dict_telgoo_contacts.keys()

    #################################
    # Change 'yesterday' for running old reports 
    #################################
    yesterday = time.mktime((datetime.today() - timedelta(days=4)).date().timetuple())
    with open('comparison.csv', encoding='utf-8') as csvfile:
        print('compare file opened to read')
        csvreader = csv.reader(csvfile)
        fields_indexes = {v:k for k, v in enumerate(priority_fields)}
        enrollment_index = priority_fields.index('ENROLLMENT_ID')
        nlad_sub_id_index = priority_fields.index('NLAD_SUBSCRIBER_ID')
        for row_num, row in enumerate(csvreader):
            if row[enrollment_index] in old_telgoo_list_of_enrollment_Ids:
                old_contact = dict_telgoo_contacts[row[enrollment_index]]
                if row_num % 100000 == 0:
                    print(f'Row number: {row_num}')
                if row == list(old_contact):
                    continue
                else:
                    for field_num, field in enumerate(row):
                        if field != old_contact[field_num]:
                            changes.get(priority_fields[field_num]).append({
                                'ENROLLMENT_ID':row[enrollment_index],
                                'Field_Name':priority_fields[field_num],
                                'Old_Value':old_contact[field_num],
                                'New_Value':field,
                                'Change_Date':yesterday,
                                'Nlad_Subscriber_Id':row[nlad_sub_id_index]
                            })
                    need_update_by_enrollment_id.append(row[enrollment_index])
            else:
                #priority_fields = ['ENROLLMENT_ID', 'DISCONNECT_REASON', 'DEVICE_ID', 'ESN', 'MDN', 'MAKE_MODEL', 'PRODUCT_TYPE', 'NLAD_SUBSCRIBER_ID', 'ACCOUNT_STATUS', 'PLAN', 'AGENT_NAME']
                changes['NEW_CONTACTS'].append({
                    'ENROLLMENT_ID':row[enrollment_index],
                    'Nlad_Subscriber_Id':row[nlad_sub_id_index],
                    'Field_Name':'NEW_CONTACTS',
                    'Change_Date':yesterday,
                    'MDN':row[fields_indexes['MDN']],
                    'DISCONNECT_REASON':row[fields_indexes['DISCONNECT_REASON']],
                    'ACCOUNT_STATUS': row[fields_indexes['ACCOUNT_STATUS']],
                    'PLAN': row[fields_indexes['PLAN']],
                    'AGENT_NAME':row[fields_indexes['AGENT_NAME']]
                })
                need_insert_by_enrollment_id.append(row[enrollment_index])
            
    os.remove('comparison.csv')
    print('Finished going through new contacts')
    
    print(f'{len(need_insert_by_enrollment_id)} contacts need to be inserted')
    new_contacts_file_length_dict[file] = len(need_insert_by_enrollment_id)
    print(f'{len(need_update_by_enrollment_id)} contacts need to be updated')

    return need_insert_by_enrollment_id, need_update_by_enrollment_id
            
def db_connect_and_insert(list_of_row_dicts):    

    with engine.connect() as conn:
        # Create an insert statement targeting our table
        stmt = insert(customerInfoTelgoo)
        
        print('SQL statement prepared')

        conn.execute(stmt, list_of_row_dicts)
        
        print('SQL statement executed')
        
        conn.commit()
        print('DB Comitted')
    return True          

def db_connect_and_update(list_of_row_dicts):
    with engine.connect() as conn:
        conn.execute(update(customerInfoTelgoo)
                    .where(customerInfoTelgoo.c['ENROLLMENT_ID'] == bindparam('b_ENROLLMENT_ID'))
                    .values(CREATED_DATE = bindparam('CREATED_DATE'),
                            ORDER_DATE = bindparam('ORDER_DATE'),
                            ACTIVATION_DATE = bindparam('ACTIVATION_DATE'),
                            CUSTOMER_ID = bindparam('CUSTOMER_ID'),
                            FIRST_NAME = bindparam('FIRST_NAME'),
                            LAST_NAME = bindparam('LAST_NAME'),
                            ADDRESS1 = bindparam('ADDRESS1'),
                            ADDRESS2 = bindparam('ADDRESS2'),
                            CITY = bindparam('CITY'),
                            STATE = bindparam('STATE'),
                            ZIP = bindparam('ZIP'),
                            SSN = bindparam('SSN'),
                            DOB = bindparam('DOB'),
                            IS_HOUSEHOLD = bindparam('IS_HOUSEHOLD'),
                            IS_TRIBAL = bindparam('IS_TRIBAL'),
                            IS_SHELTER = bindparam('IS_SHELTER'),
                            IS_STATE_DB = bindparam('IS_STATE_DB'),
                            ESN = bindparam('ESN'),
                            MDN = bindparam('MDN'),
                            EMAIL = bindparam('EMAIL'),
                            ALTERNATE_PHONE_NUMBER = bindparam('ALTERNATE_PHONE_NUMBER'),
                            DEVICE_ID = bindparam('DEVICE_ID'),
                            MAKE_MODEL = bindparam('MAKE_MODEL'),
                            NLAD_SUBSCRIBER_ID = bindparam('NLAD_SUBSCRIBER_ID'),
                            STATE_DB_MATCHED = bindparam('STATE_DB_MATCHED'),
                            RECORD_KEEPING = bindparam('RECORD_KEEPING'),
                            ORDER_STATUS = bindparam('ORDER_STATUS'),
                            PLAN = bindparam('PLAN'),
                            PRODUCT_TYPE = bindparam('PRODUCT_TYPE'),
                            TABLET_SUBSIDY_QUALIFICATION = bindparam('TABLET_SUBSIDY_QUALIFICATION'),
                            CARRIER = bindparam('CARRIER'),
                            QUALIFYING_PROGRAM = bindparam('QUALIFYING_PROGRAM'),
                            SOURCE = bindparam('SOURCE'),
                            AUTHORIZE_CODE = bindparam('AUTHORIZE_CODE'),
                            MASTER_AGENT_NAME = bindparam('MASTER_AGENT_NAME'),
                            DISTRIBUTOR_AGENT_NAME = bindparam('DISTRIBUTOR_AGENT_NAME'),
                            RETAILER_AGENT_NAME = bindparam('RETAILER_AGENT_NAME'),
                            AGENT_NAME = bindparam('AGENT_NAME'),
                            DL_NUMBER = bindparam('DL_NUMBER'),
                            WINBACK = bindparam('WINBACK'),
                            ACCOUNT_STATUS = bindparam('ACCOUNT_STATUS'),
                            PAYMENT_TYPE = bindparam('PAYMENT_TYPE'),
                            SPONSOR_ID = bindparam('SPONSOR_ID'),
                            CURRENT_APS = bindparam('CURRENT_APS'),
                            BQP_FIRST_NAME = bindparam('BQP_FIRST_NAME'),
                            BQP_LAST_NAME = bindparam('BQP_LAST_NAME'),
                            BQP_BIRTH_DATE = bindparam('BQP_BIRTH_DATE'),
                            PORTIN_STATUS = bindparam('PORTIN_STATUS'),
                            AGENT_LOGIN_ID = bindparam('AGENT_LOGIN_ID'),
                            DISCONNECT_REASON = bindparam('DISCONNECT_REASON'),
                            DEACTIVATION_DATE = bindparam('DEACTIVATION_DATE'),
                            NLAD_ENROLLMENT_TYPE = bindparam('NLAD_ENROLLMENT_TYPE'),
                            ENROLLMENT_TYPE = bindparam('ENROLLMENT_TYPE'),
                            CONSENT_FORM_AVAILABE = bindparam('CONSENT_FORM_AVAILABE'),
                            ID_PROOF_AVAILABE = bindparam('ID_PROOF_AVAILABE'),
                            DEVICE_REIMBURSEMENT__DATE = bindparam('DEVICE_REIMBURSEMENT__DATE'),
                            DEVICE_EXPECTED_RATE = bindparam('DEVICE_EXPECTED_RATE'),
                            REVIEWED_BY = bindparam('REVIEWED_BY'),
                            SAME_MONTH_DISCONNECTION = bindparam('SAME_MONTH_DISCONNECTION'),
                            ACTIVE_PERIOD = bindparam('ACTIVE_PERIOD'))
                     , list_of_row_dicts)

        
        print('SQL statement executed')
        
        conn.commit()
        print('DB Comitted')

    return True

def update_telgoo_changes():
    
    for k, v in changes.items():
        fields_for_db_update = ['DISCONNECT_REASON', 'DEVICE_ID', 'ESN', 'MDN', 'MAKE_MODEL', 'PRODUCT_TYPE', 'NEW_CONTACTS']
        if k in fields_for_db_update and len(v) > 0:
            with engine.connect() as conn:
                conn.execute(insert(telgooChanges), v)
                conn.commit()
            print(f'Field {k} updated')
    print('Update DB updated')



if __name__ == '__main__':
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'
    path_to_csv_reports = '/home/ubuntu/MaxsipReports/pythonFiles/telgooReports/Adding_New'
    new_contacts_file_length_dict = {}
    
    files = os.listdir('/home/ubuntu/MaxsipReports/pythonFiles/telgooReports/Adding_New')
    
    # Define a regular expression pattern to extract the numeric part
    pattern = re.compile(r'AO-(\d+)')

    # Sort the files based on the numeric part before the date range
    sorted_files = sorted(files, key=lambda file_name: int(pattern.search(file_name).group(1)))

    for file in sorted_files:
        print(file)
        complete_path = '/home/ubuntu/MaxsipReports/pythonFiles/telgooReports/Adding_New' + '/' + file
        # Establish a database connection
        engine = create_engine(path_to_db)
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        customerInfoTelgoo = Table("customerInfoTelgoo", metadata_obj, autoload_with=engine)
        telgooChanges = Table("Telgoo_Changes", metadata_obj, autoload_with=engine)
        deviceQty = Table('Device_Qty', metadata_obj, autoload_with=engine)
        customerDeviceDatabase = Table('Customer_Device_Database', metadata_obj, autoload_with=engine)
        
        columns = customerInfoTelgoo.c.keys()
        
        changes = {}
        
        changes['NEW_CONTACTS'] = []
        for field in columns:
            changes[field] = []


        large_csv_to_csv(complete_path)
        file_path = path_to_csv_reports + '/' + 'updated_csv.csv'
        if checking_column_data(file_path):
            print('columns match')
            csv_to_sql_sqlalchemy(file_path)
            update_telgoo_changes()
            os.remove(file_path)
            os.remove(complete_path)
        else:
            print(f'columns dont match in file {file}')
        
    print('Numbers of new contacts')
    for k, v in new_contacts_file_length_dict.items():
        print(f'{k}: {v}')
