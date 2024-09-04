import os
import csv
import time
from sqlalchemy import create_engine, MetaData, Table, select, update, bindparam, insert
from dateutil.parser import parse
from datetime import datetime, timedelta
import shutil
import paramiko
import re
import zipfile
from time import sleep
import json

def get_files_from_sftp():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    with open('/etc/config.json') as config_file:
        config = json.load(config_file)    
    ssh.connect(config.get('TELGOO_SFTP_HOSTNAME'), port=22, username=config.get('SFTP_USERNAME'), password=config.get('TELGOO_SFTP_PASSWORD'))

    sftp_client = ssh.open_sftp()
    remote_directory = '/Terracom_Activation_Report'
    local_directory = '/home/ubuntu/MaxsipReports/pythonFiles'
    
    files = sftp_client.listdir_attr(remote_directory)
    sorted_files = sorted(files, key=lambda x: x.st_mtime, reverse=True)
    for file in sorted_files:
        if re.search('activation.*zip$', file.filename):
            print(file)
            sftp_client.get(f'{remote_directory}/{file.filename}', f'{local_directory}/{file.filename}')
            with zipfile.ZipFile(file.filename, 'r') as zip_file:
                os.mkdir(f'{local_directory}/terracom_zip_file_holder')
                zip_file.extractall('terracom_zip_file_holder')
            os.remove(file.filename)
            break
    ssh.close()
    sftp_client.close()
    return '/home/ubuntu/MaxsipReports/pythonFiles/terracom_zip_file_holder'

def processing_activation_report(file_path:str, yesterday:float, current_user_id:int, report_type:str):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Uploaded_Report_Records = Table("Uploaded_Report_Records", metadata_obj, autoload_with=engine)
    Merged_Database_Changes = Table("Merged_Database_Changes", metadata_obj, autoload_with=engine)
    
    def column_mapping_terracom_to_merged(desired_map:str) -> dict:
        terracom_csv_to_merged = {
            'CREATED DATE':'Created_Date',
            'ORDER DATE':'Order_Date',
            'ACTIVATION DATE':'Activation_Date',
            'CUSTOMER ID':'Misc_Customer_ID',
            'ENROLLMENT ID':'Customer_Order_ID',
            'FIRST NAME':'First_Name',
            'LAST NAME':'Last_Name',
            'ADDRESS1':'Address1',
            'ADDRESS2':'Address2',
            'CITY':'City', #10
            'STATE':'State',
            'ZIP':'Zip',
            'SSN':'SSN',
            'DOB':'DOB',
            'IS HOUSEHOLD':'IS_HOUSEHOLD',
            'IS TRIBAL':'IS_TRIBAL',
            'IS SHELTER':'IS_SHELTER',
            'IS STATE DB':'IS_STATE_DB',
            'SIM':'ESN',
            'MDN':'MDN', #20
            'EMAIL':'Email',
            'ALTERNATE PHONE NUMBER':'Phone1',
            'DEVICE ID':'IMEI',
            'MAKE MODEL':'Device_Model_Number',
            'NLAD SUBSCRIBER ID':'NLAD_Subscriber_ID',
            'STATE DB MATCHED':'STATE_DB_MATCHED',
            'RECORD KEEPING':'RECORD_KEEPING',
            'ORDER STATUS':'ORDER_STATUS',
            'PLAN':'Plan',
            'PRODUCT TYPE':'Device_Type', #30
            'CARRIER':'Service_Carrier',
            'TABLET SUBSIDY QUALIFICATION':'TABLET_SUBSIDY_QUALIFICATION',
            'QUALIFYING PROGRAM':'Qualifying_Program',
            'SOURCE':'SOURCE',
            'AUTHORIZE CODE':'AUTHORIZE_CODE',
            'MASTER AGENT':'MASTER_AGENT_NAME',
            'DISTRIBUTOR AGENT':'DISTRIBUTOR_AGENT_NAME',
            'RETAILER AGENT':'RETAILER_AGENT_NAME',
            'AGENT':'Agent',
            'DL NUMBER':'DL_NUMBER', #40
            'WINBACK':'WINBACK',
            'ACCOUNT STATUS':'Account_Status',
            'PAYMENT TYPE':'PAYMENT_TYPE',
            'SPONSOR ID':'SPONSOR_ID',
            'CURRENT APS':'CURRENT_APS',
            'PORTIN STATUS':'PORTIN_STATUS',
            'AGENT LOGIN ID':'Agent_LoginID',
            'DISCONNECT REASON':'ACP_Status',
            'DEACTIVATION DATE':'Deactivation_Date',
            'MEDICAID ID':'MEDICAID_ID', #50
            'BQP FIRST NAME':'BQP_FIRST_NAME',
            'BQP LAST NAME':'BQP_LAST_NAME',
            'BQP BIRTH DATE':'BQP_BIRTH_DATE',
            'NLAD ENROLLMENT TYPE':'NLAD_ENROLLMENT_TYPE',
            'ENROLLMENT TYPE':'ENROLLMENT_TYPE',
            'CONSENT FORM AVAILABE(Y/N)':'CONSENT_FORM_AVAILABE',
            'ID PROOF AVAILABE(Y/N)':'ID_PROOF_AVAILABE',
            'DEVICE REIMBURSEMENT  DATE':'DEVICE_REIMBURSEMENT__DATE',
            'DEVICE EXPECTED RATE':'DEVICE_EXPECTED_RATE',
            'REVIEWED BY':'REVIEWED_BY', #60
            'SAME MONTH DISCONNECTION':'SAME_MONTH_DISCONNECTION',
            'ACTIVE PERIOD':'ACTIVE_PERIOD' #62
        }
        
        if desired_map == 'terracom_csv_to_merged':
            return terracom_csv_to_merged
        else:
            return 'Requested map doesnt exist, try "terracom_csv_to_merged"'

    def checking_column_data(current_file):
        terracom_list_of_column_names = ['#','CREATED DATE', 'ORDER DATE', 'ACTIVATION DATE', 'CUSTOMER ID', 'ENROLLMENT ID', 'FIRST NAME', 
                                    'LAST NAME', 'ADDRESS1', 'ADDRESS2', 'CITY', 'STATE', 'ZIP', 'SSN', 
                                    'DOB', 'IS HOUSEHOLD', 'IS TRIBAL', 'IS SHELTER', 'IS STATE DB', 'SIM', 'MDN', 
                                    'EMAIL', 'ALTERNATE PHONE NUMBER', 'DEVICE ID', 'MAKE MODEL', 'NLAD SUBSCRIBER ID', 'STATE DB MATCHED', 'RECORD KEEPING', 
                                    'ORDER STATUS', 'PLAN', 'PRODUCT TYPE', 'CARRIER', 'TABLET SUBSIDY QUALIFICATION', 'QUALIFYING PROGRAM', 'SOURCE', 
                                    'AUTHORIZE CODE', 'MASTER AGENT', 'DISTRIBUTOR AGENT', 'RETAILER AGENT', 'AGENT', 'DL NUMBER', 'WINBACK', 
                                    'ACCOUNT STATUS', 'PAYMENT TYPE', 'SPONSOR ID', 'CURRENT APS', 'PORTIN STATUS', 'AGENT LOGIN ID', 'DISCONNECT REASON', 
                                    'DEACTIVATION DATE', 'MEDICAID ID', 'BQP FIRST NAME', 'BQP LAST NAME', 'BQP BIRTH DATE', 'NLAD ENROLLMENT TYPE', 'ENROLLMENT TYPE', 
                                    'CONSENT FORM AVAILABE(Y/N)', 'ID PROOF AVAILABE(Y/N)', 'DEVICE REIMBURSEMENT  DATE', 'DEVICE EXPECTED RATE', 'REVIEWED BY', 
                                    'SAME MONTH DISCONNECTION', 'ACTIVE PERIOD']
        with open(current_file) as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
        if terracom_list_of_column_names == fields:
            return True
        else:
            return False

    def get_customer_order_ids_with_changes(changes:dict) -> set:
        customer_order_ids_with_changes = set()

        for field in changes.values():
            for order_id_key in field.keys():
                customer_order_ids_with_changes.add(order_id_key)

        return customer_order_ids_with_changes
    
    def get_customer_order_ids_to_insert(csv_file:str):
        customer_order_ids_to_insert = set()
        with engine.connect() as conn:
            db_order_ids = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID'])
                                            .where(Merged_Customer_Databases.c['Source_Database'] == 'Terracom')).all()
        db_order_ids_indexed = {k[0]:k[0] for k in db_order_ids}
        with open(csv_file) as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            order_id_index = fields.index('ENROLLMENT ID')
            for contact in csvreader:
                contact_order_id = contact[order_id_index]
                if not db_order_ids_indexed.get(contact_order_id, None):
                    customer_order_ids_to_insert.add(contact_order_id)
        return customer_order_ids_to_insert

    def add_new_contacts_to_changes(indexed_ids_for_insert:dict, path_to_csv:str, changes:dict) -> dict:
        changes['New Contacts'] = {}
        with open(path_to_csv) as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields_dict = {}
            for field_num, field in enumerate(fields):
                fields_dict[field] = field_num
            for contact in csvreader:
                customer_order_id = contact[fields_dict['ENROLLMENT ID']]
                if indexed_ids_for_insert.get(customer_order_id, None):
                    changes['New Contacts'][customer_order_id] = {
                            'Source_Database':'Terracom',
                            'Customer_Order_ID':customer_order_id,
                            'Nlad_Subscriber_ID':contact[fields_dict['NLAD SUBSCRIBER ID']],
                            'Field_Name':'New Contacts',
                            'New_Value':'',
                            'Old_Value':'',
                            'Change_Date':yesterday,
                            'Activation_Date':contact[fields_dict['ACTIVATION DATE']],
                            'CLEC':'',
                            'Plan':contact[fields_dict['PLAN']],
                            'Agent':contact[fields_dict['AGENT']],
                            'Account_Status':contact[fields_dict['ACCOUNT STATUS']],
                            'ACP_Status':contact[fields_dict['DISCONNECT REASON']],
                            'MDN':contact[fields_dict['MDN']]
                    }

        return changes

    def csv_to_db_upsert(path_to_csv):
        then = datetime.now()
        
        changes_pre_new_contacts = get_changes(then, file_path)

        customer_order_ids_to_insert = get_customer_order_ids_to_insert(file_path)
        customer_order_ids_with_changes = get_customer_order_ids_with_changes(changes_pre_new_contacts)
        
        print(f'Length of customer order ids to insert: {len(customer_order_ids_to_insert)}')
        print(f'Length of customer order ids to change: {len(customer_order_ids_with_changes)}')
        print(f'Time passed: {datetime.now()-then}. Finished going through columns for changes')

        indexed_ids_for_insert = {k:k for k in customer_order_ids_to_insert}
        indexed_ids_for_update = {k:k for k in customer_order_ids_with_changes}

        changes_with_new_contacts = add_new_contacts_to_changes(indexed_ids_for_insert, path_to_csv, changes_pre_new_contacts)

        whole_rows_for_update = []
        whole_rows_for_insert = []
        
        terracom_csv_to_merged_columns = column_mapping_terracom_to_merged('terracom_csv_to_merged')
        with open(path_to_csv, encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields_dict = {}
            for field_num, field in enumerate(fields):
                fields_dict[field] = field_num
            for contact in csvreader:
                if len(contact) != len(fields):
                    continue
                contact_order_id = contact[fields_dict['ENROLLMENT ID']]
                if indexed_ids_for_update.get(contact_order_id, None):
                    whole_row_columns_and_data = {}
                    for column, index in fields_dict.items():
                        if column in ['CREATED DATE', 'ORDER DATE', 'ACTIVATION DATE', 'DOB', 'DEACTIVATION DATE', 'BQP BIRTH DATE', 'DEVICE REIMBURSEMENT  DATE']:
                            if contact[index]:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = parse(contact[index]).timestamp()
                            else:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = ''
                        elif column == 'ENROLLMENT ID':
                            whole_row_columns_and_data['b_Customer_Order_ID'] = contact[index]
                        elif column == 'SSN':
                            if contact[index]:
                                temp_data = contact[index]
                                if len(temp_data) > 2:
                                    if temp_data[-2] == '.':
                                        temp_data = temp_data[:-2]
                                digits = len(temp_data)
                                if digits == 6:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data[1:-1]
                                elif digits == 4:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data
                                elif digits == 3:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'0{temp_data}'
                                elif digits == 2:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'00{temp_data}'
                                elif digits == 1:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'000{temp_data}'
                                else:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data
                            else:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = ''
                        elif column == '#':
                            continue
                        else:
                            if contact[index]:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = contact[index]
                            else:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = ''
                    whole_rows_for_update.append(whole_row_columns_and_data)
                    for field_name, contact_change_lists in changes_with_new_contacts.items():
                        if contact_change_lists.get(contact_order_id, None):
                            if contact[fields_dict['ACTIVATION DATE']]:
                                changes_with_new_contacts[field_name][contact_order_id]['Activation_Date'] = parse(contact[fields_dict['ACTIVATION DATE']]).timestamp()
                            else:
                                changes_with_new_contacts[field_name][contact_order_id]['Activation_Date'] = ''
                            changes_with_new_contacts[field_name][contact_order_id]['Nlad_Subscriber_ID'] = contact[fields_dict['NLAD SUBSCRIBER ID']]
                            changes_with_new_contacts[field_name][contact_order_id]['CLEC'] = ''
                            changes_with_new_contacts[field_name][contact_order_id]['Plan'] = contact[fields_dict['PLAN']]
                            changes_with_new_contacts[field_name][contact_order_id]['Agent'] = contact[fields_dict['AGENT']]
                            changes_with_new_contacts[field_name][contact_order_id]['Account_Status'] = contact[fields_dict['ACCOUNT STATUS']]
                            changes_with_new_contacts[field_name][contact_order_id]['ACP_Status'] = contact[fields_dict['DISCONNECT REASON']]
                            changes_with_new_contacts[field_name][contact_order_id]['MDN'] = contact[fields_dict['MDN']]
                elif indexed_ids_for_insert.get(contact_order_id, None):
                    whole_row_columns_and_data = {}
                    whole_row_columns_and_data['Source_Database'] = 'Terracom'
                    for column, index in fields_dict.items():
                        if column in ['CREATED DATE', 'ORDER DATE', 'ACTIVATION DATE', 'DOB', 'DEACTIVATION DATE', 'BQP BIRTH DATE', 'DEVICE REIMBURSEMENT  DATE']:
                            if contact[index]:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = parse(contact[index]).timestamp()
                            else:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = ''
                        elif column == '#':
                            continue
                        elif column == 'SSN':
                            if contact[index]:
                                temp_data = contact[index]
                                if len(temp_data) > 2:
                                    if temp_data[-2] == '.':
                                        temp_data = temp_data[:-2]
                                digits = len(temp_data)
                                if digits == 6:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data[1:-1]
                                elif digits == 4:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data
                                elif digits == 3:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'0{temp_data}'
                                elif digits == 2:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'00{temp_data}'
                                elif digits == 1:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = f'000{temp_data}'
                                else:
                                    whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = temp_data
                            else:
                                whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = ''
                        else:
                            whole_row_columns_and_data[terracom_csv_to_merged_columns[column]] = contact[index]
                    whole_rows_for_insert.append(whole_row_columns_and_data)

                length_of_rows_insert = len(whole_rows_for_insert)
                length_of_rows_update = len(whole_rows_for_update)
                if length_of_rows_insert % 500 == 0 and length_of_rows_insert > 0:
                    db_connect_and_insert(whole_rows_for_insert)
                    whole_rows_for_insert = []
                if length_of_rows_update % 500 == 0 and length_of_rows_update > 0:
                    db_connect_and_update(whole_rows_for_update)
                    whole_rows_for_update = []
            if len(whole_rows_for_insert) > 0:
                db_connect_and_insert(whole_rows_for_insert)
                whole_rows_for_insert = []
            print('DB inserting complete')
            if len(whole_rows_for_update) > 0:
                print(whole_rows_for_update[0])
                db_connect_and_update(whole_rows_for_update)
                whole_rows_for_update = []
            print('DB updating complete')
        return changes_with_new_contacts
    
    def get_merged_db_terracom_contacts_with_col_value(merged_col:str) -> dict:
        with engine.connect() as conn:
            stmt = select(Merged_Customer_Databases.c['Customer_Order_ID', merged_col])\
                                            .where(Merged_Customer_Databases.c['Source_Database'] == 'Terracom')
            merged_db_contacts= conn.execute(stmt).all()
            stmt_selected_cols = stmt.selected_columns.keys()
            order_id_index = stmt_selected_cols.index('Customer_Order_ID')
            merged_col_index = stmt_selected_cols.index(merged_col)

        merged_db_terracom_contacts_with_col_value = {}
        for contact in merged_db_contacts:
            merged_db_terracom_contacts_with_col_value[contact[order_id_index]] = contact[merged_col_index]
        
        return merged_db_terracom_contacts_with_col_value

    def get_csv_terracom_contacts(csv_file:str, terracom_col:str) -> dict:
        csv_terracom_contacts = {}
        with open(csv_file, encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            order_id_index = fields.index('ENROLLMENT ID')
            current_selected_terracom_col_index = fields.index(terracom_col)
            for csv_reader_contact in csvreader:
                if len(csv_reader_contact) == len(fields):
                    csv_terracom_contacts[csv_reader_contact[order_id_index]] = csv_reader_contact[current_selected_terracom_col_index]
        return csv_terracom_contacts
    
    def cleanup_changes_from_unwanted_dicts(changes:dict) -> dict:
        fields_to_delete = []
        for field, values in changes.items():
            if len(values) == 0:
                fields_to_delete.append(field)
        for field in fields_to_delete:
            del changes[field]
        if changes.get('Last_Used_Data', None):
            del changes['Last_Used_Data']
        if changes.get('Last_Used_Phone', None):
            del changes['Last_Used_Phone']
        return changes


    def declare_changes_dict() -> dict:
        changes = {}
        columns = Merged_Customer_Databases.c.keys()
        for field in columns:
            changes[field] = {}
        return changes

    def get_changes(then, csv_file) -> dict:
        changes = declare_changes_dict()

        terracom_to_merged_columns = column_mapping_terracom_to_merged('terracom_csv_to_merged')
        yesterday = time.mktime((datetime.today() - timedelta(days=1)).date().timetuple())

        for terracom_col, merged_col in terracom_to_merged_columns.items():
            print(f'Time passed: {datetime.now()-then}. Starting {merged_col}')
            
            csv_terracom_contacts = get_csv_terracom_contacts(csv_file, terracom_col)
            merged_db_terracom_contacts_with_col_value = get_merged_db_terracom_contacts_with_col_value(merged_col)

            for order_id, col_value in csv_terracom_contacts.items():
                if merged_db_terracom_contacts_with_col_value.get(order_id, None):
                    if merged_col in ['Created_Date','Order_Date','Activation_Date','Deactivation_Date','DOB','Last_Used_Data',
                                        'Last_Used_Phone','ShipmentDate','deviceReimbursementDate','FollowupPeriod_CreatedDate',
                                        'BQP_BIRTH_DATE','DEVICE_REIMBURSEMENT__DATE']:
                        if col_value:
                            col_value = parse(col_value).timestamp()
                    elif merged_col == 'SSN':
                        if col_value:
                            if len(col_value) > 2:
                                if col_value[-2] == '.':
                                    col_value = col_value[:-2]
                            digits = len(col_value)
                            if digits == 6:
                                col_value = col_value[1:-1]
                            elif digits == 3:
                                col_value = f'0{col_value}'
                            elif digits == 2:
                                col_value = f'00{col_value}'
                            elif digits == 1:
                                col_value = f'000{col_value}'
                            else:
                                col_value = col_value
                        else:
                            col_value = ''
                    if merged_db_terracom_contacts_with_col_value[order_id] != col_value:
                        changes[merged_col][order_id] = {
                            'Source_Database':'Terracom',
                            'Customer_Order_ID':order_id,
                            'Nlad_Subscriber_ID':'',
                            'Field_Name':merged_col,
                            'New_Value':col_value,
                            'Old_Value':merged_db_terracom_contacts_with_col_value[order_id],
                            'Change_Date':yesterday,
                            'Activation_Date':'',
                            'CLEC':'',
                            'Plan':'',
                            'Agent':'',
                            'Account_Status':'',
                            'ACP_Status':'',
                            'MDN':''
                        }

        print(f'Length of changes: {len(changes.keys())}')

        changes_post_cleanup = cleanup_changes_from_unwanted_dicts(changes)
        return changes_post_cleanup

    def db_connect_and_insert(list_of_row_dicts):
        with engine.connect() as conn:
            stmt = insert(Merged_Customer_Databases)
            conn.execute(stmt, list_of_row_dicts)
            conn.commit()
        return True          

    def db_connect_and_update(list_of_row_dicts):
        with engine.connect() as conn:
            conn.execute(update(Merged_Customer_Databases)
                        .where(Merged_Customer_Databases.c['Customer_Order_ID'] == bindparam('b_Customer_Order_ID'))
                        .values(Created_Date = bindparam('Created_Date'),
                                Order_Date = bindparam('Order_Date'),
                                Activation_Date = bindparam('Activation_Date'),
                                Misc_Customer_ID = bindparam('Misc_Customer_ID'),
                                First_Name = bindparam('First_Name'),
                                Last_Name = bindparam('Last_Name'),
                                Address1 = bindparam('Address1'),
                                Address2 = bindparam('Address2'),
                                City = bindparam('City'),
                                State = bindparam('State'),
                                Zip = bindparam('Zip'),
                                SSN = bindparam('SSN'),
                                DOB = bindparam('DOB'),
                                IS_HOUSEHOLD = bindparam('IS_HOUSEHOLD'),
                                IS_TRIBAL = bindparam('IS_TRIBAL'),
                                IS_SHELTER = bindparam('IS_SHELTER'),
                                IS_STATE_DB = bindparam('IS_STATE_DB'),
                                ESN = bindparam('ESN'),
                                MDN = bindparam('MDN'),
                                Email = bindparam('Email'),
                                Phone1 = bindparam('Phone1'),
                                IMEI = bindparam('IMEI'),
                                Device_Model_Number = bindparam('Device_Model_Number'),
                                NLAD_Subscriber_ID = bindparam('NLAD_Subscriber_ID'),
                                STATE_DB_MATCHED = bindparam('STATE_DB_MATCHED'),
                                RECORD_KEEPING = bindparam('RECORD_KEEPING'),
                                ORDER_STATUS = bindparam('ORDER_STATUS'),
                                Plan = bindparam('Plan'),
                                Device_Type = bindparam('Device_Type'),
                                Service_Carrier = bindparam('Service_Carrier'),
                                TABLET_SUBSIDY_QUALIFICATION = bindparam('TABLET_SUBSIDY_QUALIFICATION'),
                                Qualifying_Program = bindparam('Qualifying_Program'),
                                SOURCE = bindparam('SOURCE'),
                                AUTHORIZE_CODE = bindparam('AUTHORIZE_CODE'),
                                MASTER_AGENT_NAME = bindparam('MASTER_AGENT_NAME'),
                                DISTRIBUTOR_AGENT_NAME = bindparam('DISTRIBUTOR_AGENT_NAME'),
                                RETAILER_AGENT_NAME = bindparam('RETAILER_AGENT_NAME'),
                                Agent = bindparam('Agent'),
                                DL_NUMBER = bindparam('DL_NUMBER'),
                                WINBACK = bindparam('WINBACK'),
                                Account_Status = bindparam('Account_Status'),
                                PAYMENT_TYPE = bindparam('PAYMENT_TYPE'),
                                SPONSOR_ID = bindparam('SPONSOR_ID'),
                                CURRENT_APS = bindparam('CURRENT_APS'),
                                PORTIN_STATUS = bindparam('PORTIN_STATUS'),
                                Agent_LoginID = bindparam('Agent_LoginID'),
                                ACP_Status = bindparam('ACP_Status'),
                                Deactivation_Date = bindparam('Deactivation_Date'),
                                MEDICAID_ID = bindparam('MEDICAID_ID'),
                                BQP_FIRST_NAME = bindparam('BQP_FIRST_NAME'),
                                BQP_LAST_NAME = bindparam('BQP_LAST_NAME'),
                                BQP_BIRTH_DATE = bindparam('BQP_BIRTH_DATE'),
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
            conn.commit()
        return True
  

    def insert_changes_to_change_table(changes_dict:dict):
        counter = 0
        
        print(f'Length of changes: {len(changes_dict.keys())}')
        for field_name, id_dicts in changes_dict.items():
            counter += len(id_dicts.keys())

        print(f'Total number of change dicts to insert: {counter}')
        with engine.connect() as conn:
            for field_name, field_dict in changes_dict.items():
                print(f'Updating change table for field: {field_name}')
                list_of_data_dicts = []
                for individual_dict in field_dict.values():
                    list_of_data_dicts.append(individual_dict)
                conn.execute(insert(Merged_Database_Changes), list_of_data_dicts)
                conn.commit()

    def add_record_of_report_upload(changes:dict):
        with engine.connect() as conn:
            conn.execute(insert(Uploaded_Report_Records)
                                .values(
                                    user_id=current_user_id,
                                    report_type=report_type,
                                    number_of_new_records_added=len(changes['New Contacts']),
                                    date_uploaded=time.mktime(datetime.today().date().timetuple())
                                ))
            conn.commit()

    if checking_column_data(file_path):
        changes_dict = csv_to_db_upsert(file_path)
        insert_changes_to_change_table(changes_dict)
        add_record_of_report_upload(changes_dict)
        return 'Succesfully processed, thank you!'
    else:
        return 'Something in the columns doesnt seem to match...'

def run_file():
    yesterday_timestamp = time.mktime((datetime.today() - timedelta(days=1)).date().timetuple())
    terracom_zip_file_holder = get_files_from_sftp()
    for file in os.listdir(terracom_zip_file_holder):
        print(file)
        file_path = f'{terracom_zip_file_holder}/{file}'
        result = processing_activation_report(file_path, yesterday_timestamp, 1, 'Terracom Activation Report')
        print(result)
        os.remove(file_path)
    os.rmdir(terracom_zip_file_holder)


if __name__ == '__main__':
    program_start_time = datetime.now()
    run_file()
    print(f'Total program run time: {datetime.now() - program_start_time} \nSleep initiated for one hour.')