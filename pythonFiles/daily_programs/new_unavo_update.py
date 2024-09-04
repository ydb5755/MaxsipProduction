from sqlalchemy import create_engine, MetaData, Table, select, insert, func, update, bindparam
import os
import csv
import paramiko
from datetime import datetime, timedelta
from dateutil.parser import parse
import time
import json


def get_file_from_sftp():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    with open('/etc/config.json') as config_file:
        config = json.load(config_file)    
    ssh_client.connect(config.get('UNAVO_SFTP_HOSTNAME'), port=22, username=config.get('SFTP_USERNAME'), password=config.get('UNAVO_SFTP_PASSWORD'))
    
    sftp_client = ssh_client.open_sftp()

    remote_directory = '/reports/AccountMgmtReport'
    local_directory = '/home/ubuntu/MaxsipReports/pythonFiles'

    file_attributes = sftp_client.listdir_attr(remote_directory)
    sorted_files_with_attributes_by_recent = sorted(file_attributes, key=lambda x: x.st_mtime, reverse=True)
    latest_file_attr = sorted_files_with_attributes_by_recent[0]

    remote_filename = f'{remote_directory}/{latest_file_attr.filename}'
    local_filename = f'{local_directory}/{latest_file_attr.filename}'
    
    sftp_client.get(remote_filename, local_filename)

    sftp_client.close()
    ssh_client.close()
    return local_filename


def get_num_new_rows(current_file):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        previous_day_counter = conn.execute(select(func.count())
                                            .select_from(Merged_Customer_Databases)
                                            .where(Merged_Customer_Databases.c['Source_Database'] == 'Unavo')).first()[0]

    current_day_counter = 0
    with open(current_file) as current_file:
        csvreader = csv.reader(current_file)
        for _ in csvreader:
            current_day_counter += 1

    # Calculate the number of new rows
    num_new_rows = max(0, (current_day_counter - 1) - previous_day_counter)
    print(f'Number of new rows: {num_new_rows}')
    return num_new_rows

def declare_changes_dict() -> dict:
    changes = {}
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    columns = Merged_Customer_Databases.c.keys()
    for field in columns:
        changes[field] = {}
    return changes

def checking_column_data(current_file):
    expected_fields = ['CLEC','AgentName','Account','OrderId','StatusType','FirstName','LastName','SSN',
                       'Email','MDN','MSID','State','City','ESN','CustomerPackage','CSA','StreetAddress',
                       'Apartment#orSuite#','Zipcode','PrimaryPhone','DOB','ShipmentDate','Tracking#',
                       'DeviceId','DataLastUsed','VoiceTextLastUsed','NladProgram','LifelineSubscriberId',
                       'LifelineStatus','NladErrorType','IsAcp','AcpStatus','deviceReimbursementDate','DateAssigned',
                       'ActivationDate','DeActivationDate','PORTSTATUS','IMEI','IpAddress','AcpLifeLineCertificationType',
                       'WirelessProviderType','IsProofOfBenefitsUploaded','IsIdentityProofUploaded','IsTribal','TribalID',
                       'National Verifier Application ID','ACP SubscriberID','AgentFollowupPeriod','FollowupPeriod_CreatedDate',
                       'FollowupPeriodCreatedAuthor','Model','ModelNumber','DeviceType','FCC CheckBox','Software Consent','Customer Price','ConsentCheck']
    with open(current_file) as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
    if expected_fields == fields:
        print('columns match')
        return True
    else:
        return False

def column_mapping_unavo_to_merged(desired_map:str) -> dict:
    unavo_csv_to_merged = {
        'CLEC':'CLEC',
        'AgentName':'Agent',
        'Account':'Misc_Customer_ID',
        'OrderId':'Customer_Order_ID',
        'StatusType':'Account_Status',
        'FirstName':'First_Name',
        'LastName':'Last_Name',
        'SSN':'SSN',
        'Email':'Email',
        'MDN':'MDN', #10
        'MSID':'MSID',
        'State':'State',
        'City':'City',
        'ESN':'ESN',
        'CustomerPackage':'Plan',
        'CSA':'CSA',
        'StreetAddress':'Address1',
        'Apartment#orSuite#':'Address2',
        'Zipcode':'Zip',
        'PrimaryPhone':'Phone1', #20
        'DOB':'DOB',
        'ShipmentDate':'ShipmentDate',
        'Tracking#':'TrackingNum',
        'DeviceId':'DeviceId',
        'DataLastUsed':'Last_Used_Data',
        'VoiceTextLastUsed':'Last_Used_Phone',
        'NladProgram':'NladProgram',
        'LifelineSubscriberId':'LifelineSubscriberId',
        'LifelineStatus':'LifelineStatus',
        'NladErrorType':'NladErrorType', #30
        'IsAcp':'IsAcp',
        'AcpStatus':'ACP_Status',
        'deviceReimbursementDate':'deviceReimbursementDate',
        'DateAssigned':'Order_Date',
        'ActivationDate':'Activation_Date',
        'DeActivationDate':'Deactivation_Date',
        'PORTSTATUS':'PORTSTATUS',
        'IMEI':'IMEI',
        'IpAddress':'IpAddress',
        'AcpLifeLineCertificationType':'Qualifying_Program', #40
        'WirelessProviderType':'Service_Carrier',
        'IsProofOfBenefitsUploaded':'IsProofOfBenefitsUploaded',
        'IsIdentityProofUploaded':'IsIdentityProofUploaded',
        'IsTribal':'IsTribal',
        'TribalID':'TribalID',
        'National Verifier Application ID':'National_Verifier_Application_ID',
        'ACP SubscriberID':'NLAD_Subscriber_ID',
        'AgentFollowupPeriod':'AgentFollowupPeriod',
        'FollowupPeriod_CreatedDate':'FollowupPeriod_CreatedDate',
        'FollowupPeriodCreatedAuthor':'FollowupPeriodCreatedAuthor',#50
        'Model':'Device_Mfg',
        'ModelNumber':'Device_Model_Number',
        'DeviceType':'Device_Type',
        'FCC CheckBox':'FCC_CheckBox',
        'Software Consent':'Software_Consent',
        'Customer Price':'Customer_Price',
        'ConsentCheck':'ConsentCheck', #57
    }
    unavo_db_to_merged = {
        'CLEC':'CLEC',
        'AgentName':'Agent',
        'Account':'Misc_Customer_ID',
        'OrderId':'Customer_Order_ID',
        'StatusType':'Account_Status',
        'FirstName':'First_Name',
        'LastName':'Last_Name',
        'SSN':'SSN',
        'Email':'Email',
        'MDN':'MDN', #10
        'MSID':'MSID',
        'State':'State',
        'City':'City',
        'ESN':'ESN',
        'CustomerPackage':'Plan',
        'CSA':'CSA',
        'StreetAddress':'Address1',
        'ApartmentNumOrSuiteNum':'Address2',
        'Zipcode':'Zip',
        'PrimaryPhone':'Phone1', #20
        'DOB':'DOB',
        'ShipmentDate':'ShipmentDate',
        'TrackingNum':'TrackingNum',
        'DeviceId':'DeviceId',
        'DataLastUsed':'Last_Used_Data',
        'VoiceTextLastUsed':'Last_Used_Phone',
        'NladProgram':'NladProgram',
        'LifelineSubscriberId':'LifelineSubscriberId',
        'LifelineStatus':'LifelineStatus',
        'NladErrorType':'NladErrorType', #30
        'IsAcp':'IsAcp',
        'AcpStatus':'ACP_Status',
        'deviceReimbursementDate':'deviceReimbursementDate',
        'DateAssigned':'Order_Date',
        'ActivationDate':'Activation_Date',
        'DeActivationDate':'Deactivation_Date',
        'PORTSTATUS':'PORTSTATUS',
        'IMEI':'IMEI',
        'IpAddress':'IpAddress',
        'AcpLifeLineCertificationType':'Qualifying_Program', #40
        'WirelessProviderType':'Service_Carrier',
        'IsProofOfBenefitsUploaded':'IsProofOfBenefitsUploaded',
        'IsIdentityProofUploaded':'IsIdentityProofUploaded',
        'IsTribal':'IsTribal',
        'TribalID':'TribalID',
        'National_Verifier_Application_ID':'National_Verifier_Application_ID',
        'ACP_SubscriberID':'NLAD_Subscriber_ID',
        'AgentFollowupPeriod':'AgentFollowupPeriod',
        'FollowupPeriod_CreatedDate':'FollowupPeriod_CreatedDate',
        'FollowupPeriodCreatedAuthor':'FollowupPeriodCreatedAuthor',#50
        'Model':'Device_Mfg',
        'ModelNumber':'Device_Model_Number',
        'DeviceType':'Device_Type',
        'FCC_CheckBox':'FCC_CheckBox',
        'Software_Consent':'Software_Consent',
        'Customer_Price':'Customer_Price',
        'ConsentCheck':'ConsentCheck',
        'CreatedDate':'Created_Date' #58
    }

    if desired_map == 'unavo_db_to_merged':
        return unavo_db_to_merged
    elif desired_map == 'unavo_csv_to_merged':
        return unavo_csv_to_merged
    else:
        return 'Requested map doesnt exist, try "unavo_db_to_merged" or "unavo_csv_to_merged"'


def get_merged_db_unavo_contacts_with_col_value(merged_col:str, path_to_db:str) -> dict:
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        stmt = select(Merged_Customer_Databases.c['Customer_Order_ID', merged_col])\
                                        .where(Merged_Customer_Databases.c['Source_Database'] == 'Unavo')
        merged_db_contacts= conn.execute(stmt).all()
        stmt_selected_cols = stmt.selected_columns.keys()
        order_id_index = stmt_selected_cols.index('Customer_Order_ID')
        merged_col_index = stmt_selected_cols.index(merged_col)

    merged_db_unavo_contacts_with_col_value = {}
    for contact in merged_db_contacts:
        merged_db_unavo_contacts_with_col_value[contact[order_id_index]] = contact[merged_col_index]
    
    return merged_db_unavo_contacts_with_col_value
 
def get_csv_unavo_contacts(csv_file:str, unavo_col:str) -> dict:
    csv_unavo_contacts = {}
    with open(csv_file, encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        order_id_index = fields.index('OrderId')
        current_selected_unavo_col_index = fields.index(unavo_col)
        for csv_reader_contact in csvreader:
            if len(csv_reader_contact) == len(fields):
                csv_unavo_contacts[csv_reader_contact[order_id_index]] = csv_reader_contact[current_selected_unavo_col_index]
    return csv_unavo_contacts

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
    print(f'Length of changes post filtering: {len(changes.keys())}')
    return changes

def get_customer_order_ids_to_insert(path_to_db:str, csv_file:str):
    customer_order_ids_to_insert = set()
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        db_order_ids = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID'])
                                        .where(Merged_Customer_Databases.c['Source_Database'] == 'Unavo')).all()
    db_order_ids_indexed = {k[0]:k[0] for k in db_order_ids}
    with open(csv_file) as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        order_id_index = fields.index('OrderId')
        for contact in csvreader:
            contact_order_id = contact[order_id_index]
            if not db_order_ids_indexed.get(contact_order_id, None):
                customer_order_ids_to_insert.add(contact_order_id)
    return customer_order_ids_to_insert

def get_customer_order_ids_with_changes(changes:dict) -> set:
    customer_order_ids_with_changes = set()

    for field in changes.values():
        for order_id_key in field.keys():
            customer_order_ids_with_changes.add(order_id_key)

    return customer_order_ids_with_changes

def get_changes(then, csv_file, path_to_db, yesterday) -> dict:
    changes = declare_changes_dict()

    unavo_to_merged_columns = column_mapping_unavo_to_merged('unavo_csv_to_merged')

    for unavo_col, merged_col in unavo_to_merged_columns.items():
        print(f'Time passed: {datetime.now()-then}. Starting {merged_col}')
        
        merged_db_unavo_contacts_with_col_value = get_merged_db_unavo_contacts_with_col_value(merged_col, path_to_db)
        csv_unavo_contacts = get_csv_unavo_contacts(csv_file, unavo_col)

        for order_id, col_value in csv_unavo_contacts.items():
            if merged_db_unavo_contacts_with_col_value.get(order_id, None):
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

                if merged_db_unavo_contacts_with_col_value[order_id] != col_value:
                    changes[merged_col][order_id] = {
                        'Source_Database':'Unavo',
                        'Customer_Order_ID':order_id,
                        'Nlad_Subscriber_ID':'',
                        'Field_Name':merged_col,
                        'New_Value':col_value,
                        'Old_Value':merged_db_unavo_contacts_with_col_value[order_id],
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

def insert_changes_to_change_table(path_to_db, changes_dict:dict):
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Database_Changes = Table("Merged_Database_Changes", metadata_obj, autoload_with=engine)
    
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


def add_new_contacts_to_changes(indexed_ids_for_insert:dict, path_to_csv:str, changes:dict, yesterday:float) -> dict:
    changes['New Contacts'] = {}
    with open(path_to_csv) as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        fields_dict = {}
        for field_num, field in enumerate(fields):
            fields_dict[field] = field_num
        for contact in csvreader:
            customer_order_id = contact[fields_dict['OrderId']]
            if indexed_ids_for_insert.get(customer_order_id, None):
                changes['New Contacts'][customer_order_id] = {
                        'Source_Database':'Unavo',
                        'Customer_Order_ID':customer_order_id,
                        'Nlad_Subscriber_ID':contact[fields_dict['ACP SubscriberID']],
                        'Field_Name':'New Contacts',
                        'New_Value':'',
                        'Old_Value':'',
                        'Change_Date':yesterday,
                        'Activation_Date':contact[fields_dict['ActivationDate']],
                        'CLEC':'',
                        'Plan':contact[fields_dict['CustomerPackage']],
                        'Agent':contact[fields_dict['AgentName']],
                        'Account_Status':contact[fields_dict['StatusType']],
                        'ACP_Status':contact[fields_dict['AcpStatus']],
                        'MDN':contact[fields_dict['MDN']]
                }

    return changes

def csv_to_db_upsert(path_to_db, csv_file, yesterday):
    then = datetime.now()

    changes_pre_new_contacts = get_changes(then, csv_file, path_to_db, yesterday)

    customer_order_ids_to_insert = get_customer_order_ids_to_insert(path_to_db, csv_file)
    customer_order_ids_with_changes = get_customer_order_ids_with_changes(changes_pre_new_contacts)

    print(f'Length of customer order ids to insert: {len(customer_order_ids_to_insert)}')
    print(f'Length of customer order ids to change: {len(customer_order_ids_with_changes)}')
    print(f'Time passed: {datetime.now()-then}. Finished going through columns for changes')

    indexed_ids_for_insert = {k:k for k in customer_order_ids_to_insert}
    indexed_ids_for_update = {k:k for k in customer_order_ids_with_changes}

    changes_with_new_contacts = add_new_contacts_to_changes(indexed_ids_for_insert, csv_file, changes_pre_new_contacts, yesterday)
    
    whole_rows_for_update = []
    whole_rows_for_insert = []

    unavo_csv_to_merged_columns = column_mapping_unavo_to_merged('unavo_csv_to_merged')
    with open(csv_file, encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        fields_dict = {}
        for field_num, field in enumerate(fields):
            fields_dict[field] = field_num
        for contact in csvreader:
            if len(contact) != len(fields):
                continue
            contact_order_id = contact[fields_dict['OrderId']]
            if indexed_ids_for_update.get(contact_order_id, None):
                whole_row_columns_and_data = {}
                for column, index in fields_dict.items():
                    if column in ['DateAssigned','ActivationDate','DeActivationDate','DOB','DataLastUsed',
                                    'VoiceTextLastUsed','ShipmentDate','deviceReimbursementDate','FollowupPeriod_CreatedDate']:
                        if contact[index]:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = parse(contact[index]).timestamp()
                        else:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = ''
                    elif column == 'OrderId':
                        whole_row_columns_and_data['b_Customer_Order_ID'] = contact[index]
                    elif column == 'SSN':
                        if contact[index]:
                            temp_data = contact[index]
                            if len(temp_data) > 2:
                                if temp_data[-2] == '.':
                                    temp_data = temp_data[:-2]
                            digits = len(temp_data)
                            if digits == 4:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = temp_data
                            elif digits == 3:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'0{temp_data}'
                            elif digits == 2:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'00{temp_data}'
                            elif digits == 1:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'000{temp_data}'
                            else:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = temp_data
                        else:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = ''
                    else:
                        if contact[index]:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = contact[index]
                        else:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = ''
                whole_rows_for_update.append(whole_row_columns_and_data)
                for field_name, contact_change_lists in changes_with_new_contacts.items():
                    if contact_change_lists.get(contact_order_id, None):
                        if contact[fields_dict['ActivationDate']]:
                            changes_with_new_contacts[field_name][contact_order_id]['Activation_Date'] = parse(contact[fields_dict['ActivationDate']]).timestamp()
                        else:
                            changes_with_new_contacts[field_name][contact_order_id]['Activation_Date'] = ''
                        changes_with_new_contacts[field_name][contact_order_id]['Nlad_Subscriber_ID'] = contact[fields_dict['ACP SubscriberID']]
                        changes_with_new_contacts[field_name][contact_order_id]['CLEC'] = contact[fields_dict['CLEC']]
                        changes_with_new_contacts[field_name][contact_order_id]['Plan'] = contact[fields_dict['CustomerPackage']]
                        changes_with_new_contacts[field_name][contact_order_id]['Agent'] = contact[fields_dict['AgentName']]
                        changes_with_new_contacts[field_name][contact_order_id]['Account_Status'] = contact[fields_dict['StatusType']]
                        changes_with_new_contacts[field_name][contact_order_id]['ACP_Status'] = contact[fields_dict['AcpStatus']]
                        changes_with_new_contacts[field_name][contact_order_id]['MDN'] = contact[fields_dict['MDN']]
            elif indexed_ids_for_insert.get(contact_order_id, None):
                whole_row_columns_and_data = {}
                whole_row_columns_and_data['Source_Database'] = 'Unavo'
                whole_row_columns_and_data['ENROLLMENT_TYPE'] = 'ACP'
                whole_row_columns_and_data['Created_Date'] = yesterday
                for column, index in fields_dict.items():
                    if column in ['DateAssigned','ActivationDate','DeActivationDate','DOB','DataLastUsed',
                                    'VoiceTextLastUsed','ShipmentDate','deviceReimbursementDate','FollowupPeriod_CreatedDate']:
                        if contact[index]:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = parse(contact[index]).timestamp()
                        else:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = ''
                    elif column == 'SSN':
                        if contact[index]:
                            temp_data = contact[index]
                            if len(temp_data) > 2:
                                if temp_data[-2] == '.':
                                    temp_data = temp_data[:-2]
                            digits = len(temp_data)
                            if digits == 4:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = temp_data
                            elif digits == 3:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'0{temp_data}'
                            elif digits == 2:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'00{temp_data}'
                            elif digits == 1:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = f'000{temp_data}'
                            else:
                                whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = temp_data
                        else:
                            whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = ''
                    else:
                        whole_row_columns_and_data[unavo_csv_to_merged_columns[column]] = contact[index]
                whole_rows_for_insert.append(whole_row_columns_and_data)
            
            length_of_rows_insert = len(whole_rows_for_insert)
            length_of_rows_update = len(whole_rows_for_update)
            if length_of_rows_insert % 500 == 0 and length_of_rows_insert > 0:
                db_connect_and_insert(path_to_db, whole_rows_for_insert)
                whole_rows_for_insert = []
            if length_of_rows_update % 500 == 0 and length_of_rows_update > 0:
                db_connect_and_update(path_to_db, whole_rows_for_update)
                whole_rows_for_update = []
        if len(whole_rows_for_insert) > 0:
            db_connect_and_insert(path_to_db, whole_rows_for_insert)
            whole_rows_for_insert = []
        print('DB inserting complete')
        if len(whole_rows_for_update) > 0:
            db_connect_and_update(path_to_db, whole_rows_for_update)
            whole_rows_for_update = []
        print('DB updating complete')
    return changes_with_new_contacts
        
def db_connect_and_update(path_to_db, list_of_row_dicts):
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        conn.execute(update(Merged_Customer_Databases)
                     .where(Merged_Customer_Databases.c.Customer_Order_ID == bindparam('b_Customer_Order_ID'))
                    .values(CLEC = bindparam('CLEC'),
                                Agent = bindparam('Agent'),
                                Misc_Customer_ID = bindparam('Misc_Customer_ID'),
                                Account_Status = bindparam('Account_Status'),
                                First_Name = bindparam('First_Name'),
                                Last_Name = bindparam('Last_Name'),
                                SSN = bindparam('SSN'),
                                Email = bindparam('Email'),
                                MDN = bindparam('MDN'),
                                MSID = bindparam('MSID'),
                                State = bindparam('State'),
                                City = bindparam('City'),
                                ESN = bindparam('ESN'),
                                Plan = bindparam('Plan'),
                                CSA = bindparam('CSA'),
                                Address1 = bindparam('Address1'),
                                Address2 = bindparam('Address2'),
                                Zip = bindparam('Zip'),
                                Phone1 = bindparam('Phone1'),
                                DOB = bindparam('DOB'),
                                ShipmentDate = bindparam('ShipmentDate'),
                                TrackingNum = bindparam('TrackingNum'),
                                DeviceId = bindparam('DeviceId'),
                                Last_Used_Data = bindparam('Last_Used_Data'),
                                Last_Used_Phone = bindparam('Last_Used_Phone'),
                                NladProgram = bindparam('NladProgram'),
                                LifelineSubscriberId = bindparam('LifelineSubscriberId'),
                                LifelineStatus = bindparam('LifelineStatus'),
                                NladErrorType = bindparam('NladErrorType'),
                                IsAcp = bindparam('IsAcp'),
                                ACP_Status = bindparam('ACP_Status'),
                                deviceReimbursementDate = bindparam('deviceReimbursementDate'),
                                Order_Date = bindparam('Order_Date'),
                                Activation_Date = bindparam('Activation_Date'),
                                Deactivation_Date = bindparam('Deactivation_Date'),
                                PORTSTATUS = bindparam('PORTSTATUS'),
                                IMEI = bindparam('IMEI'),
                                IpAddress = bindparam('IpAddress'),
                                Qualifying_Program = bindparam('Qualifying_Program'),
                                Service_Carrier = bindparam('Service_Carrier'),
                                IsProofOfBenefitsUploaded = bindparam('IsProofOfBenefitsUploaded'),
                                IsIdentityProofUploaded = bindparam('IsIdentityProofUploaded'),
                                IsTribal = bindparam('IsTribal'),
                                TribalID = bindparam('TribalID'),
                                National_Verifier_Application_ID = bindparam('National_Verifier_Application_ID'),
                                NLAD_Subscriber_ID = bindparam('NLAD_Subscriber_ID'),
                                AgentFollowupPeriod = bindparam('AgentFollowupPeriod'),
                                FollowupPeriod_CreatedDate = bindparam('FollowupPeriod_CreatedDate'),
                                FollowupPeriodCreatedAuthor = bindparam('FollowupPeriodCreatedAuthor'),
                                Device_Mfg = bindparam('Device_Mfg'),
                                Device_Model_Number = bindparam('Device_Model_Number'),
                                Device_Type = bindparam('Device_Type'),
                                FCC_CheckBox = bindparam('FCC_CheckBox'),
                                Software_Consent = bindparam('Software_Consent'),
                                Customer_Price = bindparam('Customer_Price'),
                                ConsentCheck = bindparam('ConsentCheck'))
                     , list_of_row_dicts)
        conn.commit()
    return True

def db_connect_and_insert(path_to_db, list_of_row_dicts):
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        stmt = insert(Merged_Customer_Databases)
        conn.execute(stmt, list_of_row_dicts)
        conn.commit()
    return True

def add_record_of_report_upload(path_to_db:str, changes:dict):
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Uploaded_Report_Records = Table("Uploaded_Report_Records", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        conn.execute(insert(Uploaded_Report_Records)
                            .values(
                                user_id=1,
                                report_type='Unavo Customer Report',
                                number_of_new_records_added=len(changes['New Contacts']),
                                date_uploaded=time.mktime(datetime.today().date().timetuple())
                            ))
        conn.commit()
    print('Record created for uploaded unavo file')


def run_program():
    yesterday = time.mktime((datetime.today() - timedelta(days=1)).date().timetuple())
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'
    current_file = get_file_from_sftp()
    print(f'got file: {current_file}')
    if current_file:
        if checking_column_data(current_file):
            changes_dict = csv_to_db_upsert(path_to_db, current_file, yesterday)
            insert_changes_to_change_table(path_to_db, changes_dict)
            add_record_of_report_upload(path_to_db, changes_dict)
            os.remove(current_file)
            return f'Program finished. '
        else:
            return 'Columns dont match, upsert did not happen'
    else:
        return 'Download Error'


if __name__ == '__main__':
    program_start_time = datetime.now()
    result = run_program()
    print(f'Result: {result}\nTotal program run time: {datetime.now() - program_start_time} \nSleep initiated for one hour.')