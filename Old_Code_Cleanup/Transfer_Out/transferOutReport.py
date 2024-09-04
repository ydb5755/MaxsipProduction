import csv
from sqlalchemy import create_engine, MetaData, Table, select, insert, update, bindparam
from xlsxwriter import Workbook
import os
import re
from datetime import datetime

def define_paths():
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'
    path_to_ntor_files = '/home/ubuntu/MaxsipReports/pythonFiles/TransferOutReports'
    return path_to_db, path_to_ntor_files

def storing_examples():
    example_dict = {
        'enrollID':{
            'field': 'value',
            'field': 'value',
            'field': 'value',
            'field': 'value',
            'field': 'value',
            'field': 'value'
        }
    }

def run_file():
    path_to_db, path_to_ntor_files = define_paths()
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    main_customer_view = Table("Main_Customer_Database", metadata_obj, autoload_with=engine)
    telgooChanges = Table("Telgoo_Changes", metadata_obj, autoload_with=engine)

    os.chdir(path_to_ntor_files)
    files = os.listdir()
    
    # Define a regular expression pattern to extract the numeric part
    pattern = re.compile(r'NTOR - (\d+)')

    # Sort the files based on the numeric part before the date range
    sorted_files = sorted(files, key=lambda file_name: int(pattern.search(file_name).group(1)))

    for file in sorted_files:
        print(f'Starting file: {file}')
        complete_file_path = path_to_ntor_files + '/' + file

        enroll_ids = {}
        
        with open(complete_file_path) as csvreadfile:
            csvreader = csv.reader(csvreadfile)
            fields_dict = {k:v for v, k in enumerate(next(csvreader))}
            del fields_dict['#']
            # for k, v in fields_dict.items():
            #     print(f'{k}: {v}')
            for row in csvreader:
                if not enroll_ids.get(row[fields_dict['ENROLL ID']]):
                    enroll_ids[row[fields_dict['ENROLL ID']]] = {}
                    for key, value in fields_dict.items():
                        enroll_ids[row[fields_dict['ENROLL ID']]][key] = row[value]
        
        with engine.connect() as conn:
            contacts = conn.execute(select(main_customer_view.c['Customer_Order_Id', 'Created_Date', 'Order_Date', 'Activation_Date', 'Device_Type', 'NLAD_Subscriber_ID'])
                                    .where(main_customer_view.c['Customer_Order_Id'].in_(list(enroll_ids.keys())))).all()
            imei_changes_by_enroll_id = conn.execute(select(telgooChanges.c['ENROLLMENT_ID'])
                                                    .where(telgooChanges.c['Field_Name'] == 'DEVICE_ID', telgooChanges.c['ENROLLMENT_ID'].in_(list(enroll_ids.keys())))).all()
            acp_changes_by_enroll_id = conn.execute(select(telgooChanges.c['ENROLLMENT_ID', 'New_Value', 'Change_Date'])
                                                    .where(telgooChanges.c['Field_Name'] == 'DISCONNECT_REASON', telgooChanges.c['ENROLLMENT_ID'].in_(list(enroll_ids.keys())))).all()
        
        # IMEI
        imei_change_count_dict = {}
        for imei_contact in imei_changes_by_enroll_id:
            if imei_change_count_dict.get(imei_contact[0], None):
                imei_change_count_dict[imei_contact[0]] += 1
            else:
                imei_change_count_dict[imei_contact[0]] = 1

        # ACP
        acp_change_dict = {}
        for acp_contact in acp_changes_by_enroll_id:
            row_data = [acp_contact[1], acp_contact[2]]
            if row_data[1]:
                row_data[1] = datetime.fromtimestamp(float(row_data[1])).strftime('%Y-%m-%d')
            if row_data[0] == '':
                row_data[0] = 'Enrolled'
            if acp_change_dict.get(acp_contact[0], None):
                acp_change_dict[acp_contact[0]] += [row_data[0], row_data[1]]
            else:
                acp_change_dict[acp_contact[0]] = [row_data[0], row_data[1]]

        # Details from contacts and join together with imei and acp
        data_for_insert_from_db = {}
        for contact in contacts:
            if imei_change_count_dict.get(contact[0], None):
                imei_changes = imei_change_count_dict[contact[0]]
            else:
                imei_changes = 0
            if acp_change_dict.get(contact[0], None):
                acp_data = acp_change_dict[contact[0]]
            else:
                acp_data = []
            
            data_for_insert_from_db[contact[0]] = {
                'Created Date':contact[1],
                'Order Date':contact[2], 
                'Activation Date':contact[3], 
                'Device Type':contact[4],
                'IMEI Changes': imei_changes,
                'Subscriber ID':contact[5],
                'ACP_Data':acp_data
            }
        wb_name = file[:-3] + 'xlsx'
        with Workbook(wb_name) as wb:
            ws = wb.add_worksheet()
            fields = list(fields_dict.keys())
            fields = fields[:fields.index('ENROLL ID')] + ['Created Date', 'Order Date', 'Activation Date', 'Device Type', 'IMEI Changes', 'Subscriber ID'] + fields[fields.index('ENROLL ID'):] + ['ACP_History']
            ws.write_row(0,0,fields)
            counter = 1
            for enroll_id, row_data in enroll_ids.items():
                fields = list(row_data.keys())
                values = list(row_data.values())
                index_of_enroll_id = fields.index('ENROLL ID')
                contact_from_db_with_new_data = data_for_insert_from_db.get(enroll_id, None)
                if contact_from_db_with_new_data:
                    data_no_acp = []
                    for k, v in contact_from_db_with_new_data.items():
                        if k == 'ACP_Data':
                            continue
                        if k in ['Created Date', 'Order Date', 'Activation Date']:
                            if v:
                                v = datetime.fromtimestamp(float(v)).strftime('%Y-%m-%d')
                        data_no_acp.append(v)
                    row_data = values[:index_of_enroll_id] + data_no_acp + values[index_of_enroll_id:] + contact_from_db_with_new_data['ACP_Data']
                else:
                    row_data = values[:index_of_enroll_id] + [' ', ' ', ' ', ' ', ' ', ' '] + values[index_of_enroll_id:]
                ws.write_row(counter, 0, row_data)
                counter += 1
            

    
            
        




if __name__ == '__main__':
    run_file()