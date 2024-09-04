import os
import csv
import time
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.DEBUG)
from app import celery
from sqlalchemy import create_engine, MetaData, Table, select, insert
from sqlalchemy.sql.expression import func
from dateutil.parser import parse
from datetime import datetime
from xlsxwriter import Workbook

@celery.task
def ntor_generation(start_date: datetime, end_date: datetime, user_id:int):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
    
    start_date_string = start_date.strftime('%m-%d-%Y')
    end_date_string = end_date.strftime('%m-%d-%Y')
    start_date_timestamp = time.mktime(start_date.timetuple())
    end_date_timestamp = time.mktime(end_date.timetuple())

    def generate_second_worksheet(contacts_details_by_id_map:dict, txo_total:int) -> dict: 
        tx_outs_by_age_detail = {}
        tx_out_by_MA_employee_age_summary = {}

        for single_contact_info in contacts_details_by_id_map.values():
            if tx_outs_by_age_detail.get(single_contact_info['Days in Effect'], None):
                tx_outs_by_age_detail[single_contact_info['Days in Effect']]['Total Transfer Outs'] += 1
            else:
                tx_outs_by_age_detail[single_contact_info['Days in Effect']] = {
                    'Days In Effect': single_contact_info['Days in Effect'],
                    'Total Transfer Outs': 1,
                    'Percent Of all TRX Outs':''
                }
            ma_employee = f'{single_contact_info["MASTER AGENT"]}:{single_contact_info["EMPLOYEE"]}'
            if tx_out_by_MA_employee_age_summary.get(ma_employee, None):
                tx_out_by_MA_employee_age_summary[ma_employee][single_contact_info['Day Range']] += 1
            else:
                tx_out_by_MA_employee_age_summary[ma_employee] = {
                    '0-30':0,
                    '31-60':0,
                    '61-90':0,
                    '91-120':0,
                    '121-360':0,
                    '>360':0
                }
                tx_out_by_MA_employee_age_summary[ma_employee][single_contact_info['Day Range']] += 1
        
        for age_dict in tx_outs_by_age_detail.values():
            percentage = str((age_dict['Total Transfer Outs'] / txo_total)*100)
            index_of_period = percentage.index('.')
            shortened_percentage = percentage[:index_of_period + 2]
            age_dict['Percent Of all TRX Outs'] = float(shortened_percentage)
        
        return tx_outs_by_age_detail, tx_out_by_MA_employee_age_summary

    def get_max_id_plus_one() -> int:
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
        with engine.connect() as conn:
            max_report_id = conn.scalar(select(func.max(User_Generated_Reports_Table.c.id)))
            if not max_report_id:
                max_report_id = 0
            return max_report_id + 1

    def get_transfer_outs_and_its_cols(start_date_timestamp:float, end_date_timestamp:float):
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        Transfer_Outs = Table("Transfer_Outs", metadata_obj, autoload_with=engine)
        with engine.connect() as conn:
            stmt = select(Transfer_Outs.c['id', 'TRANSACTION_DATE', 'ACTIVATION_DATE', 'MASTER_AGENT', 'EMPLOYEE', 'ENROLL_ID'])\
            .where(Transfer_Outs.c['TRANSACTION_DATE'] >= start_date_timestamp, Transfer_Outs.c['TRANSACTION_DATE'] <= end_date_timestamp)

            columns_of_transfer_out_selection = {k:v for v, k in enumerate(stmt.selected_columns.keys())}
            transfers = conn.execute(stmt).all()
        return transfers, columns_of_transfer_out_selection

    def get_imei_changes(enrollment_ids:set, start_date_timestamp:float) -> dict:
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        Merged_Database_Changes = Table("Merged_Database_Changes", metadata_obj, autoload_with=engine)
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID']).where(Merged_Database_Changes.c['Field_Name'] == 'IMEI', 
                                                                  Merged_Database_Changes.c['Customer_Order_ID'].in_(enrollment_ids),
                                                                  Merged_Database_Changes.c['Change_Date'] < start_date_timestamp)
            imei_changes_by_enroll_id = conn.execute(stmt).all()
        imei_change_count_dict = {}
        for imei_contact in imei_changes_by_enroll_id:
            if imei_change_count_dict.get(imei_contact[0], None):
                imei_change_count_dict[imei_contact[0]] += 1
            else:
                imei_change_count_dict[imei_contact[0]] = 1
        return imei_change_count_dict

    def get_acp_changes(enrollment_ids:set, start_date_timestamp:float) -> dict:
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        Merged_Database_Changes = Table("Merged_Database_Changes", metadata_obj, autoload_with=engine)
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'New_Value', 'Change_Date']).where(Merged_Database_Changes.c['Field_Name'] == 'ACP_Status', 
                                                                                              Merged_Database_Changes.c['Customer_Order_ID'].in_(enrollment_ids),
                                                                                              Merged_Database_Changes.c['Change_Date'] < start_date_timestamp)
            indexes_for_columns_of_acp_changes_dict = {k:v for v, k in enumerate(stmt.selected_columns.keys())}
            acp_changes_by_enroll_id = conn.execute(stmt).all()
        acp_change_dict = {}
        for acp_contact in acp_changes_by_enroll_id:
            row_data = [x for x in acp_contact]
            if row_data[indexes_for_columns_of_acp_changes_dict['Change_Date']]:
                row_data[indexes_for_columns_of_acp_changes_dict['Change_Date']] = datetime.fromtimestamp(float(row_data[indexes_for_columns_of_acp_changes_dict['Change_Date']])).strftime('%Y-%m-%d')
            if row_data[indexes_for_columns_of_acp_changes_dict['New_Value']] == '':
                row_data[indexes_for_columns_of_acp_changes_dict['New_Value']] = 'Enrolled'
            if acp_change_dict.get(indexes_for_columns_of_acp_changes_dict['Customer_Order_ID'], None):
                acp_change_dict[indexes_for_columns_of_acp_changes_dict['Customer_Order_ID']] += [row_data[indexes_for_columns_of_acp_changes_dict['New_Value']], row_data[indexes_for_columns_of_acp_changes_dict['Change_Date']]]
            else:
                acp_change_dict[indexes_for_columns_of_acp_changes_dict['Customer_Order_ID']] = [row_data[indexes_for_columns_of_acp_changes_dict['New_Value']], row_data[indexes_for_columns_of_acp_changes_dict['Change_Date']]]
        return acp_change_dict

    def get_contacts_from_main_db(enrollment_ids:set):
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
        with engine.connect() as conn:
            stmt = select(Merged_Customer_Databases.c['Customer_Order_ID', 'Created_Date', 'Order_Date', 'Activation_Date', 'Device_Type', 'NLAD_Subscriber_ID'])\
                                    .where(Merged_Customer_Databases.c['Customer_Order_ID'].in_(enrollment_ids))
            
            contacts_from_main_db = conn.execute(stmt).all()
        return contacts_from_main_db

    def collect_enrollment_ids_and_organize_contact_data_into_sensible_dict():
        transfers, transfer_out_db_cols = get_transfer_outs_and_its_cols(start_date_timestamp, end_date_timestamp)
        contacts_details_by_id_map = {}
        enrollment_ids = set()
        for contact in transfers:
            if not contact[transfer_out_db_cols['ACTIVATION_DATE']] or not contact[transfer_out_db_cols['TRANSACTION_DATE']]:
                continue
            enrollment_ids.add(contact[transfer_out_db_cols['ENROLL_ID']])
            contacts_details_by_id_map[contact[transfer_out_db_cols['id']]] = {
                    'id':contact[transfer_out_db_cols['id']],
                    'Employee Master:Agent':f"{contact[transfer_out_db_cols['MASTER_AGENT']]}:{contact[transfer_out_db_cols['EMPLOYEE']]}",
                    'Days in Effect':(float(contact[transfer_out_db_cols['TRANSACTION_DATE']]) - float(contact[transfer_out_db_cols['ACTIVATION_DATE']])) / 86400,
                    'Day Range':'>360',
                    'ENROLL ID': contact[transfer_out_db_cols['ENROLL_ID']],
                    'Created Date':'',
                    'Order Date':'',
                    'Activation Date DB':'',
                    'Device Type':'',
                    'IMEI Changes':'',
                    'Subscriber ID':'',
                    'TRANSACTION DATE': contact[transfer_out_db_cols['TRANSACTION_DATE']],
                    'ACTIVATION DATE': contact[transfer_out_db_cols['ACTIVATION_DATE']],
                    'MASTER AGENT': contact[transfer_out_db_cols['MASTER_AGENT']],
                    'EMPLOYEE': contact[transfer_out_db_cols['EMPLOYEE']]
            }
            days_in_effect = int(contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Days in Effect'])
            if days_in_effect <= 30:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '0-30'
            elif 30 < days_in_effect < 61:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '31-60'
            elif 60 < days_in_effect < 91:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '61-90'
            elif 90 < days_in_effect < 121:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '91-120'
            elif 120 < days_in_effect < 361:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '121-360'
            elif 360 < days_in_effect:
                contacts_details_by_id_map[contact[transfer_out_db_cols['id']]]['Day Range'] = '>360'
        return enrollment_ids, contacts_details_by_id_map

    def completing_missing_info_from_organized_contact_data_dict_and_dict_for_acp_data(contacts_from_main_db, 
                                                                                       imei_change_count_dict:dict, 
                                                                                       acp_change_dict:dict, 
                                                                                       contacts_details_by_id_map:dict) -> tuple[dict, dict]:
        # Details from contacts and join together with imei and acp
        data_for_insert_from_db = {}
        for contact in contacts_from_main_db:
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

        for id, contact_dict in contacts_details_by_id_map.items():
            if data_for_insert_from_db.get(contact_dict['ENROLL ID'], None):
                db_data_contact = data_for_insert_from_db[contact_dict['ENROLL ID']]
                contact_dict['Created Date'] = db_data_contact['Created Date']
                contact_dict['Order Date'] = db_data_contact['Order Date']
                contact_dict['Activation Date DB'] = db_data_contact['Activation Date']
                contact_dict['Device Type'] = db_data_contact['Device Type']
                contact_dict['IMEI Changes'] = db_data_contact['IMEI Changes']
                contact_dict['Subscriber ID'] = db_data_contact['Subscriber ID']
        return contacts_details_by_id_map, data_for_insert_from_db

    def writing_first_worksheet(wb:Workbook):
        ws_one = wb.add_worksheet('Trx-Out Analysis Summary Report')
        cols = list(completed_contact_data_dict[list(completed_contact_data_dict.keys())[0]].keys())
        cols += ['Last ACP Status-1', 'Date', 'Last ACP Status-2', 'Date', 'Last ACP Status-3', 'Date', 'Last ACP Status-4', 'Date', 'Last ACP Status-5', 'Date', 'Last ACP Status-6', 'Date']
        ws_one.write_row(0,0,cols)
        ws_one_counter = 1
        for id, id_dict in completed_contact_data_dict.items():
            row_data = []
            for k, v in id_dict.items():
                if k in ['TRANSACTION DATE', 'ACTIVATION DATE', 'Created Date', 'Order Date', 'Activation Date DB']:
                    if v:
                        v = datetime.fromtimestamp(float(v)).strftime('%Y-%m-%d')
                row_data.append(v)
            if id_dict['ENROLL ID'] in data_for_insert_from_db.keys():
                acp_data = data_for_insert_from_db[id_dict['ENROLL ID']]['ACP_Data']
                for cell in acp_data:
                    row_data.append(cell)
            ws_one.write_row(ws_one_counter,0,row_data)
            ws_one_counter += 1
        return ws_one_counter


    new_report_id = get_max_id_plus_one()
    enrollment_ids, contacts_details_by_id_map = collect_enrollment_ids_and_organize_contact_data_into_sensible_dict()
            
    contacts_from_main_db = get_contacts_from_main_db(enrollment_ids)
    imei_change_count_dict = get_imei_changes(enrollment_ids, start_date_timestamp)
    acp_change_dict = get_acp_changes(enrollment_ids, start_date_timestamp)

    completed_contact_data_dict, data_for_insert_from_db = completing_missing_info_from_organized_contact_data_dict_and_dict_for_acp_data(contacts_from_main_db, 
                                                                                                                                          imei_change_count_dict, 
                                                                                                                                          acp_change_dict, 
                                                                                                                                          contacts_details_by_id_map)
    
    filename = f'{new_report_id}-Transfer_Out_{start_date_string}-{end_date_string}.xlsx'
    path_to_user_reports = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
    if not str(user_id) in os.listdir(path_to_user_reports):
        dir_to_be_made = f'{path_to_user_reports}/{user_id}'
        os.mkdir(dir_to_be_made)
    file_full_path = f'{path_to_user_reports}/{user_id}/{filename}'
    with Workbook(file_full_path) as wb:
        total_tx_outs = writing_first_worksheet(wb)
        ws_two = wb.add_worksheet('Trx-Out Report With Details')
        #generate second sheet
        first_part, third_part = generate_second_worksheet(contacts_details_by_id_map, total_tx_outs)
        list_to_prep_ws_two = []

        ws_two.write_row(0,0,list(list(first_part.values())[0].keys()) + ['Tally'])
        for example_dict in first_part.values():
            row_data = []
            for data in example_dict.values():
                row_data.append(data)
            list_to_prep_ws_two.append(row_data)
        list_to_prep_ws_two.sort(key=lambda x:x[0])
        
        for row_num, row in enumerate(list_to_prep_ws_two):
            if row_num == 0:
                row.append(row[2])
                continue
            row.append(list_to_prep_ws_two[row_num - 1][3] + row[2])
        ws_two_counter = 1
        for row in list_to_prep_ws_two:
            row[2] = float(str(row[2])[:5])
            row[3] = float(str(row[3])[:5])
            ws_two.write_row(ws_two_counter, 0, row)
            ws_two_counter += 1
        tx_out_by_age_summary = {
            '0-30':{
                'total':0,
                'percentage':''
            },
            '31-60':{
                'total':0,
                'percentage':''
            },
            '61-90':{
                'total':0,
                'percentage':''
            },
            '91-120':{
                'total':0,
                'percentage':''
            },
            '121-360':{
                'total':0,
                'percentage':''
            },
            '>360':{
                'total':0,
                'percentage':''
            }
        }
        for example_dict in first_part.values():
            if example_dict['Days In Effect'] <= 30:
                tx_out_by_age_summary['0-30']['total'] += example_dict['Total Transfer Outs']
            elif 30 < example_dict['Days In Effect'] <= 60:
                tx_out_by_age_summary['31-60']['total'] += example_dict['Total Transfer Outs']
            elif 60 < example_dict['Days In Effect'] <= 90:
                tx_out_by_age_summary['61-90']['total'] += example_dict['Total Transfer Outs']
            elif 90 < example_dict['Days In Effect'] <= 120:
                tx_out_by_age_summary['91-120']['total'] += example_dict['Total Transfer Outs']
            elif 120 < example_dict['Days In Effect'] <= 360:
                tx_out_by_age_summary['121-360']['total'] += example_dict['Total Transfer Outs']
            elif 360 < example_dict['Days In Effect'] <= 600:
                tx_out_by_age_summary['>360']['total'] += example_dict['Total Transfer Outs']
        for part_two_value in tx_out_by_age_summary.values():
            part_two_value['percentage'] = (part_two_value['total'] / total_tx_outs)*100
        ws_two_counter = 1
        ws_two.write_row(0,5,['Days in Effect', 'Total Amount', 'Percentage of Total'])
        for key, val in tx_out_by_age_summary.items():
            row_data = []
            row_data.append(key)
            for value in val.values():
                row_data.append(value)
            ws_two.write_row(ws_two_counter, 5, row_data)
            ws_two_counter += 1

        list_to_prep_third_part = []
        for ma_employee_key, ranges_dict in third_part.items():
            row_data = []
            row_data.append(ma_employee_key)
            for value in ranges_dict.values():
                row_data.append(value)
            total_for_ma = 0
            for num in row_data[1:]:
                total_for_ma += num
            row_data.append(total_for_ma)
            list_to_prep_third_part.append(row_data)
        list_to_prep_third_part.sort(reverse=True, key=lambda x:x[7])
        ws_two.write_row(0,9, ['MA:Employee', '0-30', '31-60','61-90','91-120','121-360','>360','Total'])
        ws_two_counter = 1
        for row in list_to_prep_third_part:
            ws_two.write_row(ws_two_counter, 9, row)
            ws_two_counter += 1

    time_created = datetime.now().timestamp()
    with engine.connect() as conn:
        conn.execute(insert(User_Generated_Reports_Table)
                     .values(id=new_report_id,
                             user_id=user_id,
                             report_type='Transfer Out',
                             number_of_rows=total_tx_outs,
                             path=file_full_path,
                             time_created=time_created,
                             range_start=start_date_timestamp,
                             range_end=end_date_timestamp))
        conn.commit()

@celery.task
def general_report_generator(date: datetime, user_id:int):
    from sqlalchemy.sql.expression import func
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Database_Changes = Table("Merged_Database_Changes", metadata_obj, autoload_with=engine)
    User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        max_report_id = conn.scalar(select(func.max(User_Generated_Reports_Table.c.id)))
        if not max_report_id:
            max_report_id = 0
        logging.info(max_report_id)
        new_report_id = max_report_id + 1
    
    requested_date_timestamp = time.mktime(date.timetuple())
    requested_date_string = date.strftime('%m-%d-%Y')
    end_counter = 0

    def new_contact_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Source_Database', 'Customer_Order_ID', 'Nlad_Subscriber_ID', 
                                                    'CLEC', 'Agent', 'Plan', 'Account_Status', 'ACP_Status', 
                                                    'MDN'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'New Contacts')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of new contacts to be written to report: {len(contacts)}')
        counter = 2
        ws = wb.add_worksheet('New Records')
        ws.write_row(0,0,["New Records", 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished New Record Report')
        logging.info(counter)
        return counter - 2

    def acp_status_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'ACP_Status')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of ACP Status changes to be written to report: {len(contacts)}')
        
        ws = wb.add_worksheet('ACP Status')
        ws.write_row(0,0,['ACP Status', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        counter = 2
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished ACP Status Report')
        logging.info(counter)
        return counter - 2
        
    def mdn_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'MDN')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of MDN changes to be written to report: {len(contacts)}')
        
        ws = wb.add_worksheet('MDN')
        ws.write_row(0,0,['MDN', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        counter = 2
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished MDN Report')
        logging.info(counter)
        return counter - 2

    def esn_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'ESN')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of ESN changes to be written to report: {len(contacts)}')
        
        ws = wb.add_worksheet('ESN')
        ws.write_row(0,0,['ESN', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        counter = 2
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished ESN Report')
        logging.info(counter)
        return counter - 2

    def device_id_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'IMEI')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of DEVICE_ID changes to be written to report: {len(contacts)}')
        
        ws = wb.add_worksheet('IMEI')
        ws.write_row(0,0,['IMEI', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        counter = 2
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished Device_Id Report')
        logging.info(counter)
        return counter - 2

    def device_model_number_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'Device_Model_Number')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of Device_Model_Number changes to be written to report: {len(contacts)}')
        
        ws = wb.add_worksheet('Device_Model_Number')
        ws.write_row(0,0,['Device Model Number', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        counter = 2
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished Device_Model_Number Report')
        logging.info(counter)
        return counter - 2

    def device_type_report(wb:Workbook):
        with engine.connect() as conn:
            stmt = select(Merged_Database_Changes.c['Customer_Order_ID', 'Old_Value', 'New_Value'])\
                                    .where(Merged_Database_Changes.c['Change_Date'] == requested_date_timestamp, Merged_Database_Changes.c['Field_Name'] == 'Device_Type')
            fields = stmt.selected_columns.keys()
            contacts = conn.execute(stmt).all()
        logging.info(f'Number of Device_Type changes to be written to report: {len(contacts)}')
        counter = 2
        ws = wb.add_worksheet('Device Type')
        ws.write_row(0,0,['Device Type', 'Total Number of Rows ->', len(contacts)])
        ws.write_row(1,0,fields)
        for contact in contacts:
            ws.write_row(counter, 0, contact)
            counter += 1
        logging.info('Finished Device_Type Report')
        logging.info(counter)
        return counter - 2
    
    path_to_user_reports = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
    filename = f'{new_report_id}-General Report {requested_date_string}.xlsx'
    if not str(user_id) in os.listdir(path_to_user_reports):
        dir_to_be_made = f'{path_to_user_reports}/{user_id}'
        os.mkdir(dir_to_be_made)
    file_full_path = f'{path_to_user_reports}/{user_id}/{filename}'
    with Workbook(file_full_path) as wb:
        function_list = [acp_status_report, new_contact_report, mdn_report, esn_report, device_id_report, device_model_number_report, device_type_report]
        for func in function_list:
            end_counter += func(wb)
    time_created = datetime.now().timestamp()
    with engine.connect() as conn:
        conn.execute(insert(User_Generated_Reports_Table)
                     .values(id=new_report_id,
                             user_id=user_id,
                             report_type='General Report',
                             number_of_rows=end_counter,
                             path=file_full_path,
                             time_created=time_created,
                             range_start=requested_date_timestamp,
                             range_end=requested_date_timestamp))
        conn.commit()

@celery.task
def trx_out_report_employee_history_generator(date: datetime, user_id:int):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Transfer_Outs = Table("Transfer_Outs", metadata_obj, autoload_with=engine)
    User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    from sqlalchemy.sql.expression import func

    result_dict_example = {
        'Employee:Master Agent': {
            'day':0,
            'three_days':0,
            'seven_days':0,
            'thirty_days':0
        }
    }
    result_dict = {}
    requested_date_timestamp = time.mktime(date.timetuple())
    requested_date_string = date.strftime('%m-%d-%Y')
    date_minus_three = requested_date_timestamp - (3*86400)
    date_minus_seven = requested_date_timestamp - (7*86400)
    date_minus_thirty = requested_date_timestamp - (30*86400)
    with engine.connect() as conn:
        stmt = select(Transfer_Outs.c['MASTER_AGENT', 'EMPLOYEE', 'TRANSACTION_DATE']).where(Transfer_Outs.c['TRANSACTION_DATE'] <= requested_date_timestamp, Transfer_Outs.c['TRANSACTION_DATE'] >= date_minus_thirty)
        stmt_cols_indexes = {k:v for v, k in enumerate(stmt.selected_columns.keys())}
        records = conn.execute(stmt).all()

    for record in records:
        combined_names_for_dict_key = f'{record[stmt_cols_indexes["MASTER_AGENT"]]}:{record[stmt_cols_indexes["EMPLOYEE"]]}'
        if not result_dict.get(combined_names_for_dict_key, None):
            result_dict[combined_names_for_dict_key] = {
                'day':0,
                'three_days':0,
                'seven_days':0,
                'thirty_days':0
            }
        if float(record[stmt_cols_indexes['TRANSACTION_DATE']]) == requested_date_timestamp:
            result_dict[combined_names_for_dict_key]['day'] += 1
            result_dict[combined_names_for_dict_key]['three_days'] += 1
            result_dict[combined_names_for_dict_key]['seven_days'] += 1
            result_dict[combined_names_for_dict_key]['thirty_days'] += 1
        elif float(record[stmt_cols_indexes['TRANSACTION_DATE']]) > date_minus_three:
            result_dict[combined_names_for_dict_key]['three_days'] += 1
            result_dict[combined_names_for_dict_key]['seven_days'] += 1
            result_dict[combined_names_for_dict_key]['thirty_days'] += 1
        elif float(record[stmt_cols_indexes['TRANSACTION_DATE']]) > date_minus_seven:
            result_dict[combined_names_for_dict_key]['seven_days'] += 1
            result_dict[combined_names_for_dict_key]['thirty_days'] += 1
        elif float(record[stmt_cols_indexes['TRANSACTION_DATE']]) > date_minus_thirty:
            result_dict[combined_names_for_dict_key]['thirty_days'] += 1
        
    with engine.connect() as conn:
        max_report_id = conn.scalar(select(func.max(User_Generated_Reports_Table.c.id)))
        if not max_report_id:
            max_report_id = 0
        logging.info(max_report_id)
        new_report_id = max_report_id + 1
    path_to_user_reports = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
    filename = f'{new_report_id}-TRX Out Employee History {requested_date_string}.xlsx'
    if not str(user_id) in os.listdir(path_to_user_reports):
        dir_to_be_made = f'{path_to_user_reports}/{user_id}'
        os.mkdir(dir_to_be_made)
    file_full_path = f'{path_to_user_reports}/{user_id}/{filename}'

    # Prepare data to write to wb
    list_of_data_lists = []
    for master_employee, date_data_dict in result_dict.items():
        row_data = []
        split_name_and_master = master_employee.split(':')
        row_data.append(split_name_and_master[1])
        row_data.append(split_name_and_master[0])
        for number in date_data_dict.values():
            row_data.append(number)
        list_of_data_lists.append(row_data)
    list_of_data_lists.sort(reverse=True, key=lambda x:x[2]) # Sort by day, highest to lowest
    list_of_data_lists = list_of_data_lists[:10]
    with engine.connect() as conn:
        for row in list_of_data_lists:
            with engine.connect() as conn:
                count_seven = conn.execute(select(func.count()).select_from(Merged_Customer_Databases)
                                            .where(Merged_Customer_Databases.c['MASTER_AGENT_NAME'] == row[1], 
                                            Merged_Customer_Databases.c['Agent'] == row[0], 
                                            Merged_Customer_Databases.c['Activation_Date'] > date_minus_seven, 
                                            Merged_Customer_Databases.c['Activation_Date'] <= requested_date_timestamp)).first()
                count_thirty = conn.execute(select(func.count()).select_from(Merged_Customer_Databases)
                                            .where(Merged_Customer_Databases.c['MASTER_AGENT_NAME'] == row[1], 
                                            Merged_Customer_Databases.c['Agent'] == row[0], 
                                            Merged_Customer_Databases.c['Activation_Date'] > date_minus_thirty,
                                            Merged_Customer_Databases.c['Activation_Date'] <= requested_date_timestamp)).first()
            if count_seven:
                row.append(count_seven[0])
            else:
                row.append(0)
            if count_thirty:
                row.append(count_thirty[0])
            else:
                row.append(0)

    with Workbook(file_full_path) as wb:
        ws = wb.add_worksheet()
        ws.write_row(0,0,['Employee Name', 'MASTER AGENT', 'Qty for day', 'Last 3 days', 'Last 7 days', 'Last 30 days', 'Activations for last 7 days', 'Activations for last 30 days'])
        counter = 1
        for row in list_of_data_lists:
            ws.write_row(counter, 0, row)
            counter += 1

    time_created = datetime.now().timestamp()
    with engine.connect() as conn:
        conn.execute(insert(User_Generated_Reports_Table)
                     .values(id=new_report_id,
                             user_id=user_id,
                             report_type='TRX Out Employee History',
                             number_of_rows=counter,
                             path=file_full_path,
                             time_created=time_created,
                             range_start=requested_date_timestamp,
                             range_end=requested_date_timestamp))
        conn.commit()

@celery.task
def addingsASR(file_path:str, current_user_id:int, report_type:str):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    AgentSalesReports = Table('AgentSalesReports', metadata_obj, autoload_with=engine)
    Uploaded_Report_Records = Table("Uploaded_Report_Records", metadata_obj, autoload_with=engine)

    fields_in_files = ['Enroll ID', 'CUST ID', 'STATUS', 'ORDER DATE', 'ACTIVATION DATE', 'LAST ACTION DATE', 'DEVICE ID', 'RAD ID', 'AMOUNT', 'TYPE', 'SIM', 'PRODUCT TYPE', 'ENROLLMENT TYPE', 'SOURCE', 'FULFILMENT', 'MASTER', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE']
    fields_in_db_table = ['Enrollment_ID', 'Customer_ID', 'Status', 'Order_Date', 'Activation_Date', 'Last_Action_Date', 'Device_ID', 'RAD_ID', 'Amount', 'Amount_Type', 'Sim', 'Product', 'Enrollment_Type', 'Source', 'Fulfillment', 'Master', 'Distributor', 'Retailer', 'Employee']

    with open(file_path) as csvfile, engine.connect() as conn:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        fields.remove(fields[0])
        for field in fields:
            if field not in fields_in_files:
                logging.info('columns not matching')
                os.remove(file_path)
                break
        for field in fields_in_files:
            if field not in fields:
                logging.info('columns not matching')
                os.remove(file_path)
                break
        fields_dict = {v:k for k, v in enumerate(fields_in_db_table)}
        whole_row_for_inserting = []
        for row_num, row in enumerate(csvreader):
            row.remove(row[0])
            dict_of_row = {}
            for col, index in fields_dict.items():
                try:
                    if col == 'Order_Date':
                        if row[index]:
                            dict_of_row['Order_Date'] = parse(row[index]).timestamp()
                        else:
                            dict_of_row['Order_Date'] = row[index]
                    elif col == 'Activation_Date':
                        if row[index]:
                            dict_of_row['Activation_Date'] = parse(row[index]).timestamp()
                        else:
                            dict_of_row['Activation_Date'] = row[index]
                    elif col == 'Last_Action_Date':
                        if row[index]:
                            dict_of_row['Last_Action_Date'] = parse(row[index]).timestamp()
                        else:
                            dict_of_row['Last_Action_Date'] = row[index]
                    else:
                        dict_of_row[col] = row[index]
                except:
                    dict_of_row[col] = ''
            whole_row_for_inserting.append(dict_of_row)
        logging.info(f'Num of contacts for insert: {len(whole_row_for_inserting)}')
        conn.execute(insert(AgentSalesReports), whole_row_for_inserting)
        conn.execute(insert(Uploaded_Report_Records)
                        .values(
                            user_id=current_user_id,
                            report_type=report_type,
                            number_of_new_records_added=len(whole_row_for_inserting),
                            date_uploaded=time.mktime(datetime.today().date().timetuple())
                        ))
        conn.commit()
    os.remove(file_path)

@celery.task
def adding_Transfer_Outs(file_path:str, current_user_id:int, report_type:str):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    TransferOuts = Table('Transfer_Outs', metadata_obj, autoload_with=engine)
    Uploaded_Report_Records = Table("Uploaded_Report_Records", metadata_obj, autoload_with=engine)

    fields_in_files = ['TRANSACTION DATE', 'TRANSACTION TYPE', 'TRANSACTION EFFECTIVE DATE', 'ENROLL ID/CUST ID', 'ENROLL ID', 'CUST ID', 'SAC', 'ACCOUNT TYPE', 'MDN', 'ALTERNATE PHONE NUMBER', ' NAME', '  ADDRESS', 'STATE', 'SERVICE INITIATION DATE', 'PROGRAM', ' DE-ENROLL STATUS', 'DE-ENROLL DATETIME', 'ETC GENERAL USE', 'EMAIL', 'IS MISMATCH', 'CURRENT STATUS', 'RE-CONNECTION DATE', 'WINBACK (YES/NO)', 'ACTIVATION DATE', 'MASTER AGENT', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE ']
    fields_in_db_table = ['TRANSACTION_DATE', 'TRANSACTION_TYPE', 'TRANSACTION_EFFECTIVE_DATE', 'ENROLL_ID_CUST_ID', 'ENROLL_ID', 'CUST_ID', 'SAC', 'ACCOUNT_TYPE', 'MDN', 'ALTERNATE_PHONE_NUMBER', 'NAME', 'ADDRESS', 'STATE', 'SERVICE_INITIATION_DATE', 'PROGRAM', 'DE_ENROLL_STATUS', 'DE_ENROLL_DATETIME', 'ETC_GEENRAL_USE', 'EMAIL', 'IS_MISMATCH', 'CURRENT_STATUS', 'RE_CONNECTION_DATE', 'WINBACK', 'ACTIVATION_DATE', 'MASTER_AGENT', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE']

    with open(file_path) as csvfile, engine.connect() as conn:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        fields.remove(fields[0])
        for field in fields:
            if field not in fields_in_files:
                logging.info(field)
                logging.info('columns not matching')
                break
        for field in fields_in_files:
            if field not in fields:
                logging.info('columns not matching')
                break
        fields_dict = {v:k for k, v in enumerate(fields_in_db_table)}
        whole_row_for_inserting = []
        for row_num, row in enumerate(csvreader):
            row.remove(row[0])
            dict_of_row = {}
            for col, index in fields_dict.items():
                try:
                    if col in ['TRANSACTION_DATE', 'TRANSACTION_EFFECTIVE_DATE', 'SERVICE_INITIATION_DATE', 'DE_ENROLL_DATETIME', 'ACTIVATION_DATE']:
                        if row[index]:
                            dict_of_row[col] = datetime.strptime(parse(row[index]).strftime('%m-%d-%Y'), '%m-%d-%Y').timestamp()
                        else:
                            dict_of_row[col] = row[index]
                    else:
                        dict_of_row[col] = row[index]
                except:
                    dict_of_row[col] = ''
            whole_row_for_inserting.append(dict_of_row)
        logging.info(f'Num of contacts for insert: {len(whole_row_for_inserting)}')
        conn.execute(insert(TransferOuts), whole_row_for_inserting)
        conn.execute(insert(Uploaded_Report_Records)
                        .values(
                            user_id=current_user_id,
                            report_type=report_type,
                            number_of_new_records_added=len(whole_row_for_inserting),
                            date_uploaded=time.mktime(datetime.today().date().timetuple())
                        ))
        conn.commit()
    os.remove(file_path)

@celery.task
def dropship_report(file_path:str, user_id:int, filename:str):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    deviceQty = Table('Device_Qty', metadata_obj, autoload_with=engine)
    User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        max_report_id = conn.scalar(select(func.max(User_Generated_Reports_Table.c.id)))
        if not max_report_id:
            max_report_id = 0
        new_report_id = max_report_id + 1
        
    path_to_user_reports = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
    if not str(user_id) in os.listdir(path_to_user_reports):
        dir_to_be_made = f'{path_to_user_reports}/{user_id}'
        os.mkdir(dir_to_be_made)
    
    mcd_sub_id_index = list(Merged_Customer_Databases.c.keys()).index('NLAD_Subscriber_ID')
    device_qty_fields = [x.replace('_', ' ') for x in deviceQty.c.keys()]
    deviceQtyFieldIndexes = {v:k for k, v in enumerate(deviceQty.c.keys())}
    
    filename = filename[:-3] + 'xlsx'
    writing_wb_name = f'/home/ubuntu/MaxsipReports/app/main/static/User_Reports/{user_id}/{new_report_id}-updated-{filename}'
    end_counter = 0
    with engine.connect() as conn, open(file_path) as csvfile, Workbook(writing_wb_name) as writing_wb:
        writing_ws = writing_wb.add_worksheet()
        csvreader = csv.reader(csvfile)
        for row_num, row in enumerate(csvreader):
            if row_num % 100 == 0:
                print(row_num)
            if row_num == 0:
                field_indexes = {k:i for i, k in enumerate(row)}
                enroll_index = field_indexes['Enrollment Id']
                both_field_lists = row + [''] + device_qty_fields
                writing_ws.write_row(0,0,both_field_lists)
                continue
            contact_from_mcd = conn.execute(select(Merged_Customer_Databases).where(Merged_Customer_Databases.c['Customer_Order_ID'] == row[enroll_index])).first()
            if contact_from_mcd:
                contact_sub_id = contact_from_mcd[mcd_sub_id_index]
                contact_from_dq = conn.execute(select(deviceQty).where(deviceQty.c['NLAD_Subscriber_ID'] == contact_sub_id)).first()
                if contact_from_dq:
                    contact_from_dq = list(contact_from_dq)
                    if contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']]:
                        contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']] = datetime.fromtimestamp(contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']]).strftime('%Y-%m-%d %H:%M:%S')
                    if contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']]: 
                        contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']] = datetime.fromtimestamp(contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']]).strftime('%Y-%m-%d %H:%M:%S')
                    row = row + [''] + contact_from_dq
                else:
                    row = row + [''] + ['This contact does not have an existing device']
                writing_ws.write_row(row_num, 0, row)
            else:
                row = row + [''] + ['The enrollment Id for this contact was not found in our databases']
                writing_ws.write_row(row_num, 0, row)
            end_counter += 1
    os.remove(file_path)
    time_created = datetime.now().timestamp()

    with engine.connect() as conn:
        conn.execute(insert(User_Generated_Reports_Table)
                     .values(id=new_report_id,
                             user_id=user_id,
                             report_type='Updated Dropship Report',
                             number_of_rows=end_counter,
                             path=writing_wb_name,
                             time_created=time_created))
        conn.commit()
            

