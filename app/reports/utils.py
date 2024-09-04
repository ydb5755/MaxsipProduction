from sqlalchemy import MetaData, create_engine, Table, select, insert
from sqlalchemy.sql.expression import func
import csv
from datetime import datetime
import os
import time
from xlsxwriter import Workbook
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.INFO)


def check_columns_front_end_ntor_report(file_path):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    fields_in_files = ['TRANSACTION DATE', 'TRANSACTION TYPE', 'TRANSACTION EFFECTIVE DATE', 'ENROLL ID/CUST ID', 'ENROLL ID', 'CUST ID', 'SAC', 'ACCOUNT TYPE', 'MDN', 'ALTERNATE PHONE NUMBER', ' NAME', '  ADDRESS', 'STATE', 'SERVICE INITIATION DATE', 'PROGRAM', ' DE-ENROLL STATUS', 'DE-ENROLL DATETIME', 'ETC GENERAL USE', 'EMAIL', 'IS MISMATCH', 'CURRENT STATUS', 'RE-CONNECTION DATE', 'WINBACK (YES/NO)', 'ACTIVATION DATE', 'MASTER AGENT', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE ']
    
    with open(file_path, encoding='utf-8') as csvfile:
        try:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields.remove(fields[0]) # Remove hashtag field
            for column in fields_in_files:
                if column not in fields:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            for column in fields:
                if column not in fields_in_files:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            return True
        except:
            return False
        
def check_columns_front_end_asr_report(file_path):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    fields_in_files = ['Enroll ID', 'CUST ID', 'STATUS', 'ORDER DATE', 'ACTIVATION DATE', 'LAST ACTION DATE', 'DEVICE ID', 'RAD ID', 'AMOUNT', 'TYPE', 'SIM', 'PRODUCT TYPE', 'ENROLLMENT TYPE', 'SOURCE', 'FULFILMENT', 'MASTER', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE']
    
    with open(file_path, encoding='utf-8') as csvfile:
        try:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields.remove(fields[0]) # Remove hashtag field
            for column in fields_in_files:
                if column not in fields:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            for column in fields:
                if column not in fields_in_files:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            return True
        except:
            return False

def check_columns_front_end_dsr_report(file_path):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    fields_in_files = ['Action','Enrollment Id','First Name','Last Name','Contact Number',' Email','Mailing Add1','Mailing Add2','Mailing City','State','Mailing Zip','Plan Name','Queue Name','Agent Name','Approve Date Time','Enrollment Type','Referral Code ','Requested Model','Tracking Number','Shipment Status','Shipment Date','Provisioning Error','Master Agent','Distributor','Retailor','Employee ','Payment Completed','Payment Amount','Source ']
    with open(file_path, encoding='utf-8') as csvfile:
        try:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields.remove(fields[0]) # Remove hashtag field
            for column in fields_in_files:
                if column not in fields:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            for column in fields:
                if column not in fields_in_files:
                    logging.info(column)
                    logging.info(fields_in_files)
                    logging.info(fields)
                    return False
            return True
        except:
            return False

def activation_groups_with_agents(start_date: datetime, end_date: datetime, user_id:int):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    User_Generated_Reports_Table = Table("User_Generated_Reports", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    start_date_string = start_date.strftime('%m-%d-%Y')
    end_date_string = end_date.strftime('%m-%d-%Y')
    timestamp_start_date = time.mktime(start_date.timetuple())
    timestamp_end_date = time.mktime(end_date.timetuple())

    with engine.connect() as conn:
        stmt = select(Merged_Customer_Databases.c['Source_Database', 'MASTER_AGENT_NAME', 'Agent', 'Group_ID']).where(
            Merged_Customer_Databases.c['Activation_Date'] >= timestamp_start_date,
            Merged_Customer_Databases.c['Activation_Date'] <= timestamp_end_date,
        )
        contacts = conn.execute(stmt).all()
    fields_dict = {k:v for v, k in enumerate(stmt.selected_columns.keys())}

    def get_group_sizes(contacts:list) -> dict:
        result = {}
        for contact in contacts:
            if result.get(contact[fields_dict['Group_ID']], None):
                result[contact[fields_dict['Group_ID']]] += 1
            else:
                result[contact[fields_dict['Group_ID']]] = 1
        return result
    
    group_to_size = get_group_sizes(contacts)

    def get_qty_range(number:int) -> str:
        if number == 1:
            return '1'
        elif 5 >= number >= 2:
            return '2-5'
        elif 10 >= number >= 6:
            return '6-10'
        else:
            return '10+'

    def get_master_agent_quantity(contacts:list) -> list[list]:
        result = []
        master_agents_and_agents = {}
        for contact in contacts:
            row_data = [x for x in contact]
            if not row_data[fields_dict['MASTER_AGENT_NAME']]:
                row_data[fields_dict['MASTER_AGENT_NAME']] = 'N/A'
            if not row_data[fields_dict['Agent']]:
                row_data[fields_dict['Agent']] = 'N/A'
            contact_master_agent_and_agent = ','.join([row_data[fields_dict['MASTER_AGENT_NAME']], row_data[fields_dict['Agent']]])

            if master_agents_and_agents.get(contact_master_agent_and_agent, None):
                master_agents_and_agents[contact_master_agent_and_agent][row_data[fields_dict['Source_Database']]] += 1
                master_agents_and_agents[contact_master_agent_and_agent]['Total'] += 1
                master_agents_and_agents[contact_master_agent_and_agent][get_qty_range(group_to_size[row_data[fields_dict['Group_ID']]])] += 1
            else:
                master_agents_and_agents[contact_master_agent_and_agent] = {
                    'Total':0,
                    'Terracom':0,
                    'Telgoo':0,
                    'Unavo':0,
                    '1':0,
                    '2-5':0,
                    '6-10':0,
                    '10+':0
                }
                master_agents_and_agents[contact_master_agent_and_agent][contact[fields_dict['Source_Database']]] += 1
                master_agents_and_agents[contact_master_agent_and_agent]['Total'] += 1
                master_agents_and_agents[contact_master_agent_and_agent][get_qty_range(group_to_size[contact[fields_dict['Group_ID']]])] += 1
        for master_agent_agent, data_dict in master_agents_and_agents.items():
            master_agent_agent_split = master_agent_agent.split(',')
            data_list = [x for x in data_dict.values()]
            result.append(master_agent_agent_split + data_list)
        for r in result:
            if issubclass(str, type(r[2])):
                logging.info(r)
                return [r]
        result.sort(key= lambda x:x[2], reverse=True)
        return result

    master_agent_to_qty = get_master_agent_quantity(contacts)

    def get_max_id_plus_one() -> int:
        with engine.connect() as conn:
            max_report_id = conn.scalar(select(func.max(User_Generated_Reports_Table.c.id)))
            if not max_report_id:
                max_report_id = 0
            return max_report_id + 1
    
    headers = ['Master Agent Name', 'Agent', 'Number of Activations', 'Qty Terracom', 
                        'Qty Maxsip Telgoo', 'Qty Unavo', 'Group of 1', 'Group of 2-5',
                        'Group of 6-10', 'Group of 10+']
    
    def write_workbook_and_save():
        new_report_id = get_max_id_plus_one()
        filename = f'{new_report_id}-Activation_Groups_With_Agents{start_date_string}-{end_date_string}.xlsx'
        path_to_user_reports = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
        if not str(user_id) in os.listdir(path_to_user_reports):
            dir_to_be_made = f'{path_to_user_reports}/{user_id}'
            os.mkdir(dir_to_be_made)
        file_full_path = f'{path_to_user_reports}/{user_id}/{filename}'
        with Workbook(file_full_path) as wb:
            ws = wb.add_worksheet()
            ws.set_column(0,2,20)
            ws.set_column(3,10,10)
            ws.write_row(0,0, headers)
            counter = 1
            for row in master_agent_to_qty:
                ws.write_row(counter, 0, row)
                counter += 1

        time_created = datetime.now().timestamp()
        with engine.connect() as conn:
            conn.execute(insert(User_Generated_Reports_Table)
                        .values(id=new_report_id,
                                user_id=user_id,
                                report_type='Activation_Groups_With_Agents',
                                number_of_rows=len(master_agent_to_qty),
                                path=file_full_path,
                                time_created=time_created,
                                range_start=timestamp_start_date,
                                range_end=timestamp_end_date))
            conn.commit()

    write_workbook_and_save()

    return master_agent_to_qty[:20], headers
