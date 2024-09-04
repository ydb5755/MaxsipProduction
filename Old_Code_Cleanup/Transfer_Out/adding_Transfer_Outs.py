import os
from sqlalchemy import create_engine, MetaData, Table, insert, select
import csv
from dateutil.parser import parse
from datetime import datetime


def adding_Transfer_Outs():
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'

    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    TransferOuts = Table('Transfer_Outs', metadata_obj, autoload_with=engine)

    fields_in_files = ['TRANSACTION DATE', 'TRANSACTION TYPE', 'TRANSACTION EFFECTIVE DATE', 'ENROLL ID/CUST ID', 'ENROLL ID', 'CUST ID', 'SAC', 'ACCOUNT TYPE', 'MDN', 'ALTERNATE PHONE NUMBER', ' NAME', '  ADDRESS', 'STATE', 'SERVICE INITIATION DATE', 'PROGRAM', ' DE-ENROLL STATUS', 'DE-ENROLL DATETIME', 'ETC GENERAL USE', 'EMAIL', 'IS MISMATCH', 'CURRENT STATUS', 'RE-CONNECTION DATE', 'WINBACK (YES/NO)', 'ACTIVATION DATE', 'MASTER AGENT', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE ']
    fields_in_db_table = ['TRANSACTION_DATE', 'TRANSACTION_TYPE', 'TRANSACTION_EFFECTIVE_DATE', 'ENROLL_ID_CUST_ID', 'ENROLL_ID', 'CUST_ID', 'SAC', 'ACCOUNT_TYPE', 'MDN', 'ALTERNATE_PHONE_NUMBER', 'NAME', 'ADDRESS', 'STATE', 'SERVICE_INITIATION_DATE', 'PROGRAM', 'DE_ENROLL_STATUS', 'DE_ENROLL_DATETIME', 'ETC_GEENRAL_USE', 'EMAIL', 'IS_MISMATCH', 'CURRENT_STATUS', 'RE_CONNECTION_DATE', 'WINBACK', 'ACTIVATION_DATE', 'MASTER_AGENT', 'DISTRIBUTOR', 'RETAILER', 'EMPLOYEE']
    path_to_folder = '/home/ubuntu/MaxsipReports/pythonFiles/Transfer_Out/Reports'
    for file in os.listdir(path_to_folder):
        print(f'Starting file: {file}')
        path_to_file = path_to_folder + '/' + file
        with open(path_to_file) as csvfile, engine.connect() as conn:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields.remove(fields[0])
            for field in fields:
                if field not in fields_in_files:
                    print(field)
                    print('columns not matching')
                    break
            for field in fields_in_files:
                if field not in fields:
                    print('columns not matching')
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
            print(f'Num of contacts for insert: {len(whole_row_for_inserting)}')
            conn.execute(insert(TransferOuts), whole_row_for_inserting)
            conn.commit()
        os.remove(path_to_file)


if __name__ == '__main__':
    adding_Transfer_Outs()