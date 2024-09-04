import os
from sqlalchemy import create_engine, MetaData, Table, insert
import csv
from dateutil.parser import parse
from datetime import datetime, timedelta



def adding_Reimbursements():
    fields_in_files = []
    fields_in_db_table = []
    path_to_folder = '/home/ubuntu/MaxsipReports/pythonFiles/Reimbursements/Reports'
    for file in os.listdir(path_to_folder):
        print(f'Starting file: {file}')
        path_to_file = path_to_folder + '/' + file
        with open(path_to_file) as csvfile, engine.connect() as conn:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            fields.remove(fields[0])
            for field in fields:
                if field not in fields_in_files:
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
            print(f'Num of contacts for insert: {len(whole_row_for_inserting)}')
            conn.execute(insert(Reimbursements), whole_row_for_inserting)
            conn.commit()



if __name__ == '__main__':
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'

    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Reimbursements = Table('Reimbursements', metadata_obj, autoload_with=engine)


    
    adding_Reimbursements()