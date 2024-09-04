from sqlalchemy import create_engine, MetaData, Table, select
import csv
from xlsxwriter import Workbook
from datetime import datetime
import os

def dropship_report():
    dir_path = '/home/ubuntu/MaxsipReports/pythonFiles/dropShipReports/Reports'
    # csv_file = 'DSR-49 03-05-24 722AM.csv'
    cdd_sub_id_index = list(customerInfoTelgoo.c.keys()).index('NLAD_SUBSCRIBER_ID')
    device_qty_fields = [x.replace('_', ' ') for x in deviceQty.c.keys()]
    deviceQtyFieldIndexes = {v:k for k, v in enumerate(deviceQty.c.keys())}

    for file in os.listdir(dir_path):
        csvfilepath = dir_path + '/' + file
        writing_wb_name = dir_path + '/updated-' + file[:-3] + 'xlsx'
        writing_wb = Workbook(writing_wb_name)
        writing_ws = writing_wb.add_worksheet()
        with engine.connect() as conn:
            with open(csvfilepath) as csvfile:
                csvreader = csv.reader(csvfile)
                for row_num, row in enumerate(csvreader):
                    if row_num % 100 == 0:
                        print(row_num)
                    if row_num == 0:
                        field_indexes = {k:i for i, k in enumerate(row)}
                        enroll_index = field_indexes['Enrollment Id']
                        model_request_index = field_indexes['Requested Model']
                        both_field_lists = row + [''] + device_qty_fields
                        writing_ws.write_row(0,0,both_field_lists)
                        continue
                    contact_from_telgoo = conn.execute(select(customerInfoTelgoo).where(customerInfoTelgoo.c['ENROLLMENT_ID'] == row[enroll_index])).first()
                    if contact_from_telgoo:
                        contact_sub_id = contact_from_telgoo[cdd_sub_id_index]
                        contact_from_dq = conn.execute(select(deviceQty).where(deviceQty.c['NLAD_Subscriber_ID'] == contact_sub_id)).first()
                        if contact_from_dq:
                            contact_from_dq = list(contact_from_dq)
                            if contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']]:
                                contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']] = datetime.fromtimestamp(contact_from_dq[deviceQtyFieldIndexes['Date_Of_First_Device']]).strftime('%Y-%m-%d %H:%M:%S:%f')[:-7]
                            if contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']]: 
                                contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']] = datetime.fromtimestamp(contact_from_dq[deviceQtyFieldIndexes['Date_Of_Last_Device']]).strftime('%Y-%m-%d %H:%M:%S:%f')[:-7]
                            row = row + [''] + contact_from_dq
                        else:
                            row = row + [''] + ['This contact does not have an existing device']
                        writing_ws.write_row(row_num + 1, 0, row)
                    else:
                        row = row + [''] + ['The enrollment Id for this contact was not found in our databases']
                        writing_ws.write_row(row_num + 1, 0, row)
            writing_wb.close()
        os.remove(csvfilepath)
            

if __name__ == '__main__':
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'
    path_to_xlsx_reports = '/home/ubuntu/MaxsipReports/pythonFiles/telgooReports/Adding_New'

    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    unavoChanges = Table("Unavo_Changes", metadata_obj, autoload_with=engine)
    telgooChanges = Table("Telgoo_Changes", metadata_obj, autoload_with=engine)
    customerInfoTelgoo = Table("customerInfoTelgoo", metadata_obj, autoload_with=engine)
    customerInfo = Table("customerInfo", metadata_obj, autoload_with=engine)
    customerDeviceDatabase = Table('Customer_Device_Database', metadata_obj, autoload_with=engine)
    deviceQty = Table('Device_Qty', metadata_obj, autoload_with=engine)

    dropship_report()