from sqlalchemy import create_engine, MetaData, Table, select, update, bindparam, insert
import time
from datetime import datetime, timedelta

def dqs_update_script():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    deviceQty = Table('Device_Qty', metadata_obj, autoload_with=engine)
    Device_Qty_Changes = Table('Device_Qty_Changes', metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table('Merged_Customer_Databases', metadata_obj, autoload_with=engine)
    yesterday = time.mktime((datetime.today() - timedelta(days=1)).date().timetuple())
    
    def db_update(list_of_row_dicts):
        with engine.connect() as conn:
            conn.execute(update(deviceQty)
                        .where(deviceQty.c['NLAD_Subscriber_ID'] == bindparam('b_NLAD_Subscriber_ID'))
                        .values(Database = bindparam('Database'),
                                Duplicate = bindparam('Duplicate'),
                                Total_Customer_Order_IDs = bindparam('Total_Customer_Order_IDs'),
                                Number_Of_Devices = bindparam('Number_Of_Devices'),
                                Number_Of_Tablets = bindparam('Number_Of_Tablets'),
                                Number_Of_Phones = bindparam('Number_Of_Phones'),
                                Number_Of_Sims = bindparam('Number_Of_Sims'),
                                Number_Of_MDNs = bindparam('Number_Of_MDNs'),
                                Number_Of_Hotspots = bindparam('Number_Of_Hotspots'),
                                Date_Of_First_Device = bindparam('Date_Of_First_Device'),
                                Date_Of_Last_Device = bindparam('Date_Of_Last_Device'))
                        , list_of_row_dicts)
            conn.commit()
    
    with engine.connect() as conn:
        print('starting')
        then = datetime.now()
        old_contacts = conn.execute(select(deviceQty)).all()
        now = datetime.now()
        print(f'Time taken to perform select: {now-then}')

        dq_columns = list(deviceQty.c.keys())
        sub_id = dq_columns.index('NLAD_Subscriber_ID')
        dict_of_old_contacts = {}
        for c in old_contacts:
            dict_of_old_contacts[c[sub_id]] = c
        
        columns = ['Database', 'NLAD_Subscriber_ID', 'Activation_Date', 'ESN', 'MDN', 'Device_Type']
        column_indexes = {k:v for v,k in enumerate(columns)}
        then = datetime.now()
        contacts = conn.execute(select(Merged_Customer_Databases.c['Source_Database', 'NLAD_Subscriber_ID', 'Activation_Date', 'ESN', 'MDN', 'Device_Type'])
                                .where(
                                    Merged_Customer_Databases.c['NLAD_Subscriber_ID'] != '',
                                    Merged_Customer_Databases.c['IMEI'] != '',
                                    Merged_Customer_Databases.c['ESN'] != '',
                                    Merged_Customer_Databases.c['MDN'] != '',
                                )
                                .order_by(Merged_Customer_Databases.c['NLAD_Subscriber_ID'])).all()
        now = datetime.now()
        length_of_list = len(contacts)
        print(f'Total Rows: {length_of_list}\nTime taken to perform select: {now-then}')
        list_of_contact_dicts_to_update = []
        list_of_contact_dicts_to_add = []
        combined_contact_row = {
            'NLAD_Subscriber_ID':None,
            'Database':None,
            'Duplicate':None,
            'Total_Customer_Order_IDs':0,
            'Number_Of_Devices':0,
            'Number_Of_Tablets':0,
            'Number_Of_Phones':0,
            'Number_Of_Sims':0,
            'Number_Of_MDNs':0,
            'Number_Of_Hotspots':0,
            'Date_Of_First_Device':None,
            'Date_Of_Last_Device':None
        }
        list_of_field_names = list(combined_contact_row.keys())
        dqs_changes = []
        for contact_num, contact in enumerate(contacts):
            if contact_num % 100000 == 0:
                print(contact_num)
            sub_num = contact[column_indexes['NLAD_Subscriber_ID']]
            if contact_num == 0:
                combined_contact_row['NLAD_Subscriber_ID'] = sub_num
                combined_contact_row['Database'] = contact[column_indexes['Database']]
                combined_contact_row['Total_Customer_Order_IDs'] += 1
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] += 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] += 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] += 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                combined_contact_row['Duplicate'] = 'No'
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] += 1
                if contact[column_indexes['MDN']]:
                    combined_contact_row['Number_Of_MDNs'] += 1
                combined_contact_row['Date_Of_First_Device'] = contact[column_indexes['Activation_Date']]
                combined_contact_row['Date_Of_Last_Device'] = contact[column_indexes['Activation_Date']]
            elif sub_num == contacts[contact_num - 1][column_indexes['NLAD_Subscriber_ID']]:
                if contact[column_indexes['Database']] != combined_contact_row['Database']:
                    combined_contact_row['Database'] = 'Both'
                combined_contact_row['Total_Customer_Order_IDs'] += 1
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] += 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] += 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] += 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                if combined_contact_row['Number_Of_Devices'] > 1:
                    combined_contact_row['Duplicate'] = 'Yes'
                else:
                    combined_contact_row['Duplicate'] = 'No'
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] += 1
                if contact[column_indexes['MDN']]:
                    combined_contact_row['Number_Of_MDNs'] += 1
                if contact[column_indexes['Activation_Date']] and combined_contact_row['Date_Of_First_Device']:
                    if contact[column_indexes['Activation_Date']] > combined_contact_row['Date_Of_Last_Device']:
                        combined_contact_row['Date_Of_Last_Device'] = contact[column_indexes['Activation_Date']]
                    if contact[column_indexes['Activation_Date']] < combined_contact_row['Date_Of_First_Device']:
                        combined_contact_row['Date_Of_First_Device'] = contact[column_indexes['Activation_Date']]
                elif contact[column_indexes['Activation_Date']]:
                    combined_contact_row['Date_Of_First_Device'] = contact[column_indexes['Activation_Date']]
                    combined_contact_row['Date_Of_Last_Device'] = contact[column_indexes['Activation_Date']]
            else:
                if combined_contact_row['NLAD_Subscriber_ID'] in dict_of_old_contacts:
                    new_contact_list = list(combined_contact_row.values())
                    old_contact_list = dict_of_old_contacts[combined_contact_row['NLAD_Subscriber_ID']]
                    for field_num, field in enumerate(new_contact_list):
                        old_cell = old_contact_list[field_num]
                        new_cell = field
                        if list_of_field_names[field_num] == 'Total_Customer_Order_IDs':
                            old_cell = int(old_cell)
                            new_cell = int(new_cell)
                        if old_cell != new_cell:
                            dqs_changes.append({
                                'NLAD_Subscriber_ID': combined_contact_row['NLAD_Subscriber_ID'],
                                'Field_Name': list_of_field_names[field_num],
                                'New_Value':new_cell,
                                'Old_Value':old_cell,
                                'Change_Date':yesterday
                            })
                    combined_contact_row['b_NLAD_Subscriber_ID'] = combined_contact_row['NLAD_Subscriber_ID']
                    list_of_contact_dicts_to_update.append(combined_contact_row)
                else:
                    list_of_contact_dicts_to_add.append(combined_contact_row)

                combined_contact_row = {
                    'NLAD_Subscriber_ID':sub_num,
                    'Database':contact[column_indexes['Database']],
                    'Duplicate':'No',
                    'Total_Customer_Order_IDs':1,
                    'Number_Of_Devices':0,
                    'Number_Of_Tablets':0,
                    'Number_Of_Phones':0,
                    'Number_Of_Sims':0,
                    'Number_Of_MDNs':0,
                    'Number_Of_Hotspots':0,
                    'Date_Of_First_Device':None,
                    'Date_Of_Last_Device':None
                }
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] += 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] += 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] += 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] += 1
                if contact[column_indexes['MDN']]:
                    combined_contact_row['Number_Of_MDNs'] += 1
                combined_contact_row['Date_Of_First_Device'] = contact[column_indexes['Activation_Date']]
                combined_contact_row['Date_Of_Last_Device'] = contact[column_indexes['Activation_Date']]
            
            if len(list_of_contact_dicts_to_add) == 500:
                conn.execute(insert(deviceQty), list_of_contact_dicts_to_add)
                conn.commit()
                list_of_contact_dicts_to_add = []
            if len(list_of_contact_dicts_to_update) == 500:
                db_update(list_of_contact_dicts_to_update)
                list_of_contact_dicts_to_update = []
            if len(dqs_changes) >= 500:
                conn.execute(insert(Device_Qty_Changes), dqs_changes)
                conn.commit()
                dqs_changes = []
                
        if combined_contact_row['NLAD_Subscriber_ID'] in dict_of_old_contacts:
            new_contact_list = list(combined_contact_row.values())
            old_contact_list = dict_of_old_contacts[combined_contact_row['NLAD_Subscriber_ID']]
            if old_contact_list != new_contact_list:
                for field_num, field in enumerate(new_contact_list):
                    old_cell = old_contact_list[field_num]
                    new_cell = field
                    if list_of_field_names[field_num] == 'Total_Customer_Order_IDs':
                        old_cell = int(old_cell)
                        new_cell = int(new_cell)
                    if old_cell != new_cell:
                        dqs_changes.append({
                            'NLAD_Subscriber_ID': combined_contact_row['NLAD_Subscriber_ID'],
                            'New_Value':new_cell,
                            'Old_Value':old_cell,
                            'Change_Date':yesterday
                        })
                combined_contact_row['b_NLAD_Subscriber_ID'] = combined_contact_row['NLAD_Subscriber_ID']
                list_of_contact_dicts_to_update.append(combined_contact_row)
        else:
            list_of_contact_dicts_to_add.append(combined_contact_row)
        if list_of_contact_dicts_to_add:
            conn.execute(insert(deviceQty), list_of_contact_dicts_to_add)
            conn.commit()
            list_of_contact_dicts_to_add = []
        if list_of_contact_dicts_to_update:
            db_update(list_of_contact_dicts_to_update)
            list_of_contact_dicts_to_update = []
        if dqs_changes:
            conn.execute(insert(Device_Qty_Changes), dqs_changes)
            conn.commit()
            dqs_changes = []

if __name__ == '__main__':
    dqs_update_script()