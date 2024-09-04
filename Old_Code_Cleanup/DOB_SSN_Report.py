from sqlalchemy import create_engine, MetaData, Table, select
from xlsxwriter import Workbook
from datetime import datetime


def DOB_SSN_Report():
    tab_one_example = {
            'dob':{
                'ssn':{
                    'qty':1, 
                    'sub_ids': {
                        'sub id': {
                            'tab_two_data': ['data'],
                            'customer_order_ids': {
                                'order_id': ['data']
                            }
                        }
                    }
                }
            }
        }

    with engine.connect() as conn, Workbook('xl_output.xlsx') as xl_output:
        tab_one = xl_output.add_worksheet('Tab One')
        tab_one.write_row(0,0,['DOB', 'SSN', 'Qty Sub Id\'s'])
        tab_two = xl_output.add_worksheet('Tab Two')
        tab_two.write_row(0,0,['DOB', 'SSN', 'Qty Sub Id\'s', 'Subscriber ID', 
                               'Qty Customer Order Id\'s', 'T/U/B', 'Qty Devices', 
                               'Qty Sims', 'Qty Phones', 'Qty Tablet', 'Qty Hotspots', 
                               'Earliest Created Date', 'Latest Created Date', 'Earliest Order Date', 
                               'Latest Order Date', 'Earliest Activation Date', 'Latest Activation Date'])
        tab_three = xl_output.add_worksheet('Tab Three')
        tab_three.write_row(0,0,['DOB', 'SSN', 'Subscriber ID', 'Customer Order ID', 
                                 'Name', 'SS Number', 'DOB', 'T/U', 'Payments', 'Device Reimbursement', 
                                 'Service Reimbursements', 'Created Date', 'Order Date', 'Device Type', 'Activation Date', 
                                 'IMEI Changes', 'Account Status', 'ACP Status', 'Deactivation Date', 'Address 1', 'ZIP',
                                 'Master Agent', 'Distributor', 'Retailer', 'Agent Name', 'Agent Login ID'])        
        tab_four = xl_output.add_worksheet('Tab Four')
        tab_four.write_row(0,0,['NLAD Subscriber ID', 'Qty of Enrollment ID\'s', 'Qty of variations in DOB-SSN'])
        counter_tab_one = 1
        counter_tab_two = 1
        counter_tab_three = 1
        counter_tab_four = 1

        tab_four_holding_dict = {}

        contacts = conn.execute(select(main_customer_view.c['DOB', 'SSN', 'NLAD_Subscriber_ID'])
                                .where(main_customer_view.c['NLAD_Subscriber_ID'] != '')
                                .order_by(main_customer_view.c['DOB'])).all()
        holding_dict = {}
        for contact_num, contact in enumerate(contacts):
            if contact_num % 200000 == 0:
                print(contact_num)
            if holding_dict.get(contact[0]):
                if holding_dict[contact[0]].get(contact[1]):
                    if contact[2] not in holding_dict[contact[0]][contact[1]]['sub_ids']:
                        holding_dict[contact[0]][contact[1]]['qty'] += 1
                        holding_dict[contact[0]][contact[1]]['sub_ids'][str(contact[2])] = {
                            'tab_two_data': [],
                            'customer_order_ids':{}
                        }
                else:
                    holding_dict[contact[0]][contact[1]] = {
                        'qty':1,
                        'sub_ids':{
                            str(contact[2]):{
                                'tab_two_data': [],
                                'customer_order_ids':{}
                            }
                        }
                    }
            else:
                holding_dict[contact[0]] = {
                    contact[1]: {
                        'qty': 1, 
                        'sub_ids': {
                            str(contact[2]): {
                                'tab_two_data': [],
                                'customer_order_ids': {}
                            }
                        }
                    }
                } 
        print(f'Number of unique DOB\'s: {len(holding_dict.keys())}')

        list_of_sub_ids = []
        for dob, dob_ssn_dict in holding_dict.items():
            for ssn, ssn_qty_and_sub_ids_dict in dob_ssn_dict.items():
                if ssn and ssn_qty_and_sub_ids_dict['qty'] > 1:
                    formatted_dob = dob
                    if dob:
                        formatted_dob = datetime.fromtimestamp(float(dob)).strftime('%Y-%m-%d')
                    tab_one.write_row(counter_tab_one, 0, [formatted_dob, ssn, ssn_qty_and_sub_ids_dict['qty']])
                    counter_tab_one += 1
                    for sub_id in ssn_qty_and_sub_ids_dict['sub_ids'].keys():
                        list_of_sub_ids.append(sub_id)
                        # tab_two.write_row(counter_tab_two, 0, [formatted_dob, ssn, ssn_qty_and_sub_ids_dict['qty'], sub_id])
                        # counter_tab_two += 1
        print(len(list_of_sub_ids))
        contacts = conn.execute(select(main_customer_view.c['DOB', 'SSN', 'NLAD_Subscriber_ID', 'Database', 'Customer_Order_Id', 'Device_Type', 'ESN', 'Created_Date', 'Order_Date', 'Activation_Date'])
                                .where(main_customer_view.c['NLAD_Subscriber_ID']
                                       .in_(list_of_sub_ids))
                                .order_by(main_customer_view.c['NLAD_Subscriber_ID'])).all()
        print('Finished second select stmt')
        combined_contact_row = {
            'DOB':None,
            'SSN':None,
            'NLAD_Subscriber_ID':None,
            'Total_Customer_Order_IDs':0,
            'List_Of_Customer_Order_IDs':[],
            'Database':None,
            'Number_Of_Devices':0,
            'Number_Of_Sims':0,
            'Number_Of_Phones':0,
            'Number_Of_Tablets':0,
            'Number_Of_Hotspots':0,
            'Earliest_Created_Date':None,
            'Latest_Created_Date':None,
            'Earliest_Order_Date':None,
            'Latest_Order_Date':None,
            'Earliest_Activation_Date':None,
            'Latest_Activation_Date':None
        }
        columns = ['DOB', 'SSN', 'NLAD_Subscriber_ID', 'Database', 'Customer_Order_Id', 'Device_Type', 'ESN', 'Created_Date', 'Order_Date', 'Activation_Date']
        column_indexes = {k:v for v,k in enumerate(columns)}
        counter_tab_two = 1
        list_of_order_ids = []
        dict_for_sub_ids_and_corresponding_data = {}
        for contact_num, contact in enumerate(contacts):
            if contact_num % 10000 == 0:
                print(contact_num)
            list_of_order_ids.append(contact[column_indexes['Customer_Order_Id']])
            sub_num = contact[column_indexes['NLAD_Subscriber_ID']]
            if contact_num == 0:
                combined_contact_row['NLAD_Subscriber_ID'] = sub_num
                combined_contact_row['Database'] = contact[column_indexes['Database']]
                combined_contact_row['DOB'] = contact[column_indexes['DOB']]
                combined_contact_row['SSN'] = contact[column_indexes['SSN']]
                if contact[column_indexes['Customer_Order_Id']]:
                    combined_contact_row['Total_Customer_Order_IDs'] = combined_contact_row['Total_Customer_Order_IDs'] + 1
                    combined_contact_row['List_Of_Customer_Order_IDs'].append(contact[column_indexes['Customer_Order_Id']])
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] = combined_contact_row['Number_Of_Tablets'] + 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] = combined_contact_row['Number_Of_Phones'] + 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] = combined_contact_row['Number_Of_Hotspots'] + 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] = combined_contact_row['Number_Of_Sims'] + 1
                combined_contact_row['Earliest_Created_Date'] = contact[column_indexes['Created_Date']]
                combined_contact_row['Latest_Created_Date'] = contact[column_indexes['Created_Date']]
                combined_contact_row['Earliest_Order_Date'] = contact[column_indexes['Order_Date']]
                combined_contact_row['Latest_Order_Date'] = contact[column_indexes['Order_Date']]
                combined_contact_row['Earliest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                combined_contact_row['Latest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                continue
            if sub_num == contacts[contact_num - 1][column_indexes['NLAD_Subscriber_ID']]:
                if combined_contact_row['DOB'] != contact[column_indexes['DOB']] or combined_contact_row['SSN'] != contact[column_indexes['SSN']]:
                    if tab_four_holding_dict.get(contact[column_indexes['NLAD_Subscriber_ID']]):
                        tab_four_holding_dict[contact[column_indexes['NLAD_Subscriber_ID']]]['qty of enrollment ids'] += 1
                    else:
                        tab_four_holding_dict[contact[column_indexes['NLAD_Subscriber_ID']]] = {
                            'qty of enrollment ids':1,
                            'qty of variations': 0
                        }
                if combined_contact_row['DOB'] != contact[column_indexes['DOB']]:
                    tab_four_holding_dict[contact[column_indexes['NLAD_Subscriber_ID']]]['qty of variations'] += 1
                    combined_contact_row['DOB'] = contact[column_indexes['DOB']]
                if combined_contact_row['SSN'] != contact[column_indexes['SSN']]:
                    tab_four_holding_dict[contact[column_indexes['NLAD_Subscriber_ID']]]['qty of variations'] += 1
                    combined_contact_row['SSN'] = contact[column_indexes['SSN']]

                if contact[column_indexes['Database']] != combined_contact_row['Database']:
                    combined_contact_row['Database'] = 'Both'
                if contact[column_indexes['Customer_Order_Id']]:
                    combined_contact_row['Total_Customer_Order_IDs'] = combined_contact_row['Total_Customer_Order_IDs'] + 1
                    combined_contact_row['List_Of_Customer_Order_IDs'].append(contact[column_indexes['Customer_Order_Id']])
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] = combined_contact_row['Number_Of_Tablets'] + 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] = combined_contact_row['Number_Of_Phones'] + 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] = combined_contact_row['Number_Of_Hotspots'] + 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] = combined_contact_row['Number_Of_Sims'] + 1
                # Created Date
                if contact[column_indexes['Created_Date']] and combined_contact_row['Earliest_Created_Date']:
                    if contact[column_indexes['Created_Date']] > combined_contact_row['Latest_Created_Date']:
                        combined_contact_row['Latest_Created_Date'] = contact[column_indexes['Created_Date']]
                    if contact[column_indexes['Created_Date']] < combined_contact_row['Earliest_Created_Date']:
                        combined_contact_row['Earliest_Created_Date'] = contact[column_indexes['Created_Date']]
                elif contact[column_indexes['Created_Date']]:
                    combined_contact_row['Earliest_Created_Date'] = contact[column_indexes['Created_Date']]
                    combined_contact_row['Latest_Created_Date'] = contact[column_indexes['Created_Date']]
                # Order Date
                if contact[column_indexes['Order_Date']] and combined_contact_row['Earliest_Order_Date']:
                    if contact[column_indexes['Order_Date']] > combined_contact_row['Latest_Order_Date']:
                        combined_contact_row['Latest_Order_Date'] = contact[column_indexes['Order_Date']]
                    if contact[column_indexes['Order_Date']] < combined_contact_row['Earliest_Order_Date']:
                        combined_contact_row['Earliest_Order_Date'] = contact[column_indexes['Order_Date']]
                elif contact[column_indexes['Order_Date']]:
                    combined_contact_row['Earliest_Order_Date'] = contact[column_indexes['Order_Date']]
                    combined_contact_row['Latest_Order_Date'] = contact[column_indexes['Order_Date']]
                # Activation Date
                if contact[column_indexes['Activation_Date']] and combined_contact_row['Earliest_Activation_Date']:
                    if contact[column_indexes['Activation_Date']] > combined_contact_row['Latest_Activation_Date']:
                        combined_contact_row['Latest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                    if contact[column_indexes['Activation_Date']] < combined_contact_row['Earliest_Activation_Date']:
                        combined_contact_row['Earliest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                elif contact[column_indexes['Activation_Date']]:
                    combined_contact_row['Earliest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                    combined_contact_row['Latest_Activation_Date'] = contact[column_indexes['Activation_Date']]
            else:
                data_to_write = {}
                for k, v in combined_contact_row.items():
                    if k in ['DOB', 'SSN', 'NLAD_Subscriber_ID']:
                        continue
                    if k in ['Earliest_Created_Date', 'Latest_Created_Date', 'Earliest_Order_Date', 'Latest_Order_Date', 'Earliest_Activation_Date', 'Latest_Activation_Date']:
                        if v:
                            v = datetime.fromtimestamp(float(v)).strftime('%Y-%m-%d')
                    data_to_write[k] = v
                dict_for_sub_ids_and_corresponding_data[combined_contact_row['NLAD_Subscriber_ID']] = data_to_write
                
                combined_contact_row = {
                    'DOB':contact[column_indexes['DOB']],
                    'SSN':contact[column_indexes['SSN']],
                    'NLAD_Subscriber_ID':sub_num,
                    'Total_Customer_Order_IDs':1,
                    'List_Of_Customer_Order_IDs':[contact[column_indexes['Customer_Order_Id']]],
                    'Database':contact[column_indexes['Database']],
                    'Number_Of_Devices':0,
                    'Number_Of_Sims':0,
                    'Number_Of_Phones':0,
                    'Number_Of_Tablets':0,
                    'Number_Of_Hotspots':0,
                    'Earliest_Created_Date':None,
                    'Latest_Created_Date':None,
                    'Earliest_Order_Date':None,
                    'Latest_Order_Date':None,
                    'Earliest_Activation_Date':None,
                    'Latest_Activation_Date':None
                }
                if contact[column_indexes['Device_Type']] in ['Tablet', 'TABLET']:
                    combined_contact_row['Number_Of_Tablets'] = combined_contact_row['Number_Of_Tablets'] + 1
                if contact[column_indexes['Device_Type']] in ['Mobile', 'PHONE']:
                    combined_contact_row['Number_Of_Phones'] = combined_contact_row['Number_Of_Phones'] + 1
                if contact[column_indexes['Device_Type']] in ['Hot Spot', 'HOTSPOT']:
                    combined_contact_row['Number_Of_Hotspots'] = combined_contact_row['Number_Of_Hotspots'] + 1
                combined_contact_row['Number_Of_Devices'] = combined_contact_row['Number_Of_Hotspots'] + combined_contact_row['Number_Of_Phones'] + combined_contact_row['Number_Of_Tablets']
                if contact[column_indexes['ESN']]:
                    combined_contact_row['Number_Of_Sims'] = combined_contact_row['Number_Of_Sims'] + 1
                combined_contact_row['Earliest_Created_Date'] = contact[column_indexes['Created_Date']]
                combined_contact_row['Latest_Created_Date'] = contact[column_indexes['Created_Date']]
                combined_contact_row['Earliest_Order_Date'] = contact[column_indexes['Order_Date']]
                combined_contact_row['Latest_Order_Date'] = contact[column_indexes['Order_Date']]
                combined_contact_row['Earliest_Activation_Date'] = contact[column_indexes['Activation_Date']]
                combined_contact_row['Latest_Activation_Date'] = contact[column_indexes['Activation_Date']]
        for k, v in tab_four_holding_dict.items():
            tab_four.write_row(counter_tab_four, 0, [k] + list(v.values()))
            counter_tab_four += 1
        data_to_write = {}
        for k, v in combined_contact_row.items():
            if k in ['DOB', 'SSN', 'NLAD_Subscriber_ID']:
                continue
            if k in ['Earliest_Created_Date', 'Latest_Created_Date', 'Earliest_Order_Date', 'Latest_Order_Date', 'Earliest_Activation_Date', 'Latest_Activation_Date']:
                if v:
                    v = datetime.fromtimestamp(float(v)).strftime('%Y-%m-%d')
            data_to_write[k] = v
            
        dict_for_sub_ids_and_corresponding_data[combined_contact_row['NLAD_Subscriber_ID']] = data_to_write
        
        for dob, dob_ssn_dict in holding_dict.items():
            for ssn, ssn_qty_and_sub_ids_dict in dob_ssn_dict.items():
                if ssn and ssn_qty_and_sub_ids_dict['qty'] > 1:
                    formatted_dob = dob
                    if dob:
                        formatted_dob = datetime.fromtimestamp(float(dob)).strftime('%Y-%m-%d')
                    for sub_id in ssn_qty_and_sub_ids_dict['sub_ids'].keys():
                        data_list = dict_for_sub_ids_and_corresponding_data[sub_id]
                        relevant_list = []
                        for k, v in data_list.items():
                            if k == 'List_Of_Customer_Order_IDs':
                                continue
                            relevant_list.append(v)
                        tab_two.write_row(counter_tab_two, 0, [formatted_dob, ssn, ssn_qty_and_sub_ids_dict['qty'], sub_id] + relevant_list)
                        counter_tab_two += 1
                        if data_list['List_Of_Customer_Order_IDs']:
                            for coi in data_list['List_Of_Customer_Order_IDs']:
                                ssn_qty_and_sub_ids_dict['sub_ids'][sub_id]['customer_order_ids'][coi] = []
        

        columns = ['First_Name', 'Last_Name', 'SSN', 'DOB', 'NLAD_Subscriber_ID', 'Database', 'Customer_Order_Id', 
                    'Created_Date', 'Order_Date', 'Device_Type', 'Activation_Date', 
                    'Account_Status', 'ACP_Status', 'Deactivation_Date', 
                    'Address_One', 'ZIP', 'MASTER_AGENT_NAME', 'DISTRIBUTOR_AGENT_NAME',
                    'RETAILER_AGENT_NAME', 'Agent', 'Agent_LoginID']
        contacts = conn.execute(select(main_customer_view.c['First_Name', 'Last_Name', 'SSN', 'DOB', 'NLAD_Subscriber_ID', 'Database', 'Customer_Order_Id', 
                                                            'Created_Date', 'Order_Date', 'Device_Type', 'Activation_Date', 
                                                            'Account_Status', 'ACP_Status', 'Deactivation_Date', 
                                                            'Address_One', 'ZIP', 'MASTER_AGENT_NAME', 'DISTRIBUTOR_AGENT_NAME',
                                                            'RETAILER_AGENT_NAME', 'Agent', 'Agent_LoginID'])
                        .where(main_customer_view.c['Customer_Order_Id']
                                .in_(list_of_order_ids))
                        .order_by(main_customer_view.c['NLAD_Subscriber_ID'])).all()
        print(f'Length of contacts list by order id: {len(contacts)}')
        
        # Get IMEI Data
        imei_changes_telgoo = conn.execute(select(telgooChanges.c['ENROLLMENT_ID']).where(telgooChanges.c['Field_Name'] == 'DEVICE_ID', telgooChanges.c['ENROLLMENT_ID'].in_(list_of_order_ids))).all()
        print(f'Amount of contacts with imei changes from telgoo: {len(imei_changes_telgoo)}')
        imei_changes_unavo = conn.execute(select(unavoChanges.c['OrderId']).where(unavoChanges.c['Field_Name'] == 'IMEI', unavoChanges.c['OrderId'].in_(list_of_order_ids))).all()
        print(f'Amount of contacts with imei changes from unavo: {len(imei_changes_unavo)}')
        total_imei_changes = [x[0] for x in imei_changes_telgoo] + [x[0] for x in imei_changes_unavo]
        dict_of_imei_change_counts = {}
        for order_id in total_imei_changes:
            if dict_of_imei_change_counts.get(order_id):
                dict_of_imei_change_counts[order_id] += 1
            else:
                dict_of_imei_change_counts[order_id] = 1

        # Get ASR Data
        enrollment_id_and_total_payments_dict = {}
        asr_contacts = conn.execute(select(AgentSalesReports.c['Enrollment_ID', 'Amount'])).all()
        for contact in asr_contacts:
            if enrollment_id_and_total_payments_dict.get(contact[0]):
                if contact[1]:
                    enrollment_id_and_total_payments_dict[contact[0]] += float(contact[1])
            else:
                if contact[1]:
                    enrollment_id_and_total_payments_dict[contact[0]] = float(contact[1])

        tab_three_data_dict_by_order_id = {}
        for contact_num, contact in enumerate(contacts):
            if contact_num % 10000 == 0:
                print(contact_num)
            contact_dict = dict(zip(columns, contact))
            enroll_id = contact_dict['Customer_Order_Id']
            
            position_of_account_status = list(contact_dict.keys()).index('Account_Status') + 3
            position_of_created_date = list(contact_dict.keys()).index('Created_Date')
            items = list(contact_dict.items())

            items.insert(position_of_created_date, ('Service Reimbursements', ''))
            items.insert(position_of_created_date, ('Device Reimbursement', ''))
            if enrollment_id_and_total_payments_dict.get(enroll_id):
                items.insert(position_of_created_date, ('Payments', enrollment_id_and_total_payments_dict[enroll_id]))
            else:
                items.insert(position_of_created_date, ('Payments', 0))
                
            if dict_of_imei_change_counts.get(enroll_id):
                items.insert(position_of_account_status, ('IMEI', dict_of_imei_change_counts[enroll_id]))
            else:
                items.insert(position_of_account_status, ('IMEI', 0))
            contact_dict = dict(items)
            tab_three_data_dict_by_order_id[contact_dict['Customer_Order_Id']] = contact_dict


        for dob, dob_ssn_dict in holding_dict.items():
            for ssn, ssn_qty_and_sub_ids_dict in dob_ssn_dict.items():
                if ssn and ssn_qty_and_sub_ids_dict['qty'] > 1:
                    formatted_dob = dob
                    if dob:
                        formatted_dob = datetime.fromtimestamp(float(dob)).strftime('%Y-%m-%d')
                    for sub_id, sub_id_dict in ssn_qty_and_sub_ids_dict['sub_ids'].items():
                        for customer_order_id in sub_id_dict['customer_order_ids'].keys():
                            relevant_list = []
                            data_list = tab_three_data_dict_by_order_id[customer_order_id]
                            total_name = []
                            for k, v in data_list.items():
                                if k in ['NLAD_Subscriber_ID', 'Customer_Order_Id']:
                                    continue
                                if k == 'First_Name':
                                    total_name.append(v)
                                    continue
                                if k == 'Last_Name':
                                    total_name.append(v)
                                    relevant_list.append(' '.join(total_name))
                                    total_name = []
                                    continue
                                if k in ['DOB', 'Created_Date', 'Order_Date', 'Activation_Date', 'Deactivation_Date']:
                                    if v:
                                        v = datetime.fromtimestamp(float(v)).strftime('%Y-%m-%d')
                                relevant_list.append(v)
                            tab_three.write_row(counter_tab_three, 0, [formatted_dob, ssn, sub_id, customer_order_id] + relevant_list)
                            counter_tab_three += 1
                   
        print(f'Number of contacts from customer order ids: {len(contacts)}')


if __name__ == '__main__':
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'

    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    unavoChanges = Table("Unavo_Changes", metadata_obj, autoload_with=engine)
    telgooChanges = Table("Telgoo_Changes", metadata_obj, autoload_with=engine)
    main_customer_view = Table("Main_Customer_Database", metadata_obj, autoload_with=engine)
    AgentSalesReports = Table('AgentSalesReports', metadata_obj, autoload_with=engine)