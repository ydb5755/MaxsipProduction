from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, select, update, bindparam
from sqlalchemy.sql.expression import func
import psutil


def adding_groups_to_contacts_take_four():
    path_to_db = 'sqlite:////home/ubuntu/MaxsipReports/instance/site.db'
    engine = create_engine(path_to_db)
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)

    print(f'Starting available memory: {psutil.virtual_memory().available}')

    then = datetime.now()
    qualifications_to_be_a_duplicate = {
        0:['NLAD_Subscriber_ID'],
        1:['First_Name', 'Last_Name', 'Address1', 'Zip'],
        2:['DOB', 'SSN', 'First_Name'],
        3:['DOB', 'SSN', 'Last_Name'],
        4:['SSN', 'First_Name', 'Address1'],
        5:['SSN', 'Last_Name', 'Address1'],
        6:['DOB', 'First_Name', 'Address1'],
        7:['DOB', 'Last_Name', 'Address1']
    }
    stmt = select(Merged_Customer_Databases.c['Customer_Order_ID', 'NLAD_Subscriber_ID', 'DOB', 
                                                'SSN', 'First_Name', 'Last_Name', 'Address1', 'Zip'])
    fields_dict = {k:v for v, k in enumerate(stmt.selected_columns.keys())}

    with engine.connect() as conn:
        contacts = conn.execute(stmt).all()

    total_contacts_length = len(contacts)
    
    def check_for_falsy(contact:list) -> bool:
        for data_piece in contact:
            if bool(data_piece) == False:
                return False
        return True

    customer_order_id_to_group = {}

    for contact in contacts:
        are_all_data_pieces_truthy = check_for_falsy(contact)
        if not are_all_data_pieces_truthy:
            customer_order_id_to_group[contact[fields_dict['Customer_Order_ID']]] = {
                'Group Number': 1,
                'Match ID':'',
                'Matched Params':''
            }

    starting_length_of_cust_order_ids = len(customer_order_id_to_group.keys())
    print(f'Number of contacts with missing data: {starting_length_of_cust_order_ids}')

    order_id_indexed_bank = {contact[fields_dict['Customer_Order_ID']]:contact for contact in contacts}

    total_bank = {}
    for params in qualifications_to_be_a_duplicate.values():
        total_bank['.'.join(params)] = {}
    for joined_params_names, param_values_to_order_id_list in total_bank.items():
        split_params = joined_params_names.split('.')
        print(f'starting: {split_params}')
        for contact in contacts:
            customer_order_id = contact[fields_dict['Customer_Order_ID']]
            if customer_order_id_to_group.get(customer_order_id, None):
                continue
            joined_param_values_of_contact = '.'.join([str(contact[fields_dict[param]]) for param in split_params])
            if param_values_to_order_id_list.get(joined_param_values_of_contact, None):
                param_values_to_order_id_list[joined_param_values_of_contact].append(customer_order_id)
            else:
                param_values_to_order_id_list[joined_param_values_of_contact] = [customer_order_id]
        print(f'Length of keys: {len(param_values_to_order_id_list.keys())}\nRemaining memory: {psutil.virtual_memory().available}')
    print(datetime.now() - then)

    group_number = 101


    def get_matches_by_params(total_group:list, params:list) -> list:
        matches = []
        for single_contact in total_group:
            joined_contact_params = '.'.join([str(single_contact[fields_dict[param]]) for param in params])
            list_of_common_order_ids = total_bank['.'.join(params)][joined_contact_params]
            for order_id_match in list_of_common_order_ids:
                if not customer_order_id_to_group.get(order_id_match, None):
                    matches.append(order_id_indexed_bank[order_id_match])
        return matches

    def go_through_data_sets(contact_data:list, index:int, match_origin:str='Parent'):
        total_group = [contact_data]
        if index == 8:
            customer_order_id_to_group[contact_data[fields_dict['Customer_Order_ID']]] = {
                'Group Number': group_number,
                'Match ID':match_origin,
                'Matched Params':''
            }
        else:
            customer_order_id_to_group[contact_data[fields_dict['Customer_Order_ID']]] = {
                'Group Number': group_number,
                'Match ID':match_origin,
                'Matched Params':index + 1
            }
        for number, param_set in qualifications_to_be_a_duplicate.items():
            if number == index:
                break
            matches = get_matches_by_params(total_group, param_set)
            if matches:
                total_group += matches
                for contact_match in matches:
                    go_through_data_sets(contact_match, number, contact_data[fields_dict['Customer_Order_ID']])

    start_time = datetime.now()
    for contact_num, contact in enumerate(contacts):
        row_data = [x for x in contact]
        customer_order_id = row_data[fields_dict['Customer_Order_ID']]
        if customer_order_id_to_group.get(customer_order_id, None):
            continue
        go_through_data_sets(row_data, index=8)
        group_number += 1
        total_customer_order_ids_accounted_for = len(customer_order_id_to_group.keys()) - starting_length_of_cust_order_ids
        print(f'\rPercent completed: {(contact_num + 1)/total_contacts_length*100}, Estimated Time to finish: {((datetime.now() - start_time)/(total_customer_order_ids_accounted_for))*(total_contacts_length-total_customer_order_ids_accounted_for)}, Length of customer order id to group: {len(customer_order_id_to_group)}       ', end='', flush=True)
    print(f'\nTotal function time: {datetime.now() - start_time}')

    with engine.connect() as conn:
        list_for_update = []
        for customer_order_id, group_data in customer_order_id_to_group.items():
            list_for_update.append({
                'b_Customer_Order_ID':customer_order_id,
                'Group_ID':group_data['Group Number']
            })
            if len(list_for_update) == 1000:
                conn.execute(update(Merged_Customer_Databases)
                                .where(Merged_Customer_Databases.c.Customer_Order_ID == bindparam('b_Customer_Order_ID'))
                                .values(Group_ID = bindparam('Group_ID'))
                                ,list_for_update)
                conn.commit()
                list_for_update = []

        if len(list_for_update) > 0:
            conn.execute(update(Merged_Customer_Databases)
                                .where(Merged_Customer_Databases.c.Customer_Order_ID == bindparam('b_Customer_Order_ID'))
                                .values(Group_ID = bindparam('Group_ID'))
                                ,list_for_update)
            conn.commit()

