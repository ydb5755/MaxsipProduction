from sqlalchemy import create_engine, MetaData, Table, select, insert, update
from sqlalchemy.sql.expression import func
import time
from datetime import datetime, timedelta


def active_master_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    target_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'MASTER_AGENT_NAME'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                                  Merged_Customer_Databases.c['MASTER_AGENT_NAME'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for master_agent in main_table_contacts.values():
            if result.get(master_agent, None):
                result[master_agent] += 1
            else:
                result[master_agent] = 1
        for ma, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Active Master Agent',
                'integer_field_one':target_timestamp,
                'text_field_one':ma,
                'integer_field_two':enroll_count
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()

def active_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    target_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'Agent'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                                  Merged_Customer_Databases.c['Agent'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for agent in main_table_contacts.values():
            if result.get(agent, None):
                result[agent] += 1
            else:
                result[agent] = 1
        for agent, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Active Agent',
                'integer_field_one':target_timestamp,
                'text_field_one':agent,
                'integer_field_two':enroll_count
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()

def total_master_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    target_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'MASTER_AGENT_NAME'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['MASTER_AGENT_NAME'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for master_agent in main_table_contacts.values():
            if result.get(master_agent, None):
                result[master_agent] += 1
            else:
                result[master_agent] = 1
        for ma, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Total Master Agent Count',
                'integer_field_one':target_timestamp,
                'text_field_one':ma,
                'integer_field_two':enroll_count
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()

def total_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    target_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'Agent'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['Agent'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for agent in main_table_contacts.values():
            if result.get(agent, None):
                result[agent] += 1
            else:
                result[agent] = 1
        for agent, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Total Agent Count',
                'integer_field_one':target_timestamp,
                'text_field_one':agent,
                'integer_field_two':enroll_count
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()

def thirty_day_master_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    today_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        thirty_days_ago_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'MASTER_AGENT_NAME'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['Activation_Date'] <= thirty_days_ago_timestamp,
                                                  Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                                  Merged_Customer_Databases.c['MASTER_AGENT_NAME'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for master_agent in main_table_contacts.values():
            if result.get(master_agent, None):
                result[master_agent] += 1
            else:
                result[master_agent] = 1
        for master_agent, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Total 30 Day Master Agent Count',
                'integer_field_one':today_timestamp,
                'text_field_one':master_agent,
                'integer_field_two':enroll_count
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()


def thirty_day_agent_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    today_timestamp = time.mktime(datetime.today().date().timetuple())
    
    with engine.connect() as conn:
        thirty_days_ago_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
        main_table_contacts = conn.execute(select(Merged_Customer_Databases.c['Customer_Order_ID', 'Agent'])
                                           .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['Activation_Date'] <= thirty_days_ago_timestamp,
                                                  Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                                  Merged_Customer_Databases.c['Agent'] != '')).all()
        main_table_contacts = {
            contact[0]:contact[1] for contact in main_table_contacts
        }
        result = {}
        list_of_inserts = []
        for agent in main_table_contacts.values():
            if result.get(agent, None):
                result[agent] += 1
            else:
                result[agent] = 1
        for agent, enroll_count in result.items():
            list_of_inserts.append({
                'report_type':'Total 30 Day Agent Count',
                'integer_field_one':today_timestamp,
                'text_field_one':agent,
                'integer_field_two':enroll_count,
            })


        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()



def customer_age_analysis_by_agent_and_master_agent():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    fifteen_days_timestamp = time.mktime((datetime.today() - timedelta(days=15)).date().timetuple())
    thirty_days_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
    ninety_days_timestamp = time.mktime((datetime.today() - timedelta(days=90)).date().timetuple())
    one_hundred_eighty_days_timestamp = time.mktime((datetime.today() - timedelta(days=180)).date().timetuple())
    three_hundred_sixty_days_timestamp = time.mktime((datetime.today() - timedelta(days=360)).date().timetuple())
    result = {
        'agents':{},
        'master_agents':{}
    }
    with engine.connect() as conn:
        contacts = conn.execute(select(Merged_Customer_Databases.c['Activation_Date', 'Agent', 'MASTER_AGENT_NAME'])
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['Agent'] != '',
                                    Merged_Customer_Databases.c['MASTER_AGENT_NAME'] != '')).all()
        for record in contacts:
            if not result['agents'].get(record[1], None):
                result['agents'][record[1]] = {
                    'total':0,
                    '0-15':0,
                    '16-30':0,
                    '31-90':0,
                    '91-180':0,
                    '181-360':0,
                    '360+':0
                }
            if not result['master_agents'].get(record[2], None):
                result['master_agents'][record[2]] = {
                    'total':0,
                    '0-15':0,
                    '16-30':0,
                    '31-90':0,
                    '91-180':0,
                    '181-360':0,
                    '360+':0
                }
            act_date = record[0]
            result['agents'][record[1]]['total'] += 1
            result['master_agents'][record[2]]['total'] += 1
            if act_date >= fifteen_days_timestamp:
                result['agents'][record[1]]['0-15'] += 1
                result['master_agents'][record[2]]['0-15'] += 1
            elif thirty_days_timestamp <= act_date < fifteen_days_timestamp:
                result['agents'][record[1]]['16-30'] += 1
                result['master_agents'][record[2]]['16-30'] += 1
            elif ninety_days_timestamp <= act_date < thirty_days_timestamp:
                result['agents'][record[1]]['31-90'] += 1
                result['master_agents'][record[2]]['31-90'] += 1
            elif one_hundred_eighty_days_timestamp <= act_date < ninety_days_timestamp:
                result['agents'][record[1]]['91-180'] += 1
                result['master_agents'][record[2]]['91-180'] += 1
            elif three_hundred_sixty_days_timestamp <= act_date < one_hundred_eighty_days_timestamp:
                result['agents'][record[1]]['181-360'] += 1
                result['master_agents'][record[2]]['181-360'] += 1
            elif act_date < three_hundred_sixty_days_timestamp:
                result['agents'][record[1]]['360+'] += 1
                result['master_agents'][record[2]]['360+'] += 1

        today_timestamp = time.mktime(datetime.today().date().timetuple())
        list_of_inserts = []
        for name, payload in result['agents'].items():
            list_of_inserts.append({
                'report_type':'Customer Age Analysis By Agent',
                'integer_field_one':today_timestamp,
                'text_field_one':name,
                'integer_field_two':payload['total'],
                'integer_field_three':payload['0-15'],
                'integer_field_four':payload['16-30'],
                'integer_field_five':payload['31-90'],
                'integer_field_six':payload['91-180'],
                'integer_field_seven':payload['181-360'],
                'integer_field_eight':payload['360+']
            })
        for name, payload in result['master_agents'].items():
            list_of_inserts.append({
                'report_type':'Customer Age Analysis By Master Agent',
                'integer_field_one':today_timestamp,
                'text_field_one':name,
                'integer_field_two':payload['total'],
                'integer_field_three':payload['0-15'],
                'integer_field_four':payload['16-30'],
                'integer_field_five':payload['31-90'],
                'integer_field_six':payload['91-180'],
                'integer_field_seven':payload['181-360'],
                'integer_field_eight':payload['360+']
            })
        stmt = insert(Staging_Data)
        conn.execute(stmt, list_of_inserts)
        conn.commit()



def update_last_time_updated_record():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        today_timestamp = time.mktime((datetime.now() - timedelta(hours=4)).timetuple())
        conn.execute(update(Staging_Data)
                    .where(Staging_Data.c['text_field_one'] == 'Last Updated')
                    .values(
                        integer_field_one=today_timestamp
                    ))
        conn.commit()
 

def run_updates():
    active_master_agent_count()
    active_agent_count()
    total_master_agent_count()
    total_agent_count()
    thirty_day_master_agent_count()
    thirty_day_agent_count()
    customer_age_analysis_by_agent_and_master_agent()
    update_last_time_updated_record()