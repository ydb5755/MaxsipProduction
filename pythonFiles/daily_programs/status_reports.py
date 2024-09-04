from sqlalchemy import create_engine, MetaData, Table, select, insert, update
from sqlalchemy.sql.expression import func
import time
from datetime import datetime, timedelta


def active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        amount = conn.execute(select(func.count())
                             .select_from(Merged_Customer_Databases)
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['ENROLLMENT_TYPE'] != '')).first()[0]
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        conn.execute(insert(Status_Report_Counts)
                     .values(
                         date=today_timestamp,
                         field='Active Customer Count',
                         count=amount
                     ))
        conn.commit()

def total_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        amount = conn.execute(select(func.count())
                             .select_from(Merged_Customer_Databases)
                             .where(Merged_Customer_Databases.c['Activation_Date'] != '')).first()[0]
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        conn.execute(insert(Status_Report_Counts)
                     .values(
                         date=today_timestamp,
                         field='Total Customer Count',
                         count=amount
                     ))
        conn.commit()

def thirty_day_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        thirty_days_ago_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        amount = conn.execute(select(func.count())
                             .select_from(Merged_Customer_Databases)
                             .where(Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] <= thirty_days_ago_timestamp,
                                    Merged_Customer_Databases.c['ENROLLMENT_TYPE'] != '')).first()[0]
        conn.execute(insert(Status_Report_Counts)
                     .values(
                         date=today_timestamp,
                         field='Thirty Day Customer Count',
                         count=amount
                     ))
        conn.commit()

def enrollment_types_active_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        plans_associations = conn.execute(select(Plan_Assignment.c['plan_name', 'program_assignment'])).all()
        plans_associations = {plan[0]:plan[1] for plan in plans_associations}
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        main_table_plans = conn.execute(select(Merged_Customer_Databases.c['Plan'])
                                           .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                                  Merged_Customer_Databases.c['Activation_Date'] != '',
                                                  Merged_Customer_Databases.c['ENROLLMENT_TYPE'] != '')).all()
        main_table_plans = [x[0] for x in main_table_plans]
        result = {}
        for plan in main_table_plans:
            if plans_associations.get(plan, None):
                if result.get(plans_associations[plan], None):
                    result[plans_associations[plan]] += 1
                else:
                    result[plans_associations[plan]] = 1
            else:
                dbs_of_new_plans = conn.execute(select(Merged_Customer_Databases.c.Source_Database)
                                        .where(Merged_Customer_Databases.c.Plan == plan)
                                        ).all()
                dbs_of_new_plans_doubles_removed = set([x[0] for x in dbs_of_new_plans])
                plans_associations[plan] = 'Unassigned'
                result[plans_associations[plan]] = 1
                for db in dbs_of_new_plans_doubles_removed:
                    conn.execute(insert(Plan_Assignment).values(source_database=db, plan_name=plan, program_assignment='Unassigned'))
                conn.commit()
        

        list_of_inserts = []
        for program, enroll_count in result.items():
            list_of_inserts.append({
                'date':today_timestamp,
                'field':program,
                'count':enroll_count
            })

        stmt = insert(Status_Report_Counts)
        conn.execute(stmt, list_of_inserts)
        conn.commit()
    
    with engine.connect() as conn:
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        for program in result.keys():
            counts = conn.execute(select(Status_Report_Counts.c['count'])
                                    .where(Status_Report_Counts.c['field'] == program)
                                    .order_by(Status_Report_Counts.c['date'].desc()).limit(30)).all()
            counts = [count[0] for count in counts]
            desired_dates = {
                'integer_field_two':0,
                'integer_field_three':1,
                'integer_field_four':6,
                'integer_field_five':29,
            }
            value_dict = {
                'report_type':'Active Customer Count By Plan',
                'text_field_one':program,
                'integer_field_one':today_timestamp
            }
            for field, date in desired_dates.items():
                if len(counts) > date:
                    value_dict[field] = counts[date]
                else:
                    value_dict[field] = 0
            conn.execute(insert(Staging_Data).values(value_dict))
        conn.commit()




def total_active_customer_age_analysis():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    thirty_days_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
    ninety_days_timestamp = time.mktime((datetime.today() - timedelta(days=90)).date().timetuple())
    one_hundred_eighty_days_timestamp = time.mktime((datetime.today() - timedelta(days=180)).date().timetuple())
    three_hundred_sixty_days_timestamp = time.mktime((datetime.today() - timedelta(days=360)).date().timetuple())
    payload = {
        'total':0,
        '0-30':0,
        '31-90':0,
        '91-180':0,
        '181-360':0,
        '360+':0
    }
    with engine.connect() as conn:
        contacts = conn.execute(select(Merged_Customer_Databases.c['Activation_Date'])
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '')).all()
        for act_date in contacts:
            act_date = act_date[0]
            payload['total'] += 1
            if act_date >= thirty_days_timestamp:
                payload['0-30'] += 1
            elif ninety_days_timestamp <= act_date < thirty_days_timestamp:
                payload['31-90'] += 1
            elif one_hundred_eighty_days_timestamp <= act_date < ninety_days_timestamp:
                payload['91-180'] += 1
            elif three_hundred_sixty_days_timestamp <= act_date < one_hundred_eighty_days_timestamp:
                payload['181-360'] += 1
            elif act_date < three_hundred_sixty_days_timestamp:
                payload['360+'] += 1
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        stringified_counts = ','.join([str(x) for x in payload.values()])
        conn.execute(insert(Status_Report_Counts).values(
            date=today_timestamp,
            field='Total Active Customer Age Analysis',
            count=stringified_counts
        ))
        conn.commit()
        
def telgoo_active_customer_age_analysis():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    thirty_days_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
    ninety_days_timestamp = time.mktime((datetime.today() - timedelta(days=90)).date().timetuple())
    one_hundred_eighty_days_timestamp = time.mktime((datetime.today() - timedelta(days=180)).date().timetuple())
    three_hundred_sixty_days_timestamp = time.mktime((datetime.today() - timedelta(days=360)).date().timetuple())
    payload = {
        'total':0,
        '0-30':0,
        '31-90':0,
        '91-180':0,
        '181-360':0,
        '360+':0
    }
    with engine.connect() as conn:
        contacts = conn.execute(select(Merged_Customer_Databases.c['Activation_Date'])
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['Source_Database'] == 'Telgoo')).all()
        for act_date in contacts:
            act_date = act_date[0]
            payload['total'] += 1
            if act_date >= thirty_days_timestamp:
                payload['0-30'] += 1
            elif ninety_days_timestamp <= act_date < thirty_days_timestamp:
                payload['31-90'] += 1
            elif one_hundred_eighty_days_timestamp <= act_date < ninety_days_timestamp:
                payload['91-180'] += 1
            elif three_hundred_sixty_days_timestamp <= act_date < one_hundred_eighty_days_timestamp:
                payload['181-360'] += 1
            elif act_date < three_hundred_sixty_days_timestamp:
                payload['360+'] += 1
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        stringified_counts = ','.join([str(x) for x in payload.values()])
        conn.execute(insert(Status_Report_Counts).values(
            date=today_timestamp,
            field='Telgoo Active Customer Age Analysis',
            count=stringified_counts
        ))
        conn.commit()
    
def terracom_active_customer_age_analysis():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    thirty_days_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
    ninety_days_timestamp = time.mktime((datetime.today() - timedelta(days=90)).date().timetuple())
    one_hundred_eighty_days_timestamp = time.mktime((datetime.today() - timedelta(days=180)).date().timetuple())
    three_hundred_sixty_days_timestamp = time.mktime((datetime.today() - timedelta(days=360)).date().timetuple())
    payload = {
        'total':0,
        '0-30':0,
        '31-90':0,
        '91-180':0,
        '181-360':0,
        '360+':0
    }
    with engine.connect() as conn:
        contacts = conn.execute(select(Merged_Customer_Databases.c['Activation_Date'])
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['Source_Database'] == 'Terracom')).all()
        for act_date in contacts:
            act_date = act_date[0]
            payload['total'] += 1
            if act_date >= thirty_days_timestamp:
                payload['0-30'] += 1
            elif ninety_days_timestamp <= act_date < thirty_days_timestamp:
                payload['31-90'] += 1
            elif one_hundred_eighty_days_timestamp <= act_date < ninety_days_timestamp:
                payload['91-180'] += 1
            elif three_hundred_sixty_days_timestamp <= act_date < one_hundred_eighty_days_timestamp:
                payload['181-360'] += 1
            elif act_date < three_hundred_sixty_days_timestamp:
                payload['360+'] += 1
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        stringified_counts = ','.join([str(x) for x in payload.values()])
        conn.execute(insert(Status_Report_Counts).values(
            date=today_timestamp,
            field='Terracom Active Customer Age Analysis',
            count=stringified_counts
        ))
        conn.commit()
    
def unavo_active_customer_age_analysis():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    thirty_days_timestamp = time.mktime((datetime.today() - timedelta(days=30)).date().timetuple())
    ninety_days_timestamp = time.mktime((datetime.today() - timedelta(days=90)).date().timetuple())
    one_hundred_eighty_days_timestamp = time.mktime((datetime.today() - timedelta(days=180)).date().timetuple())
    three_hundred_sixty_days_timestamp = time.mktime((datetime.today() - timedelta(days=360)).date().timetuple())
    payload = {
        'total':0,
        '0-30':0,
        '31-90':0,
        '91-180':0,
        '181-360':0,
        '360+':0
    }
    with engine.connect() as conn:
        contacts = conn.execute(select(Merged_Customer_Databases.c['Activation_Date'])
                             .where(Merged_Customer_Databases.c['Account_Status'] == 'Active',
                                    Merged_Customer_Databases.c['Activation_Date'] != '',
                                    Merged_Customer_Databases.c['Source_Database'] == 'Unavo')).all()
        for act_date in contacts:
            act_date = act_date[0]
            payload['total'] += 1
            if act_date >= thirty_days_timestamp:
                payload['0-30'] += 1
            elif ninety_days_timestamp <= act_date < thirty_days_timestamp:
                payload['31-90'] += 1
            elif one_hundred_eighty_days_timestamp <= act_date < ninety_days_timestamp:
                payload['91-180'] += 1
            elif three_hundred_sixty_days_timestamp <= act_date < one_hundred_eighty_days_timestamp:
                payload['181-360'] += 1
            elif act_date < three_hundred_sixty_days_timestamp:
                payload['360+'] += 1
        today_timestamp = time.mktime(datetime.today().date().timetuple())
        stringified_counts = ','.join([str(x) for x in payload.values()])
        conn.execute(insert(Status_Report_Counts).values(
            date=today_timestamp,
            field='Unavo Active Customer Age Analysis',
            count=stringified_counts
        ))
        conn.commit()


def update_last_time_updated_record():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        today_timestamp = time.mktime((datetime.now() - timedelta(hours=4)).timetuple())
        conn.execute(update(Status_Report_Counts)
                    .where(Status_Report_Counts.c['field'] == 'Last Updated')
                    .values(
                        date=today_timestamp
                    ))
        conn.commit()

    

def run_updates():
    active_customer_count()
    total_customer_count()
    thirty_day_customer_count()
    enrollment_types_active_count()
    total_active_customer_age_analysis()
    telgoo_active_customer_age_analysis()
    terracom_active_customer_age_analysis()
    unavo_active_customer_age_analysis()
    update_last_time_updated_record()

