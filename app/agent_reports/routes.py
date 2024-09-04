from app.agent_reports import agent_reports
from app.agent_reports.utils import get_range
from flask import render_template, jsonify
from flask_login import login_required
from sqlalchemy import create_engine, MetaData, Table, select, distinct
from sqlalchemy.sql import func
import time
from datetime import datetime, timedelta


@agent_reports.route('/get_date_lower_limit')
@login_required
def get_date_lower_limit():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        return jsonify({'result':len(conn.execute(select(distinct(Staging_Data.c.integer_field_one))).all()) - 40})




##### Agent Customer Analysis Collection #####
@agent_reports.route('/agent_customer_analysis')
@login_required
def agent_customer_analysis():
    return render_template('agent_customer_analysis.html')

@agent_reports.route('/get_total_active_customer_count_for_agent_and_master_agent/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_total_active_customer_count_for_agent_and_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Active Agent',
        'master_agent':'Active Master Agent'
    }
    with engine.connect() as conn:
        date_subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(Staging_Data.c['integer_field_two'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == date_subq)).all()
    total_count = 0
    for count in agents:
        total_count += int(count[0])
    return jsonify({'count':total_count})

@agent_reports.route('/get_total_number_of_agents_for_agent_customer_analysis/<agent_or_master_agent>/<day_request_offset>')
def get_total_number_of_agents_for_agent_customer_analysis(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Active Agent',
        'master_agent':'Active Master Agent'
    }
    with engine.connect() as conn:
        date_subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()
        agents = conn.execute(select(func.count()).select_from(Staging_Data)
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == date_subq)).scalar()
    
    return jsonify({'count':agents})

@agent_reports.route('/get_five_agents_for_active/<offset>/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_five_agents_for_active(offset, agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    Transfer_Outs = Table("Transfer_Outs", metadata_obj, autoload_with=engine)


    today_minus_offset = time.mktime((datetime.today() - timedelta(days=(int(day_request_offset)))).date().timetuple())
    thirty_days = time.mktime((datetime.today() - timedelta(days=30 + int(day_request_offset))).date().timetuple())
    agent_or_master_agent_map = {
        'agent':['Active Agent', 'EMPLOYEE'],
        'master_agent':['Active Master Agent', 'MASTER_AGENT']
    }
    with engine.connect() as conn:
        date_subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent][0])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()
        
        stmt = conn.execute(select(Staging_Data.c['text_field_one'])\
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent][0],
                                    Staging_Data.c['integer_field_one'] == date_subq)\
                            .order_by(Staging_Data.c['integer_field_two'].desc())\
                            .offset(offset)\
                            .limit(5)).all()
        top_five = [x[0] for x in stmt]

        agents = conn.execute(select(Staging_Data.c['text_field_one', 'integer_field_two', 'integer_field_one']) # name, date, count
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent][0],
                                    Staging_Data.c['text_field_one'].in_(top_five),
                                    Staging_Data.c['integer_field_one'] <= date_subq)).all()
        
        relevant_transfer_outs = conn.execute(select(Transfer_Outs.c['TRANSACTION_DATE', agent_or_master_agent_map[agent_or_master_agent][1]])\
                                                .where(Transfer_Outs.c[agent_or_master_agent_map[agent_or_master_agent][1]].in_(top_five), 
                                                        Transfer_Outs.c['TRANSACTION_DATE'] >= thirty_days,
                                                        Transfer_Outs.c['TRANSACTION_DATE'] <= today_minus_offset)).all()

    to_result = {}
    for to in relevant_transfer_outs:
        if to_result.get(to[1], None):
            to_result[to[1]][get_range(float(to[0]), int(day_request_offset))] += 1
            to_result[to[1]]['1-30'] += 1
        else:
            to_result[to[1]] = {
                '1-3':0,
                '4-7':0,
                '8-15':0,
                '16-30':0,
                '1-30':1
            }
            to_result[to[1]][get_range(float(to[0]), int(day_request_offset))] += 1
    
    result = {}
    for agent in agents:
        if result.get(agent[0], None):
            result[agent[0]].append([agent[1], agent[2]])
        else:
            result[agent[0]] = [[agent[1], agent[2]]]
        
    for dates in result.values():
        dates.sort(reverse=True, key=lambda x: x[1])

    agents = []
    for name, values in result.items():
        value_list_len = len(values)
        desired_dates = [0,1,6,29]
        value_data = []
        for date_count in desired_dates:
            if value_list_len > date_count:
                value_data.append(values[date_count][0])
            else:
                value_data.append(values[0][0])
        if to_result.get(name, None):
            agents.append(
                [name] + 
                value_data +  
                [' ', to_result[name]['1-3'], to_result[name]['4-7'], to_result[name]['8-15'], 
                 to_result[name]['16-30'], to_result[name]['1-30']]
            )
        else:
            agents.append([name] + value_data + [' ', ' ', ' ', ' ', ' ', ' '])
    agents.sort(reverse=True, key=lambda x:x[1])

    return jsonify({'agents':agents})





##### Total Customer Count Collection #####
@agent_reports.route('/total_customer_count_by_agent_or_master_agent')
@login_required
def total_customer_count_by_agent_or_master_agent():
    return render_template('total_customer_count_by_agent_or_master_agent.html')

@agent_reports.route('/get_five_agents_for_total/<offset>/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_five_agents_for_total(offset, agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Total Agent Count',
        'master_agent':'Total Master Agent Count'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()
                
        stmt = conn.execute(select(Staging_Data.c['text_field_one'])\
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)\
                            .order_by(Staging_Data.c['integer_field_two'].desc())\
                            .offset(offset)\
                            .limit(5)).all()
        top_five = [x[0] for x in stmt]
        agents = conn.execute(select(Staging_Data.c['text_field_one', 'integer_field_two', 'integer_field_one'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['text_field_one'].in_(top_five),
                                    Staging_Data.c['integer_field_one'] <= subq)).all()
    result = {}
    for agent in agents:
        if result.get(agent[0], None):
            result[agent[0]].append([agent[1], agent[2]])
        else:
            result[agent[0]] = [[agent[1], agent[2]]]
        
    for dates in result.values():
        dates.sort(reverse=True, key=lambda x: x[1])

    agents = []
    for name, values in result.items():
        value_list_len = len(values)
        desired_dates = [0,1,6,29]
        value_data = []
        for date_count in desired_dates:
            if value_list_len > date_count:
                value_data.append(values[date_count][0])
            else:
                value_data.append(values[0][0])
        agents.append([name] + value_data)
    agents.sort(reverse=True, key=lambda x:x[1])
    return jsonify({'agents':agents})

@agent_reports.route('/get_total_customer_count_for_agent_and_master_agent/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_total_customer_count_for_agent_and_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Total Agent Count',
        'master_agent':'Total Master Agent Count'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(Staging_Data.c['integer_field_two'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).all()
    total_count = 0
    for count in agents:
        total_count += int(count[0])
    return jsonify({'count':total_count})

@agent_reports.route('/get_total_number_of_agents_for_total_customer_count_by_agent_or_master_agent/<agent_or_master_agent>/<day_request_offset>')
def get_total_number_of_agents_for_total_customer_count_by_agent_or_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Customer Age Analysis By Agent',
        'master_agent':'Customer Age Analysis By Master Agent'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(func.count()).select_from(Staging_Data)
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).scalar()
    return jsonify({'count':agents})





##### Thirty Day Collection #####
@agent_reports.route('/thirty_day_agent_and_master_agent')
@login_required
def thirty_day_agent_and_master_agent():
    return render_template('thirty_day_agent_and_master_agent.html')

@agent_reports.route('/get_thirty_day_total_customer_count_for_agent_and_master_agent/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_thirty_day_total_customer_count_for_agent_and_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Total 30 Day Agent Count',
        'master_agent':'Total 30 Day Master Agent Count'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(Staging_Data.c['integer_field_two'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).all()
    total_count = 0
    for count in agents:
        total_count += int(count[0])
    return jsonify({'count':total_count})

@agent_reports.route('/get_total_number_of_agents_for_thirty_day_agent_and_master_agent/<agent_or_master_agent>/<day_request_offset>')
def get_total_number_of_agents_for_thirty_day_agent_and_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Total 30 Day Agent Count',
        'master_agent':'Total 30 Day Master Agent Count'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(func.count()).select_from(Staging_Data)
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).scalar()
    return jsonify({'count':agents})

@agent_reports.route('/get_five_agents_for_thirty_day_agent_and_master_agent/<offset>/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_five_agents_for_thirty_day_agent_and_master_agent(offset, agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Total 30 Day Agent Count',
        'master_agent':'Total 30 Day Master Agent Count'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()
                
        stmt = conn.execute(select(Staging_Data.c['text_field_one'])\
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)\
                            .order_by(Staging_Data.c['integer_field_two'].desc())\
                            .offset(offset)\
                            .limit(5)).all()
        top_five = [x[0] for x in stmt]
        agents = conn.execute(select(Staging_Data.c['text_field_one', 'integer_field_two', 'integer_field_one'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['text_field_one'].in_(top_five),
                                    Staging_Data.c['integer_field_one'] <= subq)).all()
    result = {}
    for agent in agents:
        if result.get(agent[0], None):
            result[agent[0]].append([agent[1], agent[2]])
        else:
            result[agent[0]] = [[agent[1], agent[2]]]
        
    for dates in result.values():
        dates.sort(reverse=True, key=lambda x: x[1])

    agents = []
    for name, values in result.items():
        value_list_len = len(values)
        desired_dates = [0,1,6,29]
        value_data = []
        for date_count in desired_dates:
            if value_list_len > date_count:
                value_data.append(values[date_count][0])
            else:
                value_data.append(values[0][0])
        agents.append([name] + value_data)
    agents.sort(reverse=True, key=lambda x:x[1])
    return jsonify({'agents':agents})





##### Customer Age Analysis Collection #####
@agent_reports.route('/customer_age_analysis_by_agent_and_master_agent')
@login_required
def customer_age_analysis_by_agent_and_master_agent():
    return render_template('customer_age_analysis_by_agent_and_master_agent.html')

@agent_reports.route('/get_date_lower_limit_for_customer_age_analysis')
@login_required
def get_date_lower_limit_for_customer_age_analysis():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    today_timestamp = time.mktime(datetime.today().date().timetuple())
    with engine.connect() as conn:
        result = conn.execute(select(Staging_Data.c.integer_field_one)
                              .where(Staging_Data.c.report_type == 'Customer Age Analysis By Agent')
                              .order_by(Staging_Data.c.integer_field_one.asc())).first()
        result = [x for x in result][0]
    days_available = today_timestamp - result
    if not days_available == 0:
        days_available = days_available/86400
    
    return jsonify({'result':days_available})

@agent_reports.route('/get_total_customer_count_for_customer_age_analysis_by_agent_and_master_agent/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_total_customer_count_for_customer_age_analysis_by_agent_and_master_agent(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Customer Age Analysis By Agent',
        'master_agent':'Customer Age Analysis By Master Agent'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(Staging_Data.c['integer_field_two'])
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).all()
    total_count = 0
    for count in agents:
        total_count += int(count[0])
    return jsonify({'count':total_count})

@agent_reports.route('/get_total_number_of_agents_for_customer_age_analysis/<agent_or_master_agent>/<day_request_offset>')
def get_total_number_of_agents_for_customer_age_analysis(agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Customer Age Analysis By Agent',
        'master_agent':'Customer Age Analysis By Master Agent'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()

        agents = conn.execute(select(func.count()).select_from(Staging_Data)
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)).scalar()
    return jsonify({'count':agents})

@agent_reports.route('/get_all_agents_for_customer_age_analysis_by_agent_and_master_agent/<offset>/<agent_or_master_agent>/<day_request_offset>')
@login_required
def get_all_agents_for_customer_age_analysis_by_agent_and_master_agent(offset, agent_or_master_agent, day_request_offset):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    agent_or_master_agent_map = {
        'agent':'Customer Age Analysis By Agent',
        'master_agent':'Customer Age Analysis By Master Agent'
    }
    with engine.connect() as conn:
        subq = select(distinct(Staging_Data.c['integer_field_one']))\
                .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent])\
                .order_by(Staging_Data.c['integer_field_one'].desc())\
                .offset(int(day_request_offset)).limit(1).scalar_subquery()
                
        stmt = conn.execute(select(Staging_Data.c['text_field_one', 'integer_field_two', 'integer_field_three', 
                                                  'integer_field_four', 'integer_field_five', 'integer_field_six', 
                                                  'integer_field_seven', 'integer_field_eight'])\
                            .where(Staging_Data.c['report_type'] == agent_or_master_agent_map[agent_or_master_agent],
                                    Staging_Data.c['integer_field_one'] == subq)\
                            .order_by(Staging_Data.c['integer_field_two'].desc())\
                            .offset(offset).limit(5)).all()
    result = []
    for agent in stmt:
        result.append([x for x in agent])
    result.sort(reverse=True, key=lambda x:x[1])
    return jsonify({'agents':result})



