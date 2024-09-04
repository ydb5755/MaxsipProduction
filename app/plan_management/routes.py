from app.plan_management import plan_management
from sqlalchemy import create_engine, MetaData, Table, select, update, bindparam, distinct
from sqlalchemy.sql.expression import func
from flask import render_template, jsonify, request, redirect, url_for, send_from_directory
from flask_login import login_required
import json
import csv
import logging
from app.plan_management.forms import EditProgramNameForm
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.INFO)

@plan_management.route('/overview_of_plans')
@login_required
def overview_of_plans():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        plans = conn.execute(select(Plan_Assignment.c['source_database', 'plan_name', 'program_assignment'])).all()
        result = {
            'Unavo':{
                'assigned':0,
                'unassigned':0
            },
            'Telgoo':{
                'assigned':0,
                'unassigned':0
            },
            'Terracom':{
                'assigned':0,
                'unassigned':0
            }
        }
        for plan in plans:
            if plan[2] == 'Unassigned':
                result[plan[0]]['unassigned'] += 1
            else:
                result[plan[0]]['assigned'] += 1
        programs = conn.execute(select(Staging_Data.c.text_field_one).where(Staging_Data.c.report_type == 'Plan Programs')).all()
        programs = [x[0] for x in programs]
    return render_template('overview_of_plans.html', 
                           result=result,
                           programs=programs)

@plan_management.route('/download_summary_of_plans')
@login_required
def download_summary_of_plans():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    # id, plan_name, source_database, program_assignment
    directory = '/home/ubuntu/MaxsipReports/app/main/static/User_Reports'
    file = 'Plan_Report.csv'
    directory_and_file = f'{directory}/{file}'
    with engine.connect() as conn, open(directory_and_file, 'w') as csvwritefile:
        csvwriter = csv.writer(csvwritefile)
        plans = conn.execute(select(Plan_Assignment.c['id', 'plan_name', 'source_database', 'program_assignment'])).all()
        csvwriter.writerow(['ID', 'Plan Name', 'Source Database', 'Program Assignment'])
        csvwriter.writerows(plans)

    return send_from_directory(directory, file)

    

@plan_management.route('/view_and_assign_plans/<source_database>')
@login_required
def view_and_assign_plans(source_database):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        program_codes = conn.execute(select(Staging_Data.c.text_field_two).where(Staging_Data.c.report_type == 'Plan Programs')).all()
        program_codes = [x[0] for x in program_codes]
    return render_template('view_and_assign_plans.html', 
                           source_database=source_database,
                           program_codes=program_codes)

@plan_management.route('/get_plan_quantities/<source_database>')
@login_required
def get_plan_quantities(source_database):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:        
        assigned_quantity = conn.execute(select(func.count())
                                    .select_from(Plan_Assignment)
                                    .where(Plan_Assignment.c['source_database'] == source_database,
                                           Plan_Assignment.c['program_assignment'] != 'Unassigned')
                                    ).first()[0]
        
        unassigned_quantity = conn.execute(select(func.count())
                                    .select_from(Plan_Assignment)
                                    .where(Plan_Assignment.c['source_database'] == source_database,
                                           Plan_Assignment.c['program_assignment'] == 'Unassigned')
                                    ).first()[0]

    return jsonify({'assigned_quantity':assigned_quantity, 'unassigned_quantity':unassigned_quantity})

@plan_management.route('/get_five_plans/<source_database>/<view>/<offset>')
@login_required
def get_five_plans(source_database, view, offset ):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        subquery = select(Merged_Customer_Databases.c.Plan.label('plan_name'), func.count().label('active_count')).select_from(Merged_Customer_Databases)\
            .where(Merged_Customer_Databases.c.Account_Status == 'Active')\
            .group_by(Merged_Customer_Databases.c.Plan).subquery()
        subquery_two = select(Merged_Customer_Databases.c.Plan.label('plan_name'), func.count().label('inactive_count')).select_from(Merged_Customer_Databases)\
            .where(Merged_Customer_Databases.c.Account_Status != 'Active')\
            .group_by(Merged_Customer_Databases.c.Plan).subquery()
        
        query = select(
            Plan_Assignment.c.id,
            Plan_Assignment.c.plan_name,
            subquery.c.active_count,
            subquery_two.c.inactive_count,
            Plan_Assignment.c.program_assignment,
            ).select_from(
                Plan_Assignment
                .outerjoin(subquery, Plan_Assignment.c.plan_name == subquery.c.plan_name)
                .outerjoin(subquery_two, Plan_Assignment.c.plan_name == subquery_two.c.plan_name)
            ).where(
                Plan_Assignment.c.source_database == source_database
            ).order_by(
                Plan_Assignment.c.id
            ).limit(
                5
            ).offset(
                offset
            )
        if view == 'assigned':
            query = query.where(Plan_Assignment.c['program_assignment'] != 'Unassigned')
        elif view == 'unassigned':
            query = query.where(Plan_Assignment.c['program_assignment'] == 'Unassigned')
        elif view == 'both':
            pass
        else:
            return jsonify({'plans':[]})
        plans = conn.execute(query).all()
        result = []
        for plan in plans:
            holding = []
            for x in plan:
                if x == None:
                    x = 0
                holding.append(x)
            result.append(holding)
    return jsonify({'plans':result})

@plan_management.route('/update_plans', methods=['POST'])
@login_required
def update_plans():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)

    list_of_data_dicts = []

    x = json.loads(request.data)

    list_of_data_dicts.append({
        'b_id':x['id'],
        'program_assignment':x['program']
    })

    with engine.connect() as conn:
        conn.execute(update(Plan_Assignment)
                     .where(Plan_Assignment.c.id == bindparam('b_id'))
                     .values(program_assignment=bindparam('program_assignment'))
                     , list_of_data_dicts)
        conn.commit()
    return jsonify({'data':'received'})

@plan_management.route('/select_program_name')
@login_required
def select_program_name():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        programs = conn.execute(select(Staging_Data.c.text_field_one).where(Staging_Data.c.report_type == 'Plan Programs')).all()
        programs = [x[0] for x in programs]
    return render_template('select_program_name.html',
                           programs=programs)

@plan_management.route('/edit_program_name/<program_name>', methods=['GET', 'POST'])
@login_required
def edit_program_name(program_name:str):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Plan_Assignment = Table("Plan_Assignment", metadata_obj, autoload_with=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        record = conn.execute(select(Staging_Data.c['id', 'text_field_two'])
                              .where(Staging_Data.c.text_field_one == program_name)).first()
    form = EditProgramNameForm(program_name=program_name, program_code=record[1], id=record[0])
    if form.validate_on_submit():
        with engine.connect() as conn:
            conn.execute(update(Staging_Data)
                         .where(Staging_Data.c.id == form.id.data)
                         .values(
                             text_field_one=form.program_name.data,
                             text_field_two=form.program_code.data
                         ))
            conn.execute(update(Plan_Assignment)
                         .where(Plan_Assignment.c.program_assignment == program_name)
                         .values(
                             program_assignment=form.program_name.data
                         ))
            conn.commit()
        return redirect(url_for('plan_management.select_program_name'))
    return render_template('edit_program_name.html', 
                           program_name=program_name,
                           form=form)

@plan_management.route('/get_program_names')
@login_required
def get_program_names():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        programs = conn.execute(select(distinct(Staging_Data.c.text_field_one))
                                .where(Staging_Data.c.report_type == 'Plan Programs')).all()
        result = {}
        for index in range(len(programs)):
            result[index] = programs[index][0]
    return jsonify({'data':result})
