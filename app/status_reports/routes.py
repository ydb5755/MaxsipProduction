from app.status_reports import status_reports
from flask import render_template, jsonify
from flask_login import login_required
from sqlalchemy import create_engine, MetaData, Table, select
from datetime import datetime

@status_reports.route('/active_total_and_thirty')
@login_required
def active_total_and_thirty():
    return render_template('active_total_and_thirty.html')

@status_reports.route('/active_by_plan')
@login_required
def active_by_plan():
    return render_template('active_by_plan.html')

@status_reports.route('/customer_age_by_portal')
@login_required
def customer_age_by_portal():
    return render_template('customer_age_by_portal.html')

@status_reports.route('/get_active_customer_count')
@login_required
def get_active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        contacts = conn.execute(select(Status_Report_Counts.c['count'])
                                .where(Status_Report_Counts.c['field'] == 'Active Customer Count')
                                .order_by(Status_Report_Counts.c['date'].desc()).limit(30)).all()
    contacts = [contact[0] for contact in contacts]
    payload = {
        'current':contacts[0],
        'yesterday':contacts[1],
        'last_week':contacts[6],
        'last_month':contacts[-1]
    }

    return jsonify(payload), 200

@status_reports.route('/get_total_customer_count')
@login_required
def get_total_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        contacts = conn.execute(select(Status_Report_Counts.c['count'])
                                .where(Status_Report_Counts.c['field'] == 'Total Customer Count')
                                .order_by(Status_Report_Counts.c['date'].desc()).limit(30)).all()
    contacts = [contact[0] for contact in contacts]
    payload = {
        'current':contacts[0],
        'yesterday':contacts[1],
        'last_week':contacts[6],
        'last_month':contacts[-1]
    }
    return jsonify(payload), 200

@status_reports.route('/get_thirty_day_customer_count')
@login_required
def get_thirty_day_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        contacts = conn.execute(select(Status_Report_Counts.c['count'])
                                .where(Status_Report_Counts.c['field'] == 'Thirty Day Customer Count')
                                .order_by(Status_Report_Counts.c['date'].desc()).limit(30)).all()
    contacts = [contact[0] for contact in contacts]
    payload = {
        'current':contacts[0],
        'yesterday':contacts[1],
        'last_week':contacts[6],
        'last_month':contacts[-1]
    }
    return jsonify(payload), 200


@status_reports.route('/get_time_last_updated_status_reports')
@login_required
def get_time_last_updated_status_reports():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        result = conn.execute(select(Status_Report_Counts.c['date']).where(Status_Report_Counts.c['field'] == 'Last Updated')).first()[0]
    result = datetime.fromtimestamp(result).strftime('%m/%d/%Y %H:%M')
    payload = {'last_updated':result}
    return jsonify(payload), 200







@status_reports.route('/get_program_data')
@login_required
def get_program_data():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Staging_Data = Table("Staging_Data", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        date_subquery = select(Staging_Data.c.integer_field_one)\
            .where(Staging_Data.c.report_type == 'Active Customer Count By Plan')\
            .order_by(Staging_Data.c.id.desc())\
            .limit(1).scalar_subquery()
        counts = conn.execute(select(Staging_Data.c['text_field_one', 'integer_field_two', 'integer_field_three', 'integer_field_four', 'integer_field_five'])
                              .where(Staging_Data.c.integer_field_one == date_subquery,
                                     Staging_Data.c.report_type == 'Active Customer Count By Plan')).all()
        payload = {}
        for count in counts:
            payload[count[0]] = [count[1], count[2], count[3], count[4]]
        return jsonify(payload)


@status_reports.route('/get_total_active_customer_count')
@login_required
def get_total_active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        record = conn.execute(select(Status_Report_Counts)
                              .where(Status_Report_Counts.c.field == 'Total Active Customer Age Analysis')
                              .order_by(Status_Report_Counts.c.date.desc())).first()
    destringified_counts = record[-1].split(',')
    payload = {
        'total':int(destringified_counts[0]),
        '0-30':int(destringified_counts[1]),
        '31-90':int(destringified_counts[2]),
        '91-180':int(destringified_counts[3]),
        '181-360':int(destringified_counts[4]),
        '360+':int(destringified_counts[5])
    }
    return jsonify(payload), 200

@status_reports.route('/get_telgoo_active_customer_count')
@login_required
def get_telgoo_active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        record = conn.execute(select(Status_Report_Counts)
                              .where(Status_Report_Counts.c.field == 'Telgoo Active Customer Age Analysis')
                              .order_by(Status_Report_Counts.c.date.desc())).first()
    destringified_counts = record[-1].split(',')
    payload = {
        'total':int(destringified_counts[0]),
        '0-30':int(destringified_counts[1]),
        '31-90':int(destringified_counts[2]),
        '91-180':int(destringified_counts[3]),
        '181-360':int(destringified_counts[4]),
        '360+':int(destringified_counts[5])
    }
    return jsonify(payload), 200

@status_reports.route('/get_terracom_active_customer_count')
@login_required
def get_terracom_active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        record = conn.execute(select(Status_Report_Counts)
                              .where(Status_Report_Counts.c.field == 'Terracom Active Customer Age Analysis')
                              .order_by(Status_Report_Counts.c.date.desc())).first()
    destringified_counts = record[-1].split(',')
    payload = {
        'total':int(destringified_counts[0]),
        '0-30':int(destringified_counts[1]),
        '31-90':int(destringified_counts[2]),
        '91-180':int(destringified_counts[3]),
        '181-360':int(destringified_counts[4]),
        '360+':int(destringified_counts[5])
    }
    return jsonify(payload), 200

@status_reports.route('/get_unavo_active_customer_count')
@login_required
def get_unavo_active_customer_count():
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Status_Report_Counts = Table("Status_Report_Counts", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        record = conn.execute(select(Status_Report_Counts)
                              .where(Status_Report_Counts.c.field == 'Unavo Active Customer Age Analysis')
                              .order_by(Status_Report_Counts.c.date.desc())).first()
    destringified_counts = record[-1].split(',')
    payload = {
        'total':int(destringified_counts[0]),
        '0-30':int(destringified_counts[1]),
        '31-90':int(destringified_counts[2]),
        '91-180':int(destringified_counts[3]),
        '181-360':int(destringified_counts[4]),
        '360+':int(destringified_counts[5])
    }
    return jsonify(payload), 200
