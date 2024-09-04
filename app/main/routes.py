from flask import render_template, url_for, redirect, jsonify
from flask_login import login_required
from app import celery
from app.main import main
from app.forms import CustomerSearchForm
from app.main.utils import   get_customer_result_by_order_id,\
                        get_contacts_with_same_dob_ssn_by_sub_id
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql.expression import func
from datetime import datetime
import json
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.INFO)


@main.route('/')
@main.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html')

@main.route('/customer_search', methods=('GET', 'POST'))
@login_required
def customer_search():
    form = CustomerSearchForm()
    contacts = []  # Initialize contacts with an empty list
    headers = []   # Initialize headers with an empty list
    if form.validate_on_submit():
        engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)
        Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
        headers = ['Source_Database', 'Customer_Order_ID', 'NLAD_Subscriber_ID', 'First_Name', 'Last_Name', 'DOB', 'SSN', 'Phone1']
        indexes_of_fields = {k:v for v, k in enumerate(headers)}
        with engine.connect() as conn:
            if form.enroll_or_sub.data == 'Customer Order ID':
                unrevised_contacts = conn.execute(select(Merged_Customer_Databases.c['Source_Database', 'Customer_Order_ID', 'NLAD_Subscriber_ID', 'First_Name', 
                                                                                     'Last_Name', 'DOB', 'SSN', 'Phone1'])
                                                  .where(Merged_Customer_Databases.c['Customer_Order_ID'] == form.id.data)).all()
            else:
                unrevised_contacts = conn.execute(select(Merged_Customer_Databases.c['Source_Database', 'Customer_Order_ID', 'NLAD_Subscriber_ID', 'First_Name', 
                                                                                     'Last_Name', 'DOB', 'SSN', 'Phone1'])
                                                  .where(Merged_Customer_Databases.c['NLAD_Subscriber_ID'] == form.id.data)).all()
            if unrevised_contacts:
                for contact in unrevised_contacts:
                    row_data = [x for x in contact]
                    if row_data[indexes_of_fields['DOB']]:
                        row_data[indexes_of_fields['DOB']] = datetime.fromtimestamp(float(row_data[indexes_of_fields['DOB']])).strftime('%Y-%m-%d')
                    contacts.append(row_data)
        if len(contacts) == 1:
            return redirect(url_for('main.customer_result_by_order_id', order_id=contacts[0][indexes_of_fields['Customer_Order_ID']]))
    return render_template('customer_search.html', 
                           form=form,
                                contacts=contacts,
                                headers=headers)

@main.route('/customer_result_by_order_id/<order_id>', methods=('GET', 'POST'))
@login_required
def customer_result_by_order_id(order_id):
    contact = get_customer_result_by_order_id(order_id)
    contacts_with_same_dob_ssn_by_sub_id = get_contacts_with_same_dob_ssn_by_sub_id(contact)
    js_data = {js_contact['NLAD_Subscriber_ID']:js_contact['List_Of_Order_Ids'] for js_contact in contacts_with_same_dob_ssn_by_sub_id}
    master_agent_agent_count = {}
    for contact_for_agent_data in contacts_with_same_dob_ssn_by_sub_id:
        for key, order_id_data in contact_for_agent_data.items():
            if key == 'List_Of_Order_Ids':
                for final_dict in order_id_data:
                    master_plus_agent = f"{final_dict['MASTER_AGENT_NAME']}:{final_dict['Agent']}"
                    if master_agent_agent_count.get(master_plus_agent, None):
                        master_agent_agent_count[master_plus_agent]['qty'] += 1
                    else:
                        master_agent_agent_count[master_plus_agent] = {'master_agent_name':final_dict['MASTER_AGENT_NAME'], 'agent_name':final_dict['Agent'], 'qty':1}
    return render_template('customer_result_by_order_id.html', 
                           contact=contact,
                           contacts_with_same_dob_ssn_by_sub_id=contacts_with_same_dob_ssn_by_sub_id,
                           master_agent_agent_count=master_agent_agent_count,
                           js_data=json.dumps(js_data))
