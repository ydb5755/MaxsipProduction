from app import mail
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from app.config import Config
from flask import url_for
from flask_mail import Message
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, select
import time

def send_email_report(emails_to_send_to: list[str], subject: str = '', body: str = None, attachments:list[MIMEBase] = None):
    for email in emails_to_send_to:
        # Set up the email details
        sender = 'ybaum@maxsiptel.com'  
        receiver = email

        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = receiver
        message['Subject'] = subject
        # Add the report content to the email body
        message.attach(MIMEText(body, 'plain'))

        # Add attachments if exist
        if attachments:
            for attachment in attachments:
                message.attach(attachment)

        # Connect to SES SMTP server and send the email
        try:
            smtp_obj = smtplib.SMTP('email-smtp.us-east-2.amazonaws.com', 587)
            smtp_obj.starttls() 
            smtp_obj.login(Config.SMTP_USER, Config.SMTP_PASSWORD)  
            smtp_obj.sendmail(sender, receiver, message.as_string())
            smtp_obj.quit()
            print("Email sent successfully.")
        except smtplib.SMTPException as e:
            print(f"Error sending email: {str(e)}")

def send_reset_email(user):
    message = MIMEMultipart()
    message['From'] = 'info@maxsipsupport.com'
    message['To'] = 'ybaum@maxsiptel.com'
    message['Subject'] = 'TEST'
    message.attach(MIMEText('This is a test body', 'plain'))
    smtp_obj = smtplib.SMTP('email-smtp.us-east-1.amazonaws.com', 587)
    smtp_obj.starttls() 
    smtp_obj.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
    smtp_obj.sendmail('info@maxsipsupport.com', 'ybaum@maxsiptel.com', message.as_string())
    smtp_obj.quit()
    token = user.get_reset_token()
#     msg = Message('Password Reset Request', 
#                   sender='noreply@demo.com', 
#                   recipients=[user.email])
#     msg.body = f'''To reset your password, visit the following link:
# {url_for('users.reset_token', token=token, _external=True)}
    
# If you did not make this request then simply ignore this email and no changes will be made.
# '''
    # mail.send(msg)

def get_customer_result_by_order_id(order_id):
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        contact = conn.execute(select(Merged_Customer_Databases).where(Merged_Customer_Databases.c['Customer_Order_ID'] == order_id)).first()
        contact = {k:v for k, v in zip(Merged_Customer_Databases.c.keys(), contact)}
        for cell in contact.keys():
            if cell in ['Created_Date', 'Order_Date', 'Activation_Date', 'Deactivation_Date', 'DOB', 
                        'Last_Used_Data', 'Last_Used_Phone', 'ShipmentDate', 'deviceReimbursementDate',
                        'FollowupPeriod_CreatedDate', 'BQP_BIRTH_DATE', 'DEVICE_REIMBURSEMENT__DATE']:
                if contact[cell] and contact[cell] != 'N/G':
                    contact[cell] = datetime.fromtimestamp(float(contact[cell])).strftime('%Y-%m-%d')
            if cell in ['IMEI', 'ESN', 'MDN']:
                if contact[cell]:
                    if contact[cell][0] == "'":
                        contact[cell] = contact[cell][1:-1]
    return contact

def get_contacts_with_same_dob_ssn_by_sub_id(contact:dict):
    def combine_columns_with_data_for_each_row_in_list(columns:list, data:list[list]) -> dict:
        collection_of_dob_ssn_contacts_with_columns = []
        subscriber_ids_already_processed = []
        for dob_ssn_contact in data:
            dob_ssn_contact_with_columns = {k:v for k, v in zip(columns, dob_ssn_contact)}
            if dob_ssn_contact_with_columns['NLAD_Subscriber_ID'] in subscriber_ids_already_processed:
                continue
            else:
                collection_of_dob_ssn_contacts_with_columns.append(dob_ssn_contact_with_columns)
                subscriber_ids_already_processed.append(dob_ssn_contact_with_columns['NLAD_Subscriber_ID'])
        return collection_of_dob_ssn_contacts_with_columns
    def get_number_of_order_ids_per_sub_id_to_add_to_contact(collection_of_dob_ssn_contacts_with_columns):
        contacts_with_added_number_of_order_ids = []
        with engine.connect() as conn:
            for contact_row in collection_of_dob_ssn_contacts_with_columns:
                stmt = select(Merged_Customer_Databases.c['Customer_Order_ID', 'First_Name', 'Last_Name', 'MASTER_AGENT_NAME', 'Agent'])\
                    .where(Merged_Customer_Databases.c['NLAD_Subscriber_ID'] == contact_row['NLAD_Subscriber_ID'])
                stmt_selected_columns = stmt.selected_columns.keys()
                order_ids = conn.execute(stmt).all()
                contact_row['List_Of_Order_Ids'] = [{k:v for k, v in zip(stmt_selected_columns, x)} for x in order_ids]
                contact_row['Length_Of_List_Of_Order_Ids'] = len(order_ids)
                contacts_with_added_number_of_order_ids.append(contact_row)
        return contacts_with_added_number_of_order_ids
    engine = create_engine('sqlite:////home/ubuntu/MaxsipReports/instance/site.db')
    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)
    Merged_Customer_Databases = Table("Merged_Customer_Databases", metadata_obj, autoload_with=engine)
    dob_timestamp = time.mktime(datetime.strptime(contact['DOB'], '%Y-%m-%d').timetuple())
    with engine.connect() as conn:
        stmt_for_dob_ssn = select(Merged_Customer_Databases.c['NLAD_Subscriber_ID', 'First_Name', 'Last_Name', 'State', 'Zip'])\
                                                            .where(Merged_Customer_Databases.c['DOB'] == dob_timestamp,
                                                                   Merged_Customer_Databases.c['SSN'] == contact['SSN']
                                                                   )
        collection_of_dob_ssn_contacts_with_columns = combine_columns_with_data_for_each_row_in_list(
            stmt_for_dob_ssn.selected_columns.keys(), conn.execute(stmt_for_dob_ssn).all()
        )
    contacts_with_added_number_of_order_ids = get_number_of_order_ids_per_sub_id_to_add_to_contact(collection_of_dob_ssn_contacts_with_columns)
    return contacts_with_added_number_of_order_ids
 
