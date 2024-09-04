from app import db
from flask import current_app
from flask_login import UserMixin, current_user
from sqlalchemy import TEXT, Column, Boolean, ForeignKey, TEXT, INTEGER, VARCHAR
import jwt
from datetime import datetime, timezone, timedelta


class User(db.Model, UserMixin):
    __tablename__ = 'user'
    id         = Column('id', INTEGER(), primary_key=True)
    first_name = Column('first_name', TEXT(), nullable=False)
    last_name  = Column('last_name', TEXT(), nullable=False)
    email      = Column('email', TEXT(), nullable=False, unique=True)
    password   = Column('password', TEXT(), nullable=False)
    admin      = Column('admin', Boolean(), default=False)
    user_type  = Column('user_type', TEXT(), nullable=False)
    
    def get_reset_token(self, expiration=600):
        reset_token = jwt.encode(
            {
                "confirm": self.id,
                "exp": datetime.now(tz=timezone.utc)
                       + timedelta(seconds=expiration)
            },
            current_app.config['SECRET_KEY'],
            algorithm="HS256"
        )
        return reset_token
    
    @staticmethod
    def verify_reset_token(token):
        try:
            data = jwt.decode(
                token,
                current_app.config['SECRET_KEY'],
                leeway=timedelta(seconds=10),
                algorithms=["HS256"]
            )
        except:
            return None
        if not User.query.get(data.get('confirm')):
            return None
        return User.query.get(data.get('confirm'))

class User_Generated_Reports(db.Model):
    __tablename__  = 'User_Generated_Reports'
    id             = Column('id', INTEGER(), primary_key=True)
    user_id        = Column('user_id', INTEGER(), nullable=True)
    report_type    = Column('report_type', TEXT(), nullable=True)
    number_of_rows = Column('number_of_rows', TEXT(), nullable=True)
    path           = Column('path', TEXT(), nullable=True)
    time_created   = Column('time_created', INTEGER(), nullable=True)
    range_start    = Column('range_start', INTEGER(), nullable=True)
    range_end      = Column('range_end', INTEGER(), nullable=True)

class Uploaded_Report_Records(db.Model):
    __tablename__  = 'Uploaded_Report_Records'
    id                          = Column('id', INTEGER(), primary_key=True, autoincrement=True)
    user_id                     = Column('user_id', INTEGER(), nullable=True)
    report_type                 = Column('report_type', TEXT(), nullable=True)
    number_of_new_records_added = Column('number_of_new_records_added', INTEGER(), nullable=True)
    date_uploaded               = Column('date_uploaded', INTEGER(), nullable=True)

class Device_Qty(db.Model):
    __tablename__ = 'Device_Qty'
    NLAD_Subscriber_ID         = Column('NLAD_Subscriber_ID', TEXT(), nullable=True, primary_key=True)
    Database                   = Column('Database', TEXT(), nullable=True)
    Duplicate                  = Column('Duplicate', TEXT(), nullable=True)
    List_Of_Customer_Order_IDs = Column('List_Of_Customer_Order_IDs', TEXT(), nullable=True)
    Total_Customer_Order_IDs   = Column('Total_Customer_Order_IDs', TEXT(), nullable=True)
    Number_Of_Devices          = Column('Number_Of_Devices', INTEGER(), nullable=True)
    Number_Of_Tablets          = Column('Number_Of_Tablets', INTEGER(), nullable=True)
    Number_Of_Phones           = Column('Number_Of_Phones', INTEGER(), nullable=True)
    Number_Of_Sims             = Column('Number_Of_Sims', INTEGER(), nullable=True)
    Number_Of_MDNs             = Column('Number_Of_MDNs', INTEGER(), nullable=True)
    Number_Of_Hotspots         = Column('Number_Of_Hotspots', INTEGER(), nullable=True)
    Date_Of_First_Device       = Column('Date_Of_First_Device', INTEGER(), nullable=True) 
    Date_Of_Last_Device        = Column('Date_Of_Last_Device', INTEGER(), nullable=True) 

class Device_Qty_Changes(db.Model):
    __tablename__ = 'Device_Qty_Changes'
    id                 = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    NLAD_Subscriber_ID = Column('NLAD_Subscriber_ID', TEXT(), nullable=True)
    Field_Name         = Column('Field_Name', TEXT(), nullable=True)
    New_Value          = Column('New_Value', TEXT(), nullable=True)
    Old_Value          = Column('Old_Value', TEXT(), nullable=True)
    Change_Date        = Column('Change_Date', INTEGER(), nullable=True)

class AgentSalesReports(db.Model):
    __tablename__ = 'AgentSalesReports'
    id               = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    Enrollment_ID    = Column('Enrollment_ID', TEXT(), nullable=True)
    Customer_ID      = Column('Customer_ID', TEXT(), nullable=True)
    Status           = Column('Status', TEXT(), nullable=True)
    Order_Date       = Column('Order_Date', INTEGER(), nullable=True)
    Activation_Date  = Column('Activation_Date', INTEGER(), nullable=True)
    Last_Action_Date = Column('Last_Action_Date', INTEGER(), nullable=True)
    Device_ID        = Column('Device_ID', TEXT(), nullable=True)
    RAD_ID           = Column('RAD_ID', TEXT(), nullable=True)
    Amount           = Column('Amount', TEXT(), nullable=True)
    Amount_Type      = Column('Amount_Type', TEXT(), nullable=True)
    Sim              = Column('Sim', TEXT(), nullable=True)
    Product          = Column('Product', TEXT(), nullable=True)
    Enrollment_Type  = Column('Enrollment_Type', TEXT(), nullable=True)
    Source           = Column('Source', TEXT(), nullable=True)
    Fulfillment      = Column('Fulfillment', TEXT(), nullable=True)
    Master           = Column('Master', TEXT(), nullable=True)
    Distributor      = Column('Distributor', TEXT(), nullable=True)
    Retailer         = Column('Retailer', TEXT(), nullable=True)
    Employee         = Column('Employee', TEXT(), nullable=True)

class Reimbursements(db.Model):
    __tablename__ = 'Reimbursements'
    id                  = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    Subscriber_ID       = Column('Subscriber_ID', TEXT(), nullable=True)
    Rate                = Column('Rate', TEXT(), nullable=True)
    Reason_Code         = Column('Reason_Code', TEXT(), nullable=True)
    Device_Benefit      = Column('Device_Benefit', TEXT(), nullable=True)
    SPIN                = Column('SPIN', TEXT(), nullable=True)
    SAC                 = Column('SAC', TEXT(), nullable=True)
    Last_Name           = Column('Last_Name', TEXT(), nullable=True)
    First_Name          = Column('First_Name', TEXT(), nullable=True)
    Street_Address      = Column('Street_Address', TEXT(), nullable=True)
    City                = Column('City', TEXT(), nullable=True)
    State               = Column('State', TEXT(), nullable=True)
    ZIP                 = Column('ZIP', TEXT(), nullable=True)
    Phone_Number        = Column('Phone_Number', TEXT(), nullable=True)
    ETC_General         = Column('ETC_General', TEXT(), nullable=True)
    Service_Type        = Column('Service_Type', TEXT(), nullable=True)
    Tribal_Benefit_Flag = Column('Tribal_Benefit_Flag', TEXT(), nullable=True)
    Eligible_For_Device = Column('Eligible_For_Device', TEXT(), nullable=True)
    Eligible_For_Rate   = Column('Eligible_For_Rate', TEXT(), nullable=True)

class Transfer_Outs(db.Model):
    __tablename__ = 'Transfer_Outs'
    id                         = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    TRANSACTION_DATE           = Column('TRANSACTION_DATE', TEXT(), nullable=True)
    TRANSACTION_TYPE           = Column('TRANSACTION_TYPE', TEXT(), nullable=True)
    TRANSACTION_EFFECTIVE_DATE = Column('TRANSACTION_EFFECTIVE_DATE', TEXT(), nullable=True)
    ENROLL_ID_CUST_ID          = Column('ENROLL_ID_CUST_ID', TEXT(), nullable=True)
    ENROLL_ID                  = Column('ENROLL_ID', TEXT(), nullable=True)
    CUST_ID                    = Column('CUST_ID', TEXT(), nullable=True)
    SAC                        = Column('SAC', TEXT(), nullable=True)
    ACCOUNT_TYPE               = Column('ACCOUNT_TYPE', TEXT(), nullable=True)
    MDN                        = Column('MDN', TEXT(), nullable=True)
    ALTERNATE_PHONE_NUMBER     = Column('ALTERNATE_PHONE_NUMBER', TEXT(), nullable=True)
    NAME                       = Column('NAME', TEXT(), nullable=True)
    ADDRESS                    = Column('ADDRESS', TEXT(), nullable=True)
    STATE                      = Column('STATE', TEXT(), nullable=True)
    SERVICE_INITIATION_DATE    = Column('SERVICE_INITIATION_DATE', TEXT(), nullable=True)
    PROGRAM                    = Column('PROGRAM', TEXT(), nullable=True)
    DE_ENROLL_STATUS           = Column('DE_ENROLL_STATUS', TEXT(), nullable=True)
    DE_ENROLL_DATETIME         = Column('DE_ENROLL_DATETIME', TEXT(), nullable=True)
    ETC_GENERAL_USE            = Column('ETC_GENERAL_USE', TEXT(), nullable=True)
    EMAIL                      = Column('EMAIL', TEXT(), nullable=True)
    IS_MISMATCH                = Column('IS_MISMATCH', TEXT(), nullable=True)
    CURRENT_STATUS             = Column('CURRENT_STATUS', TEXT(), nullable=True)
    RE_CONNECTION_DATE         = Column('RE_CONNECTION_DATE', TEXT(), nullable=True)
    WINBACK                    = Column('WINBACK', TEXT(), nullable=True)
    ACTIVATION_DATE            = Column('ACTIVATION_DATE', TEXT(), nullable=True)
    MASTER_AGENT               = Column('MASTER_AGENT', TEXT(), nullable=True)
    DISTRIBUTOR                = Column('DISTRIBUTOR', TEXT(), nullable=True)
    RETAILER                   = Column('RETAILER', TEXT(), nullable=True)
    EMPLOYEE                   = Column('EMPLOYEE', TEXT(), nullable=True)

class Merged_Customer_Databases(db.Model):
    __tablename__                    = 'Merged_Customer_Databases'
    # Below is mutual to telgoo and unavo
    Customer_Order_ID                = Column('Customer_Order_ID', TEXT(), primary_key=True)
    Source_Database                  = Column('Source_Database', TEXT(), nullable=True)
    NLAD_Subscriber_ID               = Column('NLAD_Subscriber_ID', TEXT(), nullable=True)
    Group_ID                         = Column('Group_ID', INTEGER(), nullable=True)
    CLEC                             = Column('CLEC', TEXT(), nullable=True)
    Created_Date                     = Column('Created_Date', INTEGER(), nullable=True)
    Order_Date                       = Column('Order_Date', INTEGER(), nullable=True)
    Activation_Date                  = Column('Activation_Date', INTEGER(), nullable=True)
    Qualifying_Program               = Column('Qualifying_Program', TEXT(), nullable=True)
    Account_Status                   = Column('Account_Status', TEXT(), nullable=True)
    ACP_Status                       = Column('ACP_Status', TEXT(), nullable=True)
    Deactivation_Date                = Column('Deactivation_Date', INTEGER(), nullable=True)
    Misc_Customer_ID                 = Column('Misc_Customer_ID', TEXT(), nullable=True)
    First_Name                       = Column('First_Name', TEXT(), nullable=True)
    Last_Name                        = Column('Last_Name', TEXT(), nullable=True)
    Address1                         = Column('Address1', TEXT(), nullable=True)
    Address2                         = Column('Address2', TEXT(), nullable=True)
    City                             = Column('City', TEXT(), nullable=True)
    State                            = Column('State', TEXT(), nullable=True)
    Zip                              = Column('Zip', TEXT(), nullable=True)
    Phone1                           = Column('Phone1', TEXT(), nullable=True)
    SSN                              = Column('SSN', TEXT(), nullable=True)
    DOB                              = Column('DOB', INTEGER(), nullable=True)
    Email                            = Column('Email', TEXT(), nullable=True)
    Service_Carrier                  = Column('Service_Carrier', TEXT(), nullable=True)
    Device_Mfg                       = Column('Device_Mfg', TEXT(), nullable=True)
    Device_Model_Number              = Column('Device_Model_Number', TEXT(), nullable=True)
    Device_Type                      = Column('Device_Type', TEXT(), nullable=True)
    IMEI                             = Column('IMEI', TEXT(), nullable=True)
    ESN                              = Column('ESN', TEXT(), nullable=True)
    MDN                              = Column('MDN', TEXT(), nullable=True)
    Plan                             = Column('Plan', TEXT(), nullable=True)
    Last_Used_Data                   = Column('Last_Used_Data', INTEGER(), nullable=True)
    Last_Used_Phone                  = Column('Last_Used_Phone', INTEGER(), nullable=True)
    MASTER_AGENT_NAME                = Column('MASTER_AGENT_NAME', TEXT(), nullable=True)
    DISTRIBUTOR_AGENT_NAME           = Column('DISTRIBUTOR_AGENT_NAME', TEXT(), nullable=True)
    RETAILER_AGENT_NAME              = Column('RETAILER_AGENT_NAME', TEXT(), nullable=True)
    Agent                            = Column('Agent', TEXT(), nullable=True)
    Agent_LoginID                    = Column('Agent_LoginID', TEXT(), nullable=True)
    # Below is unique to unavo
    MSID                             = Column('MSID', TEXT(), nullable=True)
    CSA                              = Column('CSA', TEXT(), nullable=True)
    TrackingNum                      = Column('TrackingNum', TEXT(), nullable=True)
    DeviceId                         = Column('DeviceId', TEXT(), nullable=True)
    NladProgram                      = Column('NladProgram', TEXT(), nullable=True)
    LifelineSubscriberId             = Column('LifelineSubscriberId', TEXT(), nullable=True)
    LifelineStatus                   = Column('LifelineStatus', TEXT(), nullable=True)
    NladErrorType                    = Column('NladErrorType', TEXT(), nullable=True)
    IsAcp                            = Column('IsAcp', TEXT(), nullable=True)
    PORTSTATUS                       = Column('PORTSTATUS', TEXT(), nullable=True)
    IpAddress                        = Column('IpAddress', TEXT(), nullable=True)
    IsProofOfBenefitsUploaded        = Column('IsProofOfBenefitsUploaded', TEXT(), nullable=True)
    IsIdentityProofUploaded          = Column('IsIdentityProofUploaded', TEXT(), nullable=True)
    IsTribal                         = Column('IsTribal', TEXT(), nullable=True)
    TribalID                         = Column('TribalID', TEXT(), nullable=True)
    National_Verifier_Application_ID = Column('National_Verifier_Application_ID', TEXT(), nullable=True)
    AgentFollowupPeriod              = Column('AgentFollowupPeriod', TEXT(), nullable=True)
    FollowupPeriodCreatedAuthor      = Column('FollowupPeriodCreatedAuthor', TEXT(), nullable=True)
    FCC_CheckBox                     = Column('FCC_CheckBox', TEXT(), nullable=True)
    Software_Consent                 = Column('Software_Consent', TEXT(), nullable=True)
    Customer_Price                   = Column('Customer_Price', TEXT(), nullable=True)
    ConsentCheck                     = Column('ConsentCheck', TEXT(), nullable=True)
    ShipmentDate                     = Column('ShipmentDate', INTEGER(), nullable=True)
    deviceReimbursementDate          = Column('deviceReimbursementDate', INTEGER(), nullable=True)
    FollowupPeriod_CreatedDate       = Column('FollowupPeriod_CreatedDate', INTEGER(), nullable=True)
    # Below is unique to telgoo
    IS_HOUSEHOLD                     = Column('IS_HOUSEHOLD', TEXT(), nullable=True)
    IS_TRIBAL                        = Column('IS_TRIBAL', TEXT(), nullable=True)
    IS_SHELTER                       = Column('IS_SHELTER', TEXT(), nullable=True)
    IS_STATE_DB                      = Column('IS_STATE_DB', TEXT(), nullable=True)
    STATE_DB_MATCHED                 = Column('STATE_DB_MATCHED', TEXT(), nullable=True)
    RECORD_KEEPING                   = Column('RECORD_KEEPING', TEXT(), nullable=True)
    ORDER_STATUS                     = Column('ORDER_STATUS', TEXT(), nullable=True)
    TABLET_SUBSIDY_QUALIFICATION     = Column('TABLET_SUBSIDY_QUALIFICATION', TEXT(), nullable=True)
    SOURCE                           = Column('SOURCE', TEXT(), nullable=True)
    AUTHORIZE_CODE                   = Column('AUTHORIZE_CODE', TEXT(), nullable=True)
    DL_NUMBER                        = Column('DL_NUMBER', TEXT(), nullable=True)
    WINBACK                          = Column('WINBACK', TEXT(), nullable=True)
    PAYMENT_TYPE                     = Column('PAYMENT_TYPE', TEXT(), nullable=True)
    SPONSOR_ID                       = Column('SPONSOR_ID', TEXT(), nullable=True)
    CURRENT_APS                      = Column('CURRENT_APS', TEXT(), nullable=True)
    BQP_FIRST_NAME                   = Column('BQP_FIRST_NAME', TEXT(), nullable=True)
    BQP_LAST_NAME                    = Column('BQP_LAST_NAME', TEXT(), nullable=True)
    PORTIN_STATUS                    = Column('PORTIN_STATUS', TEXT(), nullable=True)
    NLAD_ENROLLMENT_TYPE             = Column('NLAD_ENROLLMENT_TYPE', TEXT(), nullable=True)
    ENROLLMENT_TYPE                  = Column('ENROLLMENT_TYPE', TEXT(), nullable=True)
    CONSENT_FORM_AVAILABE            = Column('CONSENT_FORM_AVAILABE', TEXT(), nullable=True)
    ID_PROOF_AVAILABE                = Column('ID_PROOF_AVAILABE', TEXT(), nullable=True)
    DEVICE_EXPECTED_RATE             = Column('DEVICE_EXPECTED_RATE', TEXT(), nullable=True)
    REVIEWED_BY                      = Column('REVIEWED_BY', TEXT(), nullable=True)
    SAME_MONTH_DISCONNECTION         = Column('SAME_MONTH_DISCONNECTION', TEXT(), nullable=True)
    ACTIVE_PERIOD                    = Column('ACTIVE_PERIOD', TEXT(), nullable=True)
    BQP_BIRTH_DATE                   = Column('BQP_BIRTH_DATE', INTEGER(), nullable=True)
    DEVICE_REIMBURSEMENT__DATE       = Column('DEVICE_REIMBURSEMENT__DATE', INTEGER(), nullable=True)
    MEDICAID_ID                      = Column('MEDICAID_ID', TEXT(), nullable=True)

class Merged_Database_Changes(db.Model):
    __tablename__      = 'Merged_Database_Changes'
    id                 = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    Source_Database    = Column('Source_Database', TEXT(), nullable=True)
    Customer_Order_ID  = Column('Customer_Order_ID', TEXT(), nullable=True)
    Nlad_Subscriber_ID = Column('Nlad_Subscriber_ID', TEXT(), nullable=True)
    Field_Name         = Column('Field_Name', TEXT(), nullable=True)
    New_Value          = Column('New_Value', TEXT(), nullable=True)
    Old_Value          = Column('Old_Value', TEXT(), nullable=True)
    Change_Date        = Column('Change_Date', INTEGER(), nullable=True)
    Activation_Date    = Column('Activation_Date', INTEGER(), nullable=True)
    CLEC               = Column('CLEC', TEXT(), nullable=True)
    Plan               = Column('Plan', TEXT(), nullable=True)
    Agent              = Column('Agent', TEXT(), nullable=True)
    Account_Status     = Column('Account_Status', TEXT(), nullable=True)
    ACP_Status         = Column('ACP_Status', TEXT(), nullable=True)
    MDN                = Column('MDN', TEXT(), nullable=True)

class Status_Report_Counts(db.Model):
    __tablename__      = 'Status_Report_Counts'
    id                 = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    date               = Column('date', INTEGER(), nullable=True)
    field              = Column('field', TEXT(), nullable=True)
    count              = Column('count', INTEGER(), nullable=True)

class Master_Agent_And_Agent_Active_Counts(db.Model):
    __tablename__      = 'Master_Agent_And_Agent_Active_Counts'
    id                 = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    date               = Column('date', INTEGER(), nullable=True)
    name               = Column('name', TEXT(), nullable=True)
    count              = Column('count', INTEGER(), nullable=True)
    type               = Column('type', TEXT(), nullable=True)

class Staging_Data(db.Model):
    __tablename__                    = 'Staging_Data'
    id                               = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    report_type                      = Column('report_type', TEXT(), nullable=True)
    text_field_one                   = Column('text_field_one', TEXT(), nullable=True)
    text_field_two                   = Column('text_field_two', TEXT(), nullable=True)
    text_field_three                 = Column('text_field_three', TEXT(), nullable=True)
    integer_field_one                = Column('integer_field_one', INTEGER(), nullable=True)
    integer_field_two                = Column('integer_field_two', INTEGER(), nullable=True)
    integer_field_three              = Column('integer_field_three', INTEGER(), nullable=True)
    integer_field_four               = Column('integer_field_four', INTEGER(), nullable=True)
    integer_field_five               = Column('integer_field_five', INTEGER(), nullable=True)
    integer_field_six                = Column('integer_field_six', INTEGER(), nullable=True)
    integer_field_seven              = Column('integer_field_seven', INTEGER(), nullable=True)
    integer_field_eight              = Column('integer_field_eight', INTEGER(), nullable=True)
    integer_field_nine               = Column('integer_field_nine', INTEGER(), nullable=True)
    integer_field_ten                = Column('integer_field_ten', INTEGER(), nullable=True)

class Plan_Assignment(db.Model):
    __tablename__      = 'Plan_Assignment'
    id                 = Column('id', INTEGER(), autoincrement=True, primary_key=True)
    source_database    = Column('source_database', TEXT(), nullable=True)
    plan_name          = Column('plan_name', TEXT(), nullable=True)
    program_assignment = Column('program_assignment', TEXT(), nullable=True)
    
