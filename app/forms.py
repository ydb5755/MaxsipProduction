from flask_wtf import FlaskForm
from wtforms import StringField, \
                    EmailField, \
                    PasswordField, \
                    SubmitField, \
                    SelectField, \
                    BooleanField,\
                    DateField
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms.validators import DataRequired, ValidationError, NumberRange, EqualTo, Email
from app.models import User
from flask_login import current_user
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.DEBUG)

class LoginForm(FlaskForm):
    email        = EmailField('Email', validators=[DataRequired()])
    password     = PasswordField('Password', validators=[DataRequired()])
    remember     = BooleanField('Remember me')
    submit       = SubmitField('Login')

class RequestResetForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    submit = SubmitField('Request Password Reset')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is None:
            raise ValidationError ('There is no account with that email. Speak to the admin.')
    
class ResetPasswordForm(FlaskForm):
    password = PasswordField('Password', 
                        validators=[DataRequired()])
    confirm_password = PasswordField('Confirm Password', 
                        validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Reset Password')

class TelgooUploadForm(FlaskForm):
    file = FileField('Upload File',
                     validators=[FileRequired(), FileAllowed(['csv'], 'CSV Only!')])
    file_type = SelectField('What file are you uploading?', choices=['NTOR Report'], validators=[DataRequired()]) # , 'ASR Report', 'DSR Report'
    date_of_file = DateField('What date is the file for?', validators=[DataRequired()])
    submit = SubmitField('Submit')

class AddUserForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    first_name = StringField('First Name', validators=[DataRequired()])
    last_name = StringField('Last Name', validators=[DataRequired()])
    password  = StringField('Password', validators=[DataRequired()])
    confirm_password  = StringField('Confirm Password', validators=[DataRequired(), EqualTo('password', message='Passwords must match')])
    user_type = SelectField('User Type', choices=['Admin', 'Manager', 'User'])
    submit = SubmitField('Add User')

class EditUserForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    first_name = StringField('First Name', validators=[DataRequired()])
    last_name = StringField('Last Name', validators=[DataRequired()])
    user_type = SelectField('User Type', choices=['Admin', 'Manager', 'User'])
    submit = SubmitField('Save Changes')

class CustomerSearchForm(FlaskForm):
    enroll_or_sub = SelectField('ID Type', choices=['Customer Order ID', 'NLAD Subscriber ID'])
    id = StringField('ID', validators=[DataRequired()])
    submit = SubmitField('Search')

class DateRangeReportForm(FlaskForm):
    start_date = DateField('Start Date', validators=[DataRequired()])
    end_date   = DateField('End Date', validators=[DataRequired()])
    submit     = SubmitField('Generate')

    def validate_start_date(self, start_date):
        if start_date.data > self.end_date.data:
            raise ValidationError('Start date needs to be before the end date')
        
    def validate_end_date(self, end_date):
        if self.start_date.data > end_date.data:
            raise ValidationError('End date needs to be later than the start date')

class SingleDateReportForm(FlaskForm):
    date_requested = DateField('Date', validators=[DataRequired()])
    submit         = SubmitField('Generate')

class JustSubmitForm(FlaskForm):
    submit = SubmitField('Run Report')

