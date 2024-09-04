from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField
from wtforms.validators import DataRequired
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.DEBUG)


class EditProgramNameForm(FlaskForm):
    id           = IntegerField('id', validators=[DataRequired()])
    program_name = StringField('Name', validators=[DataRequired()])
    program_code = StringField('Code', validators=[DataRequired()])
    submit       = SubmitField('Submit Change')