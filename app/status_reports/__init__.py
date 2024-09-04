from flask import Blueprint

status_reports = Blueprint('status_reports', 
                     __name__, 
                     template_folder='templates', 
                     static_folder='static',
                     url_prefix='/status_reports')

from app.status_reports import routes