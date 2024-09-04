from flask import Blueprint

agent_reports = Blueprint('agent_reports', 
                     __name__, 
                     template_folder='templates', 
                     static_folder='static',
                     url_prefix='/agent_reports')

from app.agent_reports import routes