from flask import Blueprint

reports = Blueprint('reports', 
                     __name__, 
                     template_folder='templates', 
                     static_folder='static',
                     url_prefix='/reports')

from app.reports import routes