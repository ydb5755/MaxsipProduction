from flask import Blueprint

plan_management = Blueprint('plan_management', 
                     __name__, 
                     template_folder='templates', 
                     static_folder='static',
                     url_prefix='/plan_management')

from app.plan_management import routes