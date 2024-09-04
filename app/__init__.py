from flask import Flask, redirect, url_for
from celery import Celery
from app.config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager
from flask_mail import Mail
from werkzeug.middleware.proxy_fix import ProxyFix
from logging import FileHandler, INFO

db = SQLAlchemy()
migrate = Migrate()
login_manager = LoginManager()
mail = Mail()
celery = Celery('app', broker=Config.CELERY_BROKER_URL, backend=Config.CELERY_RESULT_BACKEND)


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    celery.conf.update(app.config)

    db.init_app(app)
    migrate.init_app(app,db)
    login_manager.init_app(app)
    mail.init_app(app)
    file_handler = FileHandler('errorlog.txt')
    file_handler.setLevel(INFO)
    app.logger.addHandler(file_handler)
    app.wsgi_app = ProxyFix(
        app.wsgi_app, x_for=1, x_proto=1
    )
    
    from app.models import User
    @login_manager.user_loader
    def load_user(user_id):
        user = User.query.get(user_id)
        if user:
            return user
        else:
            return None
    login_manager.login_view = 'users.login'

    from app.users import users
    from app.main import main
    from app.reports import reports
    from app.plan_management import plan_management
    from app.agent_reports import agent_reports
    from app.status_reports import status_reports
    app.register_blueprint(users)
    app.register_blueprint(main)
    app.register_blueprint(reports)
    app.register_blueprint(plan_management)
    app.register_blueprint(agent_reports)
    app.register_blueprint(status_reports)
    
    @app.route('/')
    def reroute_base_url():
        return redirect(url_for('main.dashboard'))
    return app, celery

