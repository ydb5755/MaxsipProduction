import os
import json

with open('/etc/config.json') as config_file:
	config = json.load(config_file)


class Config():
    SQLALCHEMY_DATABASE_URI = config.get('SQLALCHEMY_DATABASE_URI_SQLITE')
    SECRET_KEY              = config.get('SECRET_KEY')
    MAIL_PORT               = 587
    MAIL_USE_TLS            = True
    MAX_CONTENT_LENGTH      = 500 * 1000 * 1000
    CELERY_BROKER_URL       = 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND   = 'redis://localhost:6379/0'
    CELERY_IMPORTS          = ('app.celery_tasks', )
    SMTP_USER               = config.get('SMTP_USER')
    SMTP_PASSWORD           = config.get('SMTP_PASSWORD')
    # SERVER_NAME             = 'maxsipsupport.com'
