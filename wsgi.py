from app import create_app


app, celery = create_app()

# if __name__ == '__main__':
#     with app.app_context():
#         app.run(debug=False)

# celery command to start worker: celery -A wsgi.celery worker --loglevel=info --concurrency=1