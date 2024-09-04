from datetime import datetime
import shutil
import os

def make_backup_of_db():
    shutil.copyfile('/home/ubuntu/MaxsipReports/instance/site.db', 
                    f'/home/ubuntu/MaxsipReports/pythonFiles/DatabaseBackups/{datetime.today().strftime("%m-%d-%Y")}_backup.db')
    directory = '/home/ubuntu/MaxsipReports/pythonFiles/DatabaseBackups/'
    dir_files = os.listdir(directory)
    dir_files.sort(key=lambda x: os.path.getmtime(f'{directory}/{x}'))
    for file in dir_files[:-30]:
        os.remove(f'{directory}/{file}')
