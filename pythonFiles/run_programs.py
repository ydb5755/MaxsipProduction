from daily_programs.new_telgoo_update import run_file as telgoo_run
from daily_programs.new_terracom_update import run_file as terracom_run
from daily_programs.new_unavo_update import run_program as unavo_run
from daily_programs.grouping import adding_groups_to_contacts_take_four
from daily_programs.dqs_update import dqs_update_script
from daily_programs.status_reports import run_updates as status_report_updates
from daily_programs.master_agent_and_agent_counts import run_updates as agent_updates
from daily_programs.daily_backup import make_backup_of_db
from datetime import datetime
from time import sleep

if __name__ == '__main__':
    while True:
        if datetime.now().hour == 15:
            unavo_run()
            terracom_run()
            telgoo_run()
            adding_groups_to_contacts_take_four()
            dqs_update_script()
            status_report_updates()
            agent_updates()
            make_backup_of_db()
            print('Files have been processed for today, sleeping for an hour')
            sleep(3600)
        else:
            print(f'The time is now: {datetime.now()}. It is not yet time to run the programs')
            sleep(3600)