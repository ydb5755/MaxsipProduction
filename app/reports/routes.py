from app import db
from app.reports import reports
from app.models import User, User_Generated_Reports, Uploaded_Report_Records
from app.forms import TelgooUploadForm, DateRangeReportForm, SingleDateReportForm
from app.celery_tasks import addingsASR, adding_Transfer_Outs, dropship_report, ntor_generation, general_report_generator, trx_out_report_employee_history_generator
from app.reports.utils import check_columns_front_end_asr_report, check_columns_front_end_ntor_report, \
                                check_columns_front_end_dsr_report, activation_groups_with_agents
from werkzeug.utils import secure_filename
import os
from datetime import datetime
from flask import redirect, render_template, url_for, flash, send_from_directory
from flask_login import login_required, current_user


@reports.route('/telgoo_files', methods=('GET', 'POST'))
@login_required
def telgoo_files():
    if not current_user.user_type in ['Admin', 'Manager']:
        return redirect(url_for('main.dashboard'))
    form = TelgooUploadForm()
    result = ''
    if form.validate_on_submit():
        filename = secure_filename(form.file.data.filename)
        file_path = '/home/ubuntu/MaxsipReports/app/main/static/uploads/' + filename
        form.file.data.save(file_path)
        if form.file_type.data == 'ASR Report':
            if check_columns_front_end_asr_report(file_path):
                addingsASR.delay(file_path, int(current_user.id), form.file_type.data)
                result = f'The file named "{filename}" has been uploaded and is being processed now'
            else:
                os.remove(file_path)
                result = f'The file named "{filename}" did not pass the column check and was not uploaded'
        elif form.file_type.data == 'NTOR Report':
            if check_columns_front_end_ntor_report(file_path):
                adding_Transfer_Outs.delay(file_path, int(current_user.id), form.file_type.data)
                result = f'The file named "{filename}" has been uploaded and is being processed now'
            else:
                os.remove(file_path)
                result = f'The file named "{filename}" did not pass the column check and was not uploaded'
        elif form.file_type.data == 'DSR Report':
            if check_columns_front_end_dsr_report(file_path):
                dropship_report.delay(file_path, int(current_user.id), filename)
                result = f'The file named "{filename}" has been uploaded and is being processed now'
            else:
                os.remove(file_path)
                result = f'The file named "{filename}" did not pass the column check and was not uploaded'
    return render_template('telgoo_files.html',
                           form=form,
                           result=result)

@reports.route('/uploaded_report_records')
@login_required
def uploaded_report_records():
    if not current_user.user_type in ['Admin', 'Manager']:
        return redirect(url_for('main.dashboard'))
    unwriteable_records = Uploaded_Report_Records.query.order_by(Uploaded_Report_Records.id.desc()).limit(20)
    records = []
    for record in unwriteable_records:
        replacement_record = {}
        replacement_record['id'] = record.id
        replacement_record['user_id'] = User.query.filter_by(id=record.user_id).first().email
        replacement_record['report_type'] = record.report_type
        replacement_record['number_of_new_records_added'] = record.number_of_new_records_added
        replacement_record['date_uploaded'] = datetime.fromtimestamp(float(record.date_uploaded)).strftime('%Y-%m-%d')
        records.append(replacement_record)
    return render_template('uploaded_report_records.html',
                           records=records)
    
@reports.route('/delete_report/<report_id>')
@login_required
def delete_report(report_id):
    report = User_Generated_Reports.query.filter_by(id=report_id).first()
    if int(report.user_id) != int(current_user.id):
        flash('Please dont try to delete files that arent yours')
        return redirect(url_for('main.dashboard'))
    os.remove(report.path)
    db.session.delete(report)
    db.session.commit()
    return redirect(url_for('users.user_page', user_id=current_user.id))

@reports.route('/download_report/<user_id>/<file>')
@login_required
def download_report(user_id, file):
    directory = f'/home/ubuntu/MaxsipReports/app/main/static/User_Reports/{str(user_id)}'
    return send_from_directory(directory, file)

@reports.route('/transfer_out_report', methods=('GET', 'POST'))
@login_required
def transfer_out_report():
    form = DateRangeReportForm()
    response = ''
    if form.validate_on_submit():
        ntor_generation.delay(form.start_date.data, form.end_date.data, current_user.id)
        response = f'A report of the date {form.start_date.data}-{form.end_date.data} will appear on your user page with a downloadable link as soon as its ready'
    return render_template('transfer_out_report.html',
                           form=form,
                           response=response)

@reports.route('/activation_groups', methods=('GET', 'POST'))
@login_required
def activation_groups():
    form = DateRangeReportForm()
    data = []
    headers = []
    response = ''
    if form.validate_on_submit():
        data, headers = activation_groups_with_agents(form.start_date.data, form.end_date.data, current_user.id)
        response = f'A report of the date {form.start_date.data}-{form.end_date.data} will appear on your user page with a downloadable link as soon as its ready'
    return render_template('activation_groups_with_agents.html',
                           form=form,
                           response=response,
                           headers=headers,
                           data=data)

@reports.route('/general_report', methods=('GET', 'POST'))
@login_required
def general_report():
    form = SingleDateReportForm()
    response = ''
    if form.validate_on_submit():
        general_report_generator.delay(form.date_requested.data, current_user.id)
        response = f'A report of the date {form.date_requested.data} will appear on your user page with a downloadable link as soon as its ready'
    return render_template('general_report.html',
                           form=form,
                           response=response)

@reports.route('/trx_out_report_employee_history', methods=('GET', 'POST'))
@login_required
def trx_out_report_employee_history():
    form = SingleDateReportForm()
    response = ''
    if form.validate_on_submit():
        trx_out_report_employee_history_generator.delay(form.date_requested.data, current_user.id)
        response = f'A report of the date {form.date_requested.data} will appear on your user page with a downloadable link as soon as its ready'
    return render_template('trx_out_report_employee_history.html',
                           form=form,
                           response=response)

@reports.route('/report_generators')
@login_required
def report_generators():
    return render_template('report_generators.html')

