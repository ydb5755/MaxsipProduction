from app import db
from app.users import users
from app.models import User, User_Generated_Reports, Uploaded_Report_Records
from app.forms import LoginForm, RequestResetForm, ResetPasswordForm, EditUserForm, AddUserForm
from app.main.utils import send_reset_email
from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, login_user, current_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime
import os
import logging
logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.INFO)


@users.route('/user_page/<user_id>')
@login_required
def user_page(user_id): 
    if not int(current_user.id) == int(user_id):
        logging.warning(f'User with Id:{current_user.id} is trying to access user with id={user_id}')
        return redirect(url_for('main.dashboard')) 
    user = User.query.filter_by(id=user_id).first()
    user_reports = User_Generated_Reports.query.filter_by(user_id=user_id).order_by(User_Generated_Reports.time_created.desc()).all()
    for report in user_reports:
        if report.time_created:
            #########################################################################################################################
            ######### This is a manual fix that does not account for daylight savings changing and only shows eastern time  #########
            ##################### This needs to be changed to account for timezone on the front end  ################################
            #################################### THIS IS NOT A LONG TERM SOLUTION  ##################################################
            #########################################################################################################################
            report.time_created = datetime.fromtimestamp(float(report.time_created)-14400).strftime('%Y-%m-%d %H:%M')
        if report.range_start:
            report.range_start = datetime.fromtimestamp(float(report.range_start)).strftime('%Y-%m-%d')
        if report.range_end:
            report.range_end = datetime.fromtimestamp(float(report.range_end)).strftime('%Y-%m-%d')
        if report.path:
            _, filename = os.path.split(report.path)
            report.path = filename
    return render_template('user_page.html', 
                           user=user,
                           user_reports=user_reports)

@users.route('/')
@users.route('/login', methods=('GET', 'POST'))
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if not user or not check_password_hash(user.password, form.password.data):
            flash('Please check your login details and try again.', 'bad')
            return redirect(url_for('users.login'))
        login_user(user, remember=form.remember.data)
        flash("You've been logged in successfully!", 'good')
        next_page = request.args.get('next')
        return redirect(next_page) if next_page else redirect(url_for('main.dashboard'))
    return render_template('login.html', 
                           form=form)

@users.route('/reset_password', methods=('GET', 'POST'))
def reset_request():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = RequestResetForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user:
            send_reset_email(user)
        flash('An email has been sent with instructions to reset your password', 'good')
        return redirect(url_for('users.login'))
    return render_template('reset_request.html', 
                           form=form)

@users.route('/reset_password/<token>', methods=('GET', 'POST'))
def reset_token(token):
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    user = User.verify_reset_token(token)
    if user is None:
        flash('That is an invalid or expired token', 'bad')
        return redirect(url_for('main.reset_request'))
    form = ResetPasswordForm()
    if form.validate_on_submit():
        hashed_password = generate_password_hash(form.password.data)
        if user:
            user.password = hashed_password
        db.session.commit()
        flash('Your password has been updated! You are now able to log in', 'good')
        return redirect(url_for('users.login'))
    return render_template('reset_token.html', 
                           form=form)

@users.route('/delete_user/<user_id>')
@login_required
def delete_user(user_id):
    if not current_user.user_type == 'Admin':
        return redirect(url_for('main.dashboard'))
    user = User.query.filter_by(id=user_id).first()
    db.session.delete(user)
    db.session.commit()
    return redirect(url_for('users.admin'))

@users.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You\'ve been successfully logged out!')
    return redirect(url_for('users.login'))

@users.route('/admin', methods=('GET', 'POST'))
@login_required
def admin():
    if not current_user.user_type == 'Admin':
        return redirect(url_for('main.dashboard'))
    users = User.query.all()
    return render_template('admin.html', users=users)

@users.route('/add_user_form', methods=('GET', 'POST'))
@login_required
def add_user_form():
    if not current_user.user_type == 'Admin':
        return redirect(url_for('main.dashboard'))
    form = AddUserForm()
    if form.validate_on_submit():
        user = User(
            first_name=form.first_name.data,
            last_name=form.last_name.data,
            email=form.email.data,
            password=generate_password_hash(form.password.data),
            user_type=form.user_type.data,
            admin=False
        )
        db.session.add(user)
        db.session.commit()
        flash('User Added')
        return redirect(url_for('users.admin'))
    return render_template('add_user_form.html', form=form)

@users.route('/edit_profile/<user_id>', methods=('GET', 'POST'))
@login_required
def edit_profile(user_id):
    if not current_user.user_type == 'Admin':
        return redirect(url_for('main.dashboard'))
    user = User.query.filter_by(id=user_id).first()
    form = EditUserForm(obj=user)
    if form.validate_on_submit():
        User.query.filter_by(id=user_id).update({
            User.user_type:form.user_type.data,
            User.email:form.email.data,
            User.first_name:form.first_name.data,
            User.last_name:form.last_name.data,}, synchronize_session=False)
        db.session.commit()
        return redirect(url_for('users.admin'))
    return render_template('edit_profile.html', form=form, user=user)

@users.route('/test_sub', subdomain='<user>')
def test_sub(user):
    return f'<h1>User: {user}</h1>'