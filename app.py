

from flask import Flask, render_template, jsonify, request
from flask import Flask, render_template, request, session, redirect, url_for, flash, send_from_directory, get_flashed_messages
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from flask_wtf.csrf import CSRFProtect
from flask import make_response
import plotly.graph_objects as go
import plotly as plotly
from functionality.user import User
from functionality.database import database
from functionality.result_updater import result_updater
from functionality.util import american_to_decimal, decimal_to_american
import pandas as pd
import json
from datetime import timedelta
from datetime import datetime
import os
import stripe
import time
import atexit
from functionality.db_manager import DBManager
from functionality.models import LoginInfo, UserFilters, UserArbitrageFilters, UserPregameFilters, UserAIEVFilters, UserLoginTimes
from functionality.models import VerificationCode
import warnings
warnings.filterwarnings("ignore")
from flask_socketio import SocketIO
from chat import Chat
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from threading import Thread
import time
import pandas as pd
import flock as flock
from flask_cors import CORS
import redis
from sqlalchemy import desc
import psutil
from functools import wraps


process = psutil.Process(os.getpid())

redis_client = redis.Redis(host='localhost', port=6379, db=0)

from flask_compress import Compress
# from functionality.routes.api import api_bp
from chat_test import Chat_test
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# from amplitude import Amplitude
# from amplitude import BaseEvent


def create_app():
    app = Flask(__name__, template_folder='static/templates', static_folder='static')
    CORS(app, supports_credentials=True)
    Compress(app)
    # TODO: Put this key in the secret file
    app.secret_key = 'to_the_moon'
    app.db_manager = DBManager()
    app.db = database(app.db_manager)
    
    # app.chat = Chat()
    app.chat = Chat_test(redis_client)

    from functionality.routes.api import api
    app.register_blueprint(api)

    return app


app = create_app()
app.config['REACT_COMPONENT_DIRECTORY'] = os.path.join(app.root_path, 'react_frontend')
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='None'
)

socketio = SocketIO(app)


def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")


@app.route('/api/login_status')
def login_status():
    is_logged_in = True if 'user_id' in session else False
    return jsonify({"login_status": is_logged_in})


app.config['STRIPE_PUBLIC_KEY'] = 'pk_live_51Nm0vBHM5Jv8uc5M5hu3bxlKg6soYb2v9xSg5O7a9sXi6JQJpl7nPWiNKrNHGlXf5g8PFnN6sn0wcLOrixvxF8VH00nVoyGtCk'

app.config['STRIPE_PRIVATE_KEY'] = password = os.environ.get("STRIPE_API_KEY")

stripe.api_key = app.config['STRIPE_PRIVATE_KEY']

@atexit.register
def close_db():
    app.db_manager.close()




@app.route('/api/checkout/<string:price_id>')
def create_checkout_session(price_id):
    if price_id == "price_1OZhXvHM5Jv8uc5MyZO28LI1":
        price = 15
    elif price_id == "price_1OZhdhHM5Jv8uc5MaLORELDu":
        price = 50
    else:
        price = 20
    trakdesk_cid = request.args.get('client_reference_id')
    utm_source = session.get('utm_source')
    utm_campaign = session.get('utm_campaign')

    checkout_session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        allow_promotion_codes=True,
        line_items=[
            {
                'price': price_id,
                'quantity': 1,
            },
        ],
        mode='subscription',
        client_reference_id=trakdesk_cid,
        success_url=url_for('success_checkout', _external=True) + '?session_id={CHECKOUT_SESSION_ID}' + f'&price={price}' + f'&utm_source={utm_source}' + f'&utm_campaign={utm_campaign}',
        cancel_url="https://smartbettor.ai"
    )
    return redirect(checkout_session.url,code=302)


@app.route('/api/checkout_free_trial/<string:price_id>')
def create_checkout_session_free_trial(price_id):
    if price_id == "price_1OZhXvHM5Jv8uc5MyZO28LI1":
        price = 15
    elif price_id == "price_1OZhdhHM5Jv8uc5MaLORELDu":
        price = 50
    else:
        price = 20
    trial_end_date = int((datetime.utcnow() + timedelta(days=8)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    trakdesk_cid = request.args.get('client_reference_id')
    checkout_session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        allow_promotion_codes=True,
        line_items=[
            {
                'price': price_id,
                'quantity': 1,
            },
        ],
        mode='subscription',
        client_reference_id=trakdesk_cid,
        success_url=url_for('free_success_checkout', _external=True) + '?session_id={CHECKOUT_SESSION_ID}' + f'&price={price}',
        cancel_url="https://smartbettor.ai",
        subscription_data = {
             'trial_end': trial_end_date
        }
    )
    return redirect(checkout_session.url,code=302)


@app.route('/api/free_success_checkout')
def free_success_checkout():
    checkout_session_id = request.args.get('session_id')
    price = request.args.get('price')
    checkout_session = stripe.checkout.Session.retrieve(checkout_session_id)
    customer_email = checkout_session['customer_details']['email']

    return redirect(f'https://smartbettor.ai/register?session_id={checkout_session_id}&price={price}&customer_email={customer_email}')


@app.route('/api/success_checkout')
def success_checkout():
    checkout_session_id = request.args.get('session_id')
    price = request.args.get('price')
    utm_source = request.args.get('utm_source')
    utm_campaign = request.args.get('utm_campaign')
    checkout_session = stripe.checkout.Session.retrieve(checkout_session_id)
    customer_email = checkout_session['customer_details']['email']
    
    return redirect(f'https://smartbettor.ai/register?session_id={checkout_session_id}&price={price}&customer_email={customer_email}&utm_source={utm_source}&utm_campaign={utm_campaign}')


@app.route('/api/get_arbitrage_dash_data', methods=['POST'])
def get_arbitrage_dash_data():

    data = request.get_json()
    
    filters = data.get('filters', '')

    try:
        bankroll = app.db.get_user_bank_roll(session["user_id"])
    except KeyError:
        bankroll = 5000

    df = read_cached_df('arb_dash_cache')

    data = app.db.get_arbitrage_dash_data_react(filters, bankroll, df)
    
    if data.empty:
        data = pd.DataFrame(columns=['bankroll', 'update'])
        data = data.append({'bankroll': bankroll, 'update': False}, ignore_index=True)
    else:
        data['bankroll'] = bankroll
    data_json = data.to_dict(orient='records')

    return jsonify(data_json)



@app.route('/api/account')
def account():
  is_logged_in = True if 'user_id' in session else False

  if 'user_id' in session:
        users = app.db.get_user_info(session['user_id'])
        return render_template('account_settings.html', users = users, is_logged_in = is_logged_in)
  else:
        return redirect(url_for('login'))


@app.route('/api/update_bankroll', methods=['POST'])
def update_bankroll():

    if request.method == 'POST':

        data = request.get_json()
        
        success = app.db.update_bankroll(data['email'], data['bankroll'])

        if success:
            response = jsonify({
                        "success": True,
                        "message": "Your bankroll has been successfully updated!"
                    })
            return response
            
        else:
            response = jsonify({
                        "success": False,
                        "message": "Something went wrong... Please try again later or contact support."
                    })
            return response
        

@app.route('/api/update_unitSize', methods=['POST'])
def update_unitSize():

    if request.method == 'POST':

        data = request.get_json()
        
        success = app.db.update_unitSize(data['email'], data['unitSize'])

        if success['return_bool']:
            response = jsonify({
                        "success": True,
                        "message": "Your Unit Size has been successfully updated!",
                        "kelley": success['return_kelley']
                    })
            return response
            
        else:
            response = jsonify({
                        "success": False,
                        "message": "Something went wrong... Please try again later or contact support.",
                        "kelley": success['return_kelley']
                    })
            return response

@app.route('/api/update_kelleyCriterion', methods=['POST'])
def update_kelleyCriterion():

    if request.method == 'POST':

        data = request.get_json()

        logger.info(f"UpdatekelleyCriterion: {data['kelley_criterion']}")
        
        success = app.db.update_kelleyCriterion(data['email'], data['kelley_criterion'])

        if success['return_bool']:
            response = jsonify({
                        "success": True,
                        "message": "Your Kelley Criterion Multiplier has been successfully updated!",
                        "unitSize": success['return_unitSize']
                    })
            return response
            
        else:
            response = jsonify({
                        "success": False,
                        "message": "Something went wrong... Please try again later or contact support.",
                        "unitSize": success['return_unitSize']
                    })
            return response



# TODO: make it such that a bad request is displayed on the front end
@app.route('/api/register', methods=['GET', 'POST'])
def register():

    app.db.get_all_usernames()
    app.db.update_customer_emails()
    users = app.db.users

    data = request.get_json()

    if request.method == 'POST':

        first_name = data['firstName']
        
        last_name = data['lastName']
        username = data['email'].lower()
        password = data['password']
        confirm_password = data['confirmPassword']
        phone = '+1' + str(data['phoneNumber'])
        unitSize = float(data['unitSize'])
        unitSize = abs(float(unitSize))
        if unitSize == 0:
            unitSize = 25
        if unitSize is None:
            unitSize = 25
        sign_up_date = datetime.now()
        payed = False
        how_heard = data['howHeard']
        kelley_criterion = 0.25
        bankroll = float(unitSize) / kelley_criterion
        try:
            referral_name = data['referralName']
        except:
            referral_name = ''
        try:
            other_source = data['otherSource']
        except:
            other_source = ''

        if password != confirm_password:
            error_message = "Passwords do not match. Please try again."
            response = jsonify({
                        "success": False,
                        "message": error_message
                    })
            return response, 400
        if username in users:
            app.db.check_payments(username)

            has_payed=app.db.check_duplicate_account(username)

            if has_payed:
                payed = True
                app.db.add_user(first_name, last_name, username, password, phone, bankroll, sign_up_date, payed, how_heard, referral_name, other_source, unitSize, kelley_criterion)
                users = app.db.users

                login_allowed = app.db.check_login_credentials(username, password)

                if login_allowed:
                    session['user_id'] = username
                    response = jsonify({
                        "success": True
                    })
                    return response
                elif not login_allowed:
                    error_message = "Email or password incorrect. Please try again."
                    response = jsonify({
                        "success": False,
                        "message": error_message
                    })
                    return response, 400     
        
            else:
                error_message = "Account with this Email already exists and payment has not been completed. Please return to home page and complete your purchase."

                response = jsonify({
                        "success": False,
                        "message": error_message
                    })
                return response, 400
        else:
            app.db.add_user(first_name, last_name, username, password, phone, bankroll, sign_up_date, payed, how_heard, referral_name, other_source, unitSize, kelley_criterion)
            login_allowed = app.db.check_login_credentials(username, password)
            if login_allowed:
                session['user_id'] = username

                response = jsonify({
                    "success": True
                })
                return response

            elif not login_allowed:
                logger.info('user not created successfully')
                error_message = "Email or password incorrect. Please try again."
                response = jsonify({
                        "success": False,
                        "message": error_message
                    })
                return response, 400   
    
    customer_email = request.args.get('customer_email', '')
    return render_template('register.html', username_exists=False, form_data={}, customer_email=customer_email)



@app.route('/api/login/v2/', methods=['POST', 'GET'])
def login_v2():
    try:

        my_db = database(app.db_manager)
        
        if request.method == 'GET':
            remember_token = request.cookies.get('remember_token')
            if remember_token:
                username = my_db.get_username_by_remember_token(remember_token)
                if username:
           
                    permission = my_db.get_permission(username.lower())

                    session['user_id'] = username
                    

                    session['permission'] = permission
                    response_data = {
                        "success": True,
                        "user": {
                            "username": username,
                            "permission": permission
                        }
                    }
                    return jsonify(response_data), 200
            return jsonify({"success": False, "message": "No remember token found or user not recognized"}), 401

        else :
            # Handle POST request
            data = request.get_json()
            username = data.get('username')
            password = data.get('password')
            remember = data.get('remember', False)
            
            if not username or not password:
                return jsonify({"success": False, "message": "Username and password are required."}), 400
            
            login_allowed = my_db.check_login_credentials(username, password)

            if login_allowed:
                permission = my_db.get_permission(username.lower())
                session['user_id'] = username
                session['permission'] = permission

                
                response_data = {
                    "success": True,
                    "user": {
                        "username": username,
                        "permission": permission
                    }
                }

                try:
                    make_session = app.db_manager.create_session()

                    new_login = UserLoginTimes(user_id=str(username.lower()), date = str(datetime.now()), permission=str(permission['permission']))

                    make_session.add(new_login)
                    
                    make_session.commit()
                except Exception as e:
                    logger.info(f"Couldn't add user login time to user_login_times: {e}")

                if remember:
    
                    remember_token = my_db.generate_secure_token()
                    my_db.store_remember_token(username, remember_token)
                    response = jsonify(response_data)
                    response.set_cookie('remember_token', 
                                        remember_token, max_age=60 * 60 * 24 * 10, 
                                        secure=True,
                                        httponly=True,
                                        samesite='None')

                    return response, 200
                
                return jsonify(response_data), 200
            
            else:
                return jsonify({"success": False, "message": "Invalid username or password"}), 401
        
    except Exception as e:
        print(f"Error in login process: {str(e)}")
        return jsonify({"success": False, "message": "An unexpected error occurred. Please try again later."}), 500

@app.route('/api/get_user_saved_filters', methods=['POST', 'GET'])
def get_user_saved_filters():
    try:

        make_session = app.db_manager.create_session()

        user = make_session.query(UserFilters).filter_by(username=session["user_id"]).first()
        if user:
            ret = user.saved_filters
        else:
            ret = {"sort-by": ["ev", False], "best-odds-filter": {"maxodds": "", "minodds": ""}}

    except Exception as e:
        print(e)
        return str(e)
    finally:
        make_session.close()
        return jsonify(ret)
    

@app.route('/api/get_user_saved_ai_ev_filters', methods=['POST', 'GET'])
def get_user_saved_ai_ev_filters():
    try:

        make_session = app.db_manager.create_session()

        user = make_session.query(UserAIEVFilters).filter_by(username=session["user_id"]).first()
        if user:
            ret = user.saved_filters
        else:
            ret = {"sort-by": ["ev", False], "best-odds-filter": {"maxodds": "", "minodds": ""}}

    except Exception as e:
        print(e)
        return str(e)
    finally:
        make_session.close()
        return jsonify(ret)


@app.route('/api/get_live_bet_data', methods = ["GET", "POST"])
def get_live_bet_data():

    try:
        bankroll = app.db.get_user_bank_roll(session["user_id"])
    except KeyError:
        bankroll = 5000
   
    data = app.db.get_live_bet_dash_data(bankroll)
    
    if data.empty:
        data = pd.DataFrame(columns=['bankroll', 'update'])
        data = data.append({'bankroll': bankroll, 'update': False}, ignore_index=True)
    else:
        data['bankroll'] = bankroll
    data_json = data.to_dict(orient='records')

    return jsonify(data_json)


@app.route('/api/get_user_saved_arbitrage_filters', methods=['POST', 'GET'])
def get_user_saved_arbitrage_filters():
    try:
        make_session = app.db_manager.create_session()

        if 'user_id' in session:

            user = make_session.query(UserArbitrageFilters).filter_by(username=session["user_id"]).first()

        if user:
            ret = user.saved_filters
        else:
            ret = {"sort-by": ["ev", False], "best-odds-filter": {"maxodds": "", "minodds": ""}}

    except Exception as e:
        print(e)
        return str(e)
    finally:
        make_session.close()
        return jsonify(ret)
    

@app.route('/api/get_user_saved_pregame_filters', methods=['POST', 'GET'])
def get_user_saved_pregame_filters():
    try:
        make_session = app.db_manager.create_session()

        user = make_session.query(UserPregameFilters).filter_by(username=session["user_id"]).first()
        if user:
            ret = user.saved_filters
        else:
            ret = {"sort-by": ["quality_rank", False], "best-odds-filter": {"maxodds": "", "minodds": ""}}

    except Exception as e:
        print(e)
        ret = {"sort-by": ["quality_rank", False], "best-odds-filter": {"maxodds": "", "minodds": ""}}
        return str(e)
    finally:
        make_session.close()
        return jsonify(ret)


def retry_on_session_error(max_retries=3, delay=1):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return f(*args, **kwargs)
                except KeyError as e:
                    if 'user_id' in str(e) and attempt < max_retries - 1:
                        logger.warning(f"user_id not found in session. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                    else:
                        raise
        return wrapper
    return decorator


# @app.before_request
# def log_request_info():
#     logger.info(f"Request Method: {request.method}")
#     logger.info(f"Request Path: {request.path}")
#     logger.info(f"Headers: {request.headers}")
#     logger.info(f"Body: {request.get_data()}")
#     logger.info(f"Session: {dict(session)}")

# @app.after_request
# def log_response_info(response):
#     logger.info(f"Response Status: {response.status}")
#     logger.info(f"Response Headers: {response.headers}")
#     logger.info(f"Session after request: {dict(session)}")
#     return response



@app.route('/api/save_user_filters', methods=['POST'])
@retry_on_session_error()
def save_user_filters():
    logger.info("Saving user filters")
    
    filters = request.get_json()
    
    if 'user_id' not in session:
        logger.error("user_id not found in session")
        return jsonify({"status": "error", "message": "User not authenticated"}), 401
    
    username = session['user_id']
    
    try:
        make_session = app.db_manager.create_session()
        user = make_session.query(UserFilters).filter_by(username=username).first()
        
        if user:
            result = make_session.query(UserFilters).filter_by(username=username).update({
                'saved_filters': filters, 
                'created_at': datetime.now()
            })
        else:
            new_user = UserFilters(username=username, saved_filters=filters, created_at=datetime.now())
            make_session.add(new_user)
        
        make_session.commit()
        return jsonify({
            "status": "success", 
            "message": "Filters saved successfully", 
            "user": username, 
            "filters": filters
        }), 200
    except Exception as e:
        logger.exception("Error saving user filters")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        make_session.close()  

@app.after_request
def session_commit(response):
    session.modified = True
    return response


@app.route('/api/save_user_arbitrage_filters', methods = ['POST'])
def save_user_arbitrage_filters():

    filters = request.get_json()

    try:
        make_session = app.db_manager.create_session()

        user = make_session.query(UserArbitrageFilters).filter_by(username=session["user_id"]).first()

        if user:
            user.saved_filters = filters
            user.created_at = datetime.now()
        else:
            new_user = UserArbitrageFilters(username=session["user_id"], saved_filters=filters, created_at = datetime.now())
            make_session.add(new_user)
        make_session.commit()
            
    except Exception as e:
            print(e)
            return str(e)
    finally:
            make_session.close()
            return jsonify({"status": "success", "message": "Filters saved successfully"}), 200
 


@app.route('/api/save_user_ai_ev_filters', methods = ['POST'])
def save_user_ai_ev_filters():

    filters = request.get_json()

    try:
        make_session = app.db_manager.create_session()

        user = make_session.query(UserAIEVFilters).filter_by(username=session["user_id"]).first()

        if user:
            user.saved_filters = filters
            user.created_at = datetime.now()
        else:
            new_user = UserAIEVFilters(username=session["user_id"], saved_filters=filters, created_at = datetime.now())
            make_session.add(new_user)
        make_session.commit()
            
    except Exception as e:
            print(e)
            return str(e)
    finally:
            make_session.close()
            return jsonify({"status": "success", "message": "Filters saved successfully"}), 200
 

@app.route('/api/save_user_pregame_filters', methods = ['POST'])
def save_user_pregame_filters():

    filters = request.get_json()

    username = session['user_id']

    try:
        make_session = app.db_manager.create_session()

        user = make_session.query(UserPregameFilters).filter_by(username=username).first()

        if user:
            user.saved_filters = filters
            user.created_at = datetime.now()
        else:
            new_user = UserPregameFilters(username=session["user_id"], saved_filters=filters, created_at = datetime.now())
            make_session.add(new_user)
        make_session.commit()
            
    except Exception as e:
            print(e)
            return str(e)
    finally:
            make_session.close()
            return jsonify({"status": "success", "message": "Filters saved successfully"}), 200
 

@app.route('/api/get_pos_ev_data_react', methods = ['GET', 'POST'])
def get_pos_ev_data_react():


    logger.info(f'starting get positive_ev_dash_data_react {datetime.now()}, memory: {process.memory_info().rss / 1024 / 102}')

    if 'user_id' in session:
        try:
            bankroll = app.db.get_user_bank_roll(session["user_id"])
        except:
            bankroll = 5000
    else:
        bankroll = 5000


    
    df = read_cached_df('pos_ev_dash_cache')


    # Commented out for testing

    username = session["user_id"] if "user_id" in session else None

    df = app.db.get_positive_ev_dash_data_react(bankroll, df, username)

    logger.info(f'ending get positive_ev_dash_data_react {datetime.now()}, memory: {process.memory_info().rss / 1024 / 102}')


    return df.to_json(orient='records')


@app.route('/api/get_ai_ev_data_react', methods=['GET'])
def get_ai_ev_data_react():

    if 'user_id' in session:
        try:
            bankroll = app.db.get_user_bank_roll(session["user_id"])
        except:
            bankroll = 5000
    else:
        bankroll = 5000
    
    df = read_cached_df('ai_ev_dash_cache')

    logger.info(df)

    username = session["user_id"] if "user_id" in session else None

    df = app.db.get_ai_ev_dash_data_react(bankroll, df, username)

    logger.info(f'ending get ai_ev_dash_data_react {datetime.now()}, memory: {process.memory_info().rss / 1024 / 102}')


    return df.to_json(orient='records')


@app.route('/api/get_arb_data_react', methods = ['GET', 'POST'])
def get_arb_data_react():
    print('arb react')

    try:
        bankroll = app.db.get_user_bank_roll(session["user_id"])
    except:
        bankroll = 5000

    df = read_cached_df('arb_dash_cache')

    df = app.db.get_arbitrage_dash_data_react(df)

    df['bankroll'] = bankroll

    return df.to_json(orient='records')


@app.route('/api/logout/v2/', methods=['POST'])
def logout_v2():
    try:
        session.clear()
        response = jsonify({
            "success": True
        })
        # response.set_cookie('remember_token', '', expires=0, secure=True, httponly=True, samesite='None')
        return response, 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

    
@app.route("/api/cancel_subscription", methods=["POST"])
def cancel_subscription():
    # Perform the subscription cancellation logic here
    action = request.get_json().get("action")

    if action == "cancel":
        # Implement the subscription cancellation process here
        # You can interact with your subscription service or database
        db = database(app.db_manager)
        db.cancel_subscription(session['user_id'])
        return redirect(url_for('logout'))
    else:
        return jsonify({"success": False})


@app.route("/api/get_user_permission", methods=['GET'])
def get_user_permission():
     if 'permission' in session:
         return jsonify({'permission':session['permission']})
     elif 'user_id' in session:
        return jsonify({'permission': 
                        app.db.get_permission(session['user_id'])['permission']})
     else:
         return jsonify({'permission': 'free'})
 

@app.route('/api/reset_password', methods=['POST','GET'])
def reset_password():

    data = request.get_json()

    username = data.get('email')

    def generate_random_code():
    # Generate a random 6-digit code
        return str(random.randint(100000, 999999))

    def send_email(email, code):
        sender_email = 'getsmartbettor@gmail.com'
        sender_password = 'gfjk lqhs xnqn syji'


    # Create a MIMEText object for the email body
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = email
        message['Subject'] = f'Your Smartbettor Verification Code: {code}'
        body = f"""Hey Valued SmartBettor User,

        To verify your password reset request please enter the following code when prompted:

        {code}

        Please note that this code is only valid for the next 5 minutes.

        Thank you!
        """
        
    # Attach the body to the email
        message.attach(MIMEText(body, 'plain'))

    # Establish a connection to the SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, email, message.as_string())
        print(f"Verification code sent to {username}")

    def password_function(email):
        try:
            session = app.db_manager.create_session()
            user = session.query(LoginInfo).filter_by(username=username).first()
            if user != None:
                code = generate_random_code()
                send_email(email, code)
                return code
            else:
                #TODO: test
                return 0
        except Exception as e:
            print(e)
            return str(e)
        finally:
            session.close()
    
    def set_reset_instance(code,username):
        if code != 0:
            try:
            # Create a session
                session = app.db_manager.create_session()
                current_datetime = datetime.now()

    # Add 5 minutes to the current datetime
                time_plus_5 = current_datetime + timedelta(minutes=5)

                new_code = VerificationCode(username=username, code=code, time_allowed=time_plus_5, used = False)


                # Add the new user to the session and commit the transaction
                session.add(new_code)
                session.commit()
            except Exception as e:
                print(e)
                return str(e)
            finally:
                session.close()

    code = password_function(username)

    if code == 0:
        error_message = "Email not found in the database. If you have paid through stripe, please complete registration."
        return render_template('register.html', incorrect_password=True, form_data={}, error_message=error_message) 
    
    set_reset_instance(code,username)

    response = jsonify({
            "success": True
        })

    return response


@app.route('/api/confirm_password')
def confirm_password():
    username = request.args.get('username')
    try:
        msg = request.args.get('msg')
    except:
        msg = ''

    if request.referrer and 'reset_password' in request.referrer:
        return render_template('confirm_password.html', username = username)
    
    elif request.referrer and 'confirm_password' in request.referrer:
        return render_template('confirm_password.html', username = username, msg = msg)
    else:
        # Redirect to login page if the referrer is not reset_password
        return redirect(url_for('login'))
    

@app.route('/api/confirm_password_button', methods=['POST','GET'])
def confirm_password_button():
    data = request.get_json()
    username = data['email']
    code = data['code']

    try:
        session = app.db_manager.create_session()
        code = int(code)
        verification_code = session.query(VerificationCode).filter_by(username=username, code=code).first()

        if verification_code:
            current_datetime = datetime.now()
            if verification_code.time_allowed > current_datetime and not verification_code.used:
                # Set the code as used and commit the transaction
                session.delete(verification_code)
                session.commit()
                response = jsonify({
                    "success": True
                })
                return response
    
            else:
                response = jsonify({
                    "success": False,
                    "msg": "Code expired or already used"
                })
                return response
        else:
            msg= "Invalid code for the given username"
            response = jsonify({
                    "success": False,
                    "msg": msg
            })
            return response
      

    except Exception as e:
        print(e)
        return jsonify({"success": False, "message": str(e)})
    finally:
        session.close()


@app.route('/api/set_new_password_db', methods=['GET', 'POST'])
def set_new_password_db():

    data = request.get_json()
    username = data['email']
    password = data['password']

    try:
        # Create a session
        session = app.db_manager.create_session()
        
            
        # Update all entries for this username
        result = session.query(LoginInfo).filter_by(username=username).update({'password': generate_password_hash(password)})
        session.commit()
        if result > 0:

            # Flash success message and redirect to login or any other page
            response = jsonify({
                    "success": True,
            })
            return response

        else:

            response = jsonify({
                    "success": False,
                    "msg": "Something bad happened, please try to go through this process again..."
            })
            return response

    except Exception as e:
        print(e)

    finally:
        session.close()

    response = jsonify({
                    "success": True,
            })
    return response


@app.route('/api/password_update_successful')
def password_update_successful():
    return render_template('password_update_successful.html')


@app.route('/api/change_account', methods=["GET"])
def change_account():
    session.clear()
    response_data = {
                    "success": True,
                }

    
    response = jsonify(response_data)
    response.cookies = request.cookies
    try:
        response.set_cookie(
            'remember_token',
            value='',  # Set the cookie value to an empty string
            max_age=0,  # Set the maximum age to 0 to expire the cookie immediately
            secure=True,
            httponly=True,
            samesite='None'
        )
    except Exception as e:
        print(f"Error setting cookie: {e}")

    return response, 200
    

@app.route('/api/get_user_information', methods=["GET"])
def get_user_account_info():
    
    username = session['user_id']

    user_dict = app.db.get_user_account_info(username)

    response = jsonify({
                    "success": True,
                    "bankroll": user_dict['bankroll'],
                    "firstName": user_dict['first_name'],
                    "lastName": user_dict['last_name'],
                    "email": user_dict['email'],
                    "phone": user_dict['phone_number'],
                    "unitSize": user_dict['unitSize'],
                    "date_signed_up": user_dict['date_signed_up'],
                    "kelley_criterion": user_dict['kelley_criterion']

    })
    return response

@app.route('/api/get_MMA_Data', methods=['GET'])
def get_MMA_data():
    event_data = app.db.get_mma_data()
    return jsonify(event_data)


@app.route('/api/get_MMA_Game_Data', methods = ['GET'])
def get_MMA_Game_Data():
    game_id = request.args.get('gameId')
    game_data = app.db.get_MMA_game_data(game_id)
    logger.info('Game data')
    logger.info(game_data)
    return jsonify(game_data)


from sqlalchemy import text

from flask import session, jsonify
from sqlalchemy import text
from datetime import datetime, timedelta

@app.route('/api/get_ai_ev_popup_status', methods=['GET'])
def get_ai_ev_popup_status():
    if 'user_id' not in session:
        return jsonify({"error": "User not logged in", "show_popup": True})

    sql_query = text("""
        SELECT *
        FROM user_login_times
        WHERE user_id = :username
        ORDER BY STR_TO_DATE(date, '%Y-%m-%d %H:%i:%s.%f') DESC
        LIMIT 2
    """)
    
    db_session = app.db_manager.create_session()
    try:
        results = db_session.execute(sql_query, {'username': session['user_id']}).fetchall()
        
        if len(results) < 2:
            # If there's only one or no login records, always show the popup
            return jsonify({"show_popup": True, "reason": "Insufficient login history"})

        # Get the second-to-last login time
        previous_login = results[1]
        date_str = previous_login.date
        try:
            # Attempt to parse the date string
            date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # If microseconds are not present in the string
            date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        
        # Calculate the time difference
        time_difference = datetime.now() - date_obj
        
        # Determine if the popup should be shown (e.g., if last login was more than 24 hours ago)
        show_popup = time_difference > timedelta(hours=24)
        
        return jsonify({
            "last_login": date_obj.isoformat(),
            "show_popup": show_popup,
            "reason": "Time since previous login" if show_popup else "Recent login"
        })
    finally:
        db_session.close()



if __name__ == '__main__':

    socketio.run(app, debug=True, port=5000, use_reloader=False)