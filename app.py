
from flask import Flask, request, session, redirect, jsonify, url_for, render_template, jsonify, abort
from flask_socketio import SocketIO
from functionality.database import database
import requests
import pandas as pd
import os
import stripe
import time
import atexit
from functionality.db_manager import DBManager
import warnings
from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash
warnings.filterwarnings("ignore")
from flask_socketio import SocketIO
import jsonpickle
from flask import Flask, jsonify
from flask_socketio import SocketIO
import time
import pandas as pd
import flock as flock
from flask_cors import CORS
import redis
import psutil
import random
from functools import wraps
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import simplejson as json
from decimal import Decimal
from datetime import datetime, timedelta
from functionality.models import LoginInfoHOF, VerificationCodeHOF
from flask_jwt_extended import JWTManager
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
from flask_jwt_extended.exceptions import JWTExtendedException
from authlib.integrations.flask_client import OAuth
import uuid 


process = psutil.Process(os.getpid())

redis_client = redis.Redis(host='localhost', port=6379, db=0)

from flask_compress import Compress
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def create_app():
    app = Flask(__name__, template_folder='static/templates', static_folder='static')
    CORS(app, resources={r"/*": {"origins": ["https://homeoffightpicks.com", "http://localhost:3000","http://localhost:3001", "https://app.homeoffightpicks.com"]}}, supports_credentials=True)

    Compress(app)
    # TODO: Put this key in the secret file
    app.secret_key = 'to_the_moon'
    app.db_manager = DBManager()
    app.db = database(app.db_manager)

    from functionality.routes.api import api
    app.register_blueprint(api)

    return app


app = create_app()


oauth = OAuth(app)
discord = oauth.register(
    name='discord',
    client_id=os.environ.get('DISCORD_CLIENT_ID'),
    client_secret=os.environ.get('DISCORD_CLIENT_SECRET'),
    access_token_url='https://discord.com/api/oauth2/token',
    access_token_params=None,
    authorize_url='https://discord.com/api/oauth2/authorize',
    authorize_params=None,
    api_base_url='https://discord.com/api/users/@me',
    client_kwargs={
        'scope': 'identify email guilds guilds.members.read',
        'response_type': 'code',
    },
)



app.config['REACT_COMPONENT_DIRECTORY'] = os.path.join(app.root_path, 'react_frontend')
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY')
app.config['SERVER_NAME'] = 'app.homeoffightpicks.com'
# app.config['SERVER_NAME'] = '127.0.0.1:5000'

jwt = JWTManager(app)

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

@app.route('/api/auth/discord')
def discord_login():
    """
    Redirect user to Discord OAuth authorization page
    """
    redirect_uri = url_for('discord_authorize', _external=True)
    return discord.authorize_redirect(redirect_uri)


@app.route('/api/auth/discord/callback')
def discord_authorize():
    """
    Handle Discord OAuth callback
    """
    try:
        logger.info('Starting Discord callback processing')
        logger.info(f'Received code: {request.args.get("code")}')
        
        # Exchange code for token with explicit parameters
        token = discord.authorize_access_token()
        logger.info(f'Token response: {token}')  # Be careful not to log this in production
        
        if not token:
            logger.error('No token received from Discord')
            return jsonify({"error": "No token received"}), 401

        # Make sure we have an access token before proceeding
        access_token = token.get('access_token')
        if not access_token:
            logger.error('No access_token in token response')
            return jsonify({"error": "No access token"}), 401

        # Use the token to get user info
        headers = {'Authorization': f'Bearer {access_token}'}
        user_resp = requests.get('https://discord.com/api/users/@me', headers=headers)
        
        if not user_resp.ok:
            logger.error(f'Failed to get user info: {user_resp.status_code} {user_resp.text}')
            return jsonify({"error": "Failed to get user info"}), 401

        user_info = user_resp.json()
        logger.info(f'Successfully got user info: {user_info}')

        # Get guilds
        guilds_resp = requests.get('https://discord.com/api/users/@me/guilds', headers=headers)
        
        if not guilds_resp.ok:
            logger.error(f'Failed to get guilds: {guilds_resp.status_code} {guilds_resp.text}')
            return jsonify({"error": "Failed to get guilds"}), 401

        guilds = guilds_resp.json()
        
        # Check guild membership and roles
        target_guild_id = os.environ.get('DISCORD_GUILD_ID')
        target_role_id = os.environ.get('DISCORD_ROLE_ID')
        
        logger.info(f'Checking for guild {target_guild_id} and role {target_role_id}')
        
        bot_token = os.environ.get('DISCORD_BOT_TOKEN')
        if not bot_token:
            logger.error('Missing DISCORD_BOT_TOKEN environment variable')
            return jsonify({"error": "Server configuration error"}), 500

        # Use bot token for guild member request
        bot_headers = {'Authorization': f'Bot {bot_token}'}
        
        is_verified = False
        for guild in guilds:
            if guild['id'] == target_guild_id:
                member_resp = requests.get(
                    f'https://discord.com/api/guilds/{target_guild_id}/members/{user_info["id"]}',
                    headers=bot_headers  # Use bot headers here
                )
                
                if not member_resp.ok:
                    logger.error(f'Failed to get member info: {member_resp.status_code} {member_resp.text}')
                    continue

                member = member_resp.json()
                if target_role_id in member.get('roles', []):
                    is_verified = True
                    break

        if is_verified:
            logger.info('User is verified')
            access_token = create_access_token(
                identity={
                    'discord_id': user_info['id'],
                    'username': user_info['username'],
                    'email': user_info.get('email'),
                    'is_verified': True
                },
                expires_delta=timedelta(days=7)
            )
            frontend_url = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
            return redirect(f"{frontend_url}/market_view?token={access_token}")
        else:
            logger.info('User is not verified')
            frontend_url = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
            return redirect(f"{frontend_url}/login")

    except Exception as e:
        logger.error(f"Discord OAuth error: {str(e)}", exc_info=True)
        return jsonify({
            "error": "Authentication failed",
            "details": str(e),
            "type": type(e).__name__
        }), 401
   

@app.route('/logout')
def logout():
    """
    Logout route to clear session
    """
    session.clear()
    return redirect(url_for('login'))



@app.route('/api/get_MMA_Data', methods=['GET'])
def get_MMA_data():
    # Check if data is cached in Redis
    cache_key = "mma_data"
    cached_data = redis_client.get(cache_key)

    if cached_data:
        # Decode the cached data to check if it's empty
        decoded_data = jsonpickle.decode(cached_data)
        
        # If cached data is found but empty, delete the cache and query the database
        if not decoded_data:  # Assuming an empty list or dataframe in the cache
            logger.info('Empty cached data, clearing cache')
            redis_client.delete(cache_key)
        else:
            logger.info('here is the cached data')
            return jsonify(decoded_data)

    # If not cached or the cache was empty, query the database
    event_data = app.db.get_mma_data()
    logger.info('here is the data from the db')
    # Store the result in Redis with a timeout (e.g., 1 hour = 3600 seconds)
    redis_client.set(cache_key, jsonpickle.encode(event_data), ex=700)

    return jsonify(event_data)



@app.route('/api/get_MMA_Game_Data', methods=['GET'])
def get_MMA_Game_Data():
    game_id = request.args.get('gameId')
    cache_key = f"mma_game_data:{game_id}"
    
    # Check if game data is cached
    cached_data = redis_client.get(cache_key)
    logger.info(f'start of mma_game_data request{datetime.now()}')

    if cached_data:
        logger.info(f'Cached data returned{datetime.now()}')
        # Return cached data if available
        return jsonify(jsonpickle.decode(cached_data))

    # Otherwise, query the database
    logger.info(f'start of mma_game_data db rq{datetime.now()}')
    game_data = app.db.get_MMA_game_data(game_id)
    logger.info(f'end of mma_game_data db rq{datetime.now()}')

    # Store the result in Redis with a timeout
    redis_client.set(cache_key, jsonpickle.encode(game_data), ex=700)
    logger.info(f'set cache{datetime.now()}')

    return jsonify(game_data)


@app.route('/api/google_auth', methods=['POST'])
def google_auth():
    data = request.get_json()
    uid = data['uid']
    email = data['email']
    name = data['name']

    db_session = app.db_manager.create_session()
    logger.info(f"User {email} is logging in with google")

    try:
        # Check if the user exists
        user = db_session.query(LoginInfoHOF).filter_by(uid=uid).first()
        logger.info(f"User {user} is the user")

        if user:
            # User exists; check if they have paid
            if user.subscription_status == 'paid':
                logger.info(f"User {email} is logged in and has paid")
              
                access_token = create_access_token(
                    identity={
                        'email': email,
                        'stripe_email': user.stripe_email,
                        'name': user.name
                    },
                    expires_delta=timedelta(days=7)
                )
                response = jsonify({'redirect': '/market_view', 'access_token': access_token})
                return response, 200
            else:
                pass  # Proceed with Stripe checkout

    except Exception as e:
        db_session.rollback()
        logger.info(f"Error logging in with google: {e}")
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()

    # Stripe checkout session for unpaid users
    try:
        stripe_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price_data': {
                    'currency': 'usd',
                    'product_data': {
                        'name': 'Premium Subscription',
                    },
                    'unit_amount': 0,  # Amount in cents ($10)
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url=url_for('market_view_success', _external=True, _scheme='https')+ f"?session_id={{CHECKOUT_SESSION_ID}}&email={email}&name={name}&uid={uid}",
            cancel_url=url_for('register', _external=True, _scheme='https'),
            metadata={'uid': uid}
        )

        return jsonify({'id': stripe_session.id})

    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/login_email', methods=['POST'])
def login_email():
    data = request.get_json()
    email = data['email']
    password = data['password']

    db_session = app.db_manager.create_session()

    try:
        logger.info('before user definitions')
        # Check if the user exists by email
        user = db_session.query(LoginInfoHOF).filter_by(email=email).first()
        logger.info('after user definitions')
        
        if user:
            # Check if the user has a password set (i.e., not a Google login)
            if user.password:
                # Check if the password matches
                if check_password_hash(user.password, password):
                    if user.subscription_status == 'paid':
                        access_token = create_access_token(
                                identity={
                                    'email': email,
                                    'stripe_email': user.stripe_email,
                                    'name': user.name
                                },
                                expires_delta=timedelta(days=7)
                            )
                        response = jsonify({'redirect': '/market_view', 'access_token': access_token})
                        return response, 200
                    else:
                        return jsonify({'message': 'Payment required'}), 403
                else:
                    return jsonify({'error': 'Invalid credentials'}), 401
            else:
                # Handle the case where the user doesn't have a password (Google login)
                return jsonify({'error': 'This account is registered with Google. Please sign in using Google.'}), 400
        else:
            logger.info('here')
            return jsonify({'error': 'User not found'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


@app.route('/api/register_email', methods=['POST'])
def register_email():
    data = request.get_json()
    email = data['email']
    password = data['password']
    name = data['name']
    hashed_password = generate_password_hash(password)

    db_session = app.db_manager.create_session()

    try:
        # Check if the user already exists
        user = db_session.query(LoginInfoHOF).filter_by(email=email).first()

        if user:
            return jsonify({'error': 'User already exists'}), 400

        # Start Stripe checkout process
        stripe_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            allow_promotion_codes=True,
            line_items=[{
                'price_data': {
                    'currency': 'usd',
                    'product_data': {
                        'name': 'Premium Subscription',
                    },
                    'unit_amount': 0,  # Amount in cents ($10)
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url=url_for('market_view_success', _external=True, _scheme='https')+f"?session_id={{CHECKOUT_SESSION_ID}}&email={email}&name={name}&password={hashed_password}",
            cancel_url=url_for('register', _external=True, _scheme='https'),
            metadata={'email': email}
        )


        return jsonify({'id': stripe_session.id})

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


@app.route('/api/market_view_success')
def market_view_success():
    session_id = request.args.get('session_id')
    email = request.args.get('email')
    name = request.args.get('name')
    uid = request.args.get('uid')
    hashed_password = request.args.get('password')

    if not session_id:
        return jsonify({'error': 'Session ID not provided'}), 400

    try:
        # Retrieve the Stripe session to verify payment success
        stripe_session = stripe.checkout.Session.retrieve(session_id)

        if stripe_session.payment_status == 'paid':
            db_session = app.db_manager.create_session()

            try:
                stripe_email = stripe_session.customer_details.email
                new_user = LoginInfoHOF(
                    uid=uid,
                    email=email,
                    name=name,
                    password=hashed_password,
                    stripe_email=stripe_email,
                    subscription_status='paid'
                )
                db_session.add(new_user)
                db_session.commit()
                return redirect(f'https://app.homeoffightpicks.com/login')

            except Exception as e:
                db_session.rollback()
                return redirect(f'https://app.homeoffightpicks.com/register')

            finally:
                db_session.close()

        else:
            
            return redirect(f'https://app.homeoffightpicks.com/register')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
 

# Route for Register
@app.route('/api/register')
def register():
    return redirect(f'https://app.homeoffightpicks.com/register')





@app.route('/api/stripe_dup_wbhk', methods=['POST'])
def stripe_dup_wbhk():
    event = None
    payload = request.data
    sig_header = request.headers['STRIPE_SIGNATURE']

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.environ.get('dev_stripe_webhook_dup_secret')
        )
    except ValueError as e:
        # Invalid payload
        logger.info('Invalid Payload')
        raise e
    except stripe.error.SignatureVerificationError as e:
        logger.info('Signature verification')
        # Invalid signature
        raise 

    # Handle the event
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        logger.info(f"registered that it is checkout session completed")
        logger.info(session)
        handle_checkout_session_completed(session)

    return jsonify({'status': 'success'}), 200


def handle_checkout_session_completed(session):
    # Get customer info from the session
    customer_id = session['customer']
    email = session['customer_details']['email']
    logger.info(f"{email} of checkout session")

    # Check if an existing customer already exists based on email
    existing_customer = find_existing_customer_by_email(email)
    logger.info(f"{existing_customer} is there customer already")

    if existing_customer:
        # Finally, delete the existing customer so that there is only the most recent one
        stripe.Customer.delete(existing_customer.id)
        logger.info(f"Deleted old existing customer {existing_customer.id}")
    else:
        # If no existing customer, just proceed with the subscription created during the checkout session
        logger.info(f"New customer created with email {email}")


def find_existing_customer_by_email(email):

    try:
        # List customers by email
        customers = stripe.Customer.list(email=email, limit=100)
        logger.info("Customers")

        if customers and len(customers) > 1:
            logger.info(f"Multiple customers found for email {email}. Total: {len(customers.data)}")
            # Sort customers by creation date (newest first)
            customers.data.sort(key=lambda c: c.created, reverse=True)
            return customers.data[1]  # Return the second customer
        return None
    except Exception as e:
        logger.info(f"Error finding customer by email: {e}")
        return None


def find_active_subscription_for_customer(customer_id):
    try:
        # List active subscriptions for the customer
        subscriptions = stripe.Subscription.list(customer=customer_id, status='active', limit=1).data
        trialing_subscriptions = stripe.Subscription.list(customer=customer_id, status='trialing').data
        if subscriptions:
            return subscriptions[0]
        elif trialing_subscriptions:
            return trialing_subscriptions[0]
        return None
    except Exception as e:
        logger.info(f"Error finding active subscription: {e}")
        return None


def update_subscription_to_new_product(subscription_id, price_id):
    try:
        # Modify the existing subscription with a new price ID (product)
        stripe.Subscription.modify(
            subscription_id,
            items=[{
                'id': stripe.Subscription.retrieve(subscription_id)['items']['data'][0]['id'],
                'price': price_id,
            }]
        )
    except Exception as e:
         logger.info(f"Error updating subscription: {e}")



@app.route('/api/market_view')
@jwt_required()
def market_view():
    try:
        current_user = get_jwt_identity()
        logger.info(f"User {current_user['email']} is accessing the market view.")
        return jsonify({'message': 'Welcome to the Market View!', 'user_email': current_user['email']})
    except (ExpiredSignatureError, InvalidTokenError):
        # This will catch any JWT-related errors that weren't caught by the decorators
        return jsonify({"message": "Token has expired"}), 401
    

@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    return jsonify({"message": "Token has expired"}), 401

@jwt.invalid_token_loader
def invalid_token_callback(error):
    return jsonify({"message": "Invalid token"}), 401

@app.errorhandler(JWTExtendedException)
def handle_jwt_exception(error):
    return jsonify({"message": "Token has expired"}), 401



@jwt.unauthorized_loader
def unauthorized_callback(error):
    return jsonify({'error': 'Unauthorized access', 'message': str(error)}), 401








# Reset password 

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
        message['Subject'] = f'Your Home Of Fight Picks Verification Code: {code}'
        body = f"""Hey Valued HOF User,

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
            user = session.query(LoginInfoHOF).filter_by(email=username).first()
            if user != None:
                code = generate_random_code()
                send_email(email, code)
                return code
            else:
                #TODO: test
                return 0
        except Exception as e:
            logger.info(e)
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

                new_code = VerificationCodeHOF(username=username, code=code, time_allowed=time_plus_5, used = False)


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
        verification_code = session.query(VerificationCodeHOF).filter_by(username=username, code=code).first()

        if verification_code:
            current_datetime = datetime.now()
            if verification_code.time_allowed > current_datetime and not verification_code.used:
                # Set the code as used and commit the transaction
                session.delete(verification_code)
                session.commit()
                access_token = create_access_token(identity={'username': username}, expires_delta=timedelta(minutes=10))
                response = jsonify({
                    "success": True,
                    "access_token": access_token
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
@jwt_required()
def set_new_password_db():

    data = request.get_json()
    currrent_user = get_jwt_identity()
    logger.info(f"Setting new password for current user['username]")
    username = currrent_user['username']
    password = data['password']

    try:
        # Create a session
        session = app.db_manager.create_session()
        
            
        # Update all entries for this username
        result = session.query(LoginInfoHOF).filter_by(email=username).update({'password': generate_password_hash(password)})
        session.commit()
        if result > 0:
            logger.info(f"Setting new password successfully updated")

            # Flash success message and redirect to login or any other page
            response = jsonify({
                    "success": True,
            })
            return response

        else:

            response = jsonify({
                    "success": False,
                    "msg": "No user found for this username"
            })
            return response

    except Exception as e:
        logger.info(e)
        response = jsonify({
                    "success": False,
                    "msg": "Something bad happened, please try to go through this process again..."
        })
        return response

    finally:
        session.close()



@app.route('/api/password_update_successful')
def password_update_successful():
    return render_template('password_update_successful.html')

if __name__ == '__main__':

    socketio.run(app, debug=True, port=5000, use_reloader=False)