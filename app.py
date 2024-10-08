
from flask import Flask, request, session, redirect, jsonify, url_for, render_template
from flask_socketio import SocketIO
import plotly as plotly
from functionality.database import database
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
from functools import wraps
import simplejson as json
from decimal import Decimal
from datetime import datetime, timedelta
from functionality.models import LoginInfoHOF
from flask_jwt_extended import JWTManager
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity



process = psutil.Process(os.getpid())

redis_client = redis.Redis(host='localhost', port=6379, db=0)

from flask_compress import Compress
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def create_app():
    app = Flask(__name__, template_folder='static/templates', static_folder='static')
    CORS(app, supports_credentials=False, resources={r"/*": {"origins": "*"}})



    Compress(app)
    # TODO: Put this key in the secret file
    app.secret_key = 'to_the_moon'
    app.db_manager = DBManager()
    app.db = database(app.db_manager)
    app.jwt = JWTManager(app)

    from functionality.routes.api import api
    app.register_blueprint(api)

    return app


app = create_app()
app.config['REACT_COMPONENT_DIRECTORY'] = os.path.join(app.root_path, 'react_frontend')
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY')
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

    try:
        # Check if the user exists
        user = db_session.query(LoginInfoHOF).filter_by(uid=uid).first()

        if user:
            # User exists; check if they have paid
            if user.subscription_status == 'paid':
                logger.info(f'subscription_status paid')
                access_token = create_access_token(identity={'email': email}, expires_delta=timedelta(days=7))
                response = jsonify({'redirect': '/market_view', 'access_token': access_token})
                response.set_cookie('access_token', access_token, httponly=True, secure=True)
            else:
                pass  # Proceed with Stripe checkout

        else:
            # Register the new Google user
            new_user = LoginInfoHOF(uid=uid, email=email, name=name, subscription_status='unpaid')
            db_session.add(new_user)
            db_session.commit()

    except Exception as e:
        db_session.rollback()
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
                    'unit_amount': 1000,  # Amount in cents ($10)
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url=url_for('market_view_success', _external=True)+ f"?session_id={{CHECKOUT_SESSION_ID}}&email={email}",
            cancel_url=url_for('register', _external=True),
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
                        access_token = create_access_token(identity={'email': email}, expires_delta=timedelta(days=7))
                        response = jsonify({'redirect': '/market_view', 'access_token': access_token})
                        response.set_cookie('access_token', access_token, httponly=True, secure=True)
                        logger.info(access_token)
                        logger.info(response)
                        logger.info('returning response')
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

    db_session = app.db_manager.create_session()

    try:
        # Check if the user already exists
        user = db_session.query(LoginInfoHOF).filter_by(email=email).first()

        if user:
            return jsonify({'error': 'User already exists'}), 400

        # Hash the password for storage
        hashed_password = generate_password_hash(password)

        # Register the new user
        new_user = LoginInfoHOF(
            uid=None,  # Generate a UID if necessary
            email=email,
            name=name,
            password=hashed_password,
            subscription_status='unpaid'
        )
        db_session.add(new_user)
        db_session.commit()

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
                    'unit_amount': 1000,  # Amount in cents ($10)
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url=url_for('market_view_success', _external=True)+f"?session_id={{CHECKOUT_SESSION_ID}}&email={email}",
            cancel_url=url_for('register', _external=True),
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

    if not session_id:
        return jsonify({'error': 'Session ID not provided'}), 400

    try:
        # Retrieve the Stripe session to verify payment success
        stripe_session = stripe.checkout.Session.retrieve(session_id)

        if stripe_session.payment_status == 'paid':
            session = app.db_manager.create_session()

            try:
                # Update the subscription_status to 'paid' for the user with the given email
                user = session.query(LoginInfoHOF).filter_by(email=email).first()
                
                if user:
                    user.subscription_status = 'paid'
                    session.commit()
                    
                    # Generate JWT for the user with email and set expiration
                    access_token = create_access_token(identity={'email': email}, expires_delta=timedelta(days=7))
                    
                    # Set JWT in the response cookies
                    response = jsonify({'message': 'Payment successful, redirecting...'})
                    response.set_cookie('access_token', access_token, httponly=True, secure=True)  # Use secure=True if using HTTPS
                    
                    # Redirect to the internal market_view route
                    response.headers['Location'] = '/market_view'
                    response.status_code = 302  # Found status code for redirection
                    return response
                
                else:
                    return jsonify({'error': 'User not found'}), 404

            except Exception as e:
                session.rollback()
                return jsonify({'error': str(e)}), 500

            finally:
                session.close()

        else:
            return jsonify({'error': 'Payment not completed'}), 400

    except Exception as e:
        return jsonify({'error': str(e)}), 500
 

# Route for Register
@app.route('/api/register')
def register():
    return redirect(f'https://homeoffightpicks.com/register')





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
    current_user = get_jwt_identity()  # This will contain the user's email
    return jsonify({'message': 'Welcome to the Market View!', 'user_email': current_user['email']})

if __name__ == '__main__':

    socketio.run(app, debug=True, port=5000, use_reloader=False)