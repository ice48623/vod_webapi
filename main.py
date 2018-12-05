#!/usr/bin/env python3
import os
import string
import random
import pika
import time
import socketio
import threading
import json
import logging
import hashlib
import io
import time
import requests
from bson import ObjectId
from flask import Flask, jsonify, request, Response, session
from flask_cors import CORS
from flask_login import login_user, logout_user, login_required, current_user, LoginManager, UserMixin, confirm_login
from werkzeug.exceptions import InternalServerError
from werkzeug.security import generate_password_hash, check_password_hash
from http import HTTPStatus
from pymongo import MongoClient
from kombu import Connection, Consumer, Exchange, Queue


app = Flask(__name__)
app.config['videos'] = os.getenv('BASE_VIDEOS_FOLDER', './videos')
login_manager = LoginManager()
login_manager.init_app(app)
# SIO = socketio.Server(async_mode='threading')
# app.wsgi_app = socketio.Middleware(SIO, app.wsgi_app)
CORS(app)

SECRET_KEY = os.getenv('SECRET_KEY','some-secret-key')

MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')

RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
RABBIT_PORT = int(os.getenv('RABBIT_PORT', 5672))

VALID_VID_EXT = set(['.mpeg4', '.mp4', '.avi', '.wmv',
                     '.mpegps', '.flv', '.3gpp'])
VALID_IMG_EXT = set(['.jpg', '.jpeg', '.png'])

BASE_URL = os.getenv('BASE_URL', 'http://localhost:4000')


mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)

db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]
my_users = db['my_users']


LOG = logging
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class User(UserMixin):
    def __init__(self,user_doc):
        self.id = str(user_doc['_id'])
        self.username = user_doc['username']


# Utils


def hash_key(uid, name):
    concat_data = uid + name + str(time.time())
    return hashlib.md5(concat_data.encode('utf-8')).hexdigest()

# Rabbit Sender


def send_job(queue_name, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    LOG.info(f'Sent: {message} into queue: {queue_name}')
    connection.close()


# Flask Route

@app.route("/ping")
def ping():
    return "ping"


@app.route("/upload", methods=['POST'])
def upload_vid():

    name = request.form.get('name')
    uid = request.form.get('uid')

    video_data = request.files.get('file')

    # Check uploaded Video and Image are None Type, or Not
    if video_data == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    video_filename = video_data.filename
    video_id = hash_key(uid, name)

    # Check valid vid using ext
    _, vid_ext = os.path.splitext(video_filename)
    if vid_ext not in VALID_VID_EXT:
        return jsonify({'success': False, 'error': 'Unsupported Media Type'})

    # Make directory using keyID and save uploaded file
    path = os.path.join(app.config['videos'], f'{video_id}/')

    try:
        os.makedirs(path)
        new_vid_filename = name+vid_ext
        video_data.save(path+new_vid_filename)
    except (OSError, IOError):
        return jsonify({'success': False, 'error': 'Save Video File Error'})

    data = {
        'video_id': video_id,
        'name': name,
        'filename': new_vid_filename,
        'uid': uid,
        'likes': [],
        'comments': [],
        'source': [],
    }

    collection.insert_one(data)

    # Send Job to convert queue
    resolutions = [360, 720, 1080]
    for r in resolutions:
        json_packed = json.dumps({
            'video_id': video_id,
            'filename': new_vid_filename,
            'resolution': r,
        })

        send_job('convert', json_packed)

    return jsonify({'success': True, 'error': ''})


@app.route('/video/<video_id>', methods=['POST'])
def get_vid_status(video_id):
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    uid = request.json.get('uid')
    likes_uid = search_result['likes']
    is_like = uid in likes_uid
    likes = {
        'amount' : len(likes_uid),
        'is_like' : is_like,
    }
    normalized_data = {
        'name': search_result['name'],
        'video_id': search_result['video_id'],
        'filename': search_result['filename'],
        'uid': search_result['uid'],
        'source': search_result['source'],
        'likes': likes,
        'comments': search_result['comments'],
    }
    return jsonify({'success': True, 'error': '', 'data': normalized_data})


@app.route('/video')
def get_all_vid():
    vids = collection.find({})
    data = []
    for doc in vids:
        info = {
            'name': doc['name'],
            'video_id': doc['video_id'],
            'uid': doc['uid'],
            'img': f"{BASE_URL}/thumb/{doc['video_id']}/{doc['filename']}/thumb-1000.jpg",
            'likes': len(doc['likes']),
            'comments': len(doc['comments'])
        }
        data.append(info)
    return jsonify({'success': True, 'error': '', 'data': data})


@app.route('/comment', methods=['PUT'])
def comment():
    video_id = request.json.get('video_id')
    comment = request.json.get('comment')
    uid = request.json.get('uid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'video_id': video_id,
        'uid': uid,
        'comment': comment,
    })
    send_job('comment', json_packed)
    return jsonify({'success': True, 'error': ''})

@app.route('/comment/<video_id>', methods=['GET'])
def get_comment(video_id):
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    normalized_data = {
        'comments': search_result['comments'],
    }
    return jsonify({'success': True, 'error': '', 'data': normalized_data})

@app.route('/like', methods=['POST'])
def like():
    video_id = request.json.get('video_id')
    uid = request.json.get('uid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'video_id': video_id,
        'uid': uid,
        'like': True,
    })

    send_job('like', json_packed)
    return jsonify({'success': True, 'error': ''})


@app.route('/unlike', methods=['POST'])
def unlike():
    video_id = request.json.get('video_id')
    uid = request.json.get('uid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'video_id': video_id,
        'uid': uid,
        'like': False,
    })

    send_job('like', json_packed)
    return jsonify({'success': True, 'error': ''})


@app.route('/register', methods=['POST'])
def register():
    username = request.json.get('username')
    password = request.json.get('password')
    existing_user = my_users.find_one({'username': username})
    if existing_user != None:
        return jsonify({'success': False, 'error': 'Username already exist'})

    hash_pwd = generate_password_hash(password, method='pbkdf2:sha1', salt_length=8)
    my_users.insert_one({
        'username': username,
        'password': hash_pwd,
    })

    user = my_users.find_one({'username': username})
    data = {
        'uid': str(user['_id']),
        'username': user['username'],
    }
    user = User(user)
    login_user(user,remember=True)
    return jsonify({'success': True, 'error': '', 'data': data})

@app.route('/login', methods=['POST'])
def login():
    username = request.json.get('username')
    password = request.json.get('password')
    existing_user = my_users.find_one({'username': username})
    if existing_user == None:
        return jsonify({'success': False, 'error': 'Username not found'})

    user_pwd = existing_user['password']
    if check_password_hash(user_pwd, password):
        user = User(existing_user)
        login_user(user,remember=True)
    else:
        return jsonify({'success': False, 'error': 'Wrong password'})

    data = {
        'uid': str(existing_user['_id']),
        'username': existing_user['username'],
    }
    # print("authenticated : ",username," ",current_user.is_authenticated)

    return jsonify({'success': current_user.is_authenticated, 'error': '', 'data': data})

@app.route('/check',methods=['POST'])
def check():
    print(current_user)
    return jsonify({'status':current_user.is_authenticated})

# Not Done (work?????)
@app.route('/logout', methods=['POST'])
@login_required
def logout():
    print(current_user)
    detected_error = ""
    test_before_logout = current_user.is_authenticated
    print("authentication status: ", test_before_logout)
    # print("authenticated : ",current_user.is_authenticated)
    logout_user()
    if(current_user.is_authenticated == True):
        detected_error = "user is still logged in, WTF?"
    return jsonify({'success': not current_user.is_authenticated, 'error': detected_error})

@login_manager.unauthorized_handler
def unauthorized():
    return jsonify({'error':'user not authorized'})

@login_manager.user_loader
def user_loader(user_id):
    user_doc = my_users.find_one({'_id':ObjectId(user_id)})
    if user_doc is not None:
        return User(user_doc)
    return None

# @SIO.on('connection')
# def on_connection(sid):
#     LOG.info(f'Client connected, sid : {sid}')


# def handle_upload_status(body):
#     parsed_body = json.loads(body)
#     video_id = parsed_body.get('video_id')
#     SIO.emit('on upload', data="", room=video_id)


# class RabbitThread:
#     def __init__(self, interval=1):
#         self.interval = interval
#         self.rabbit_url = "amqp://" + RABBIT_HOST + ":" + RABBIT_PORT + "/"
#         self.conn = Connection(self.rabbit_url)
#         self.exchange = Exchange("update_convert", type="direct")
#         self.queue = Queue(
#             name="update_convert",
#             exchange=self.exchange,
#             routing_key="update_convert")
#         thread = threading.Thread(target=self.run, args=())
#         thread.daemon = True  # Daemonize thread
#         thread.start()  # Start the execution

#     def run(self):
#         def process_message(body, message):
#             LOG.info(f'The body is {body}')
#             handle_upload_status(body)
#             message.ack()

#         while True:
#             with Consumer(
#                     self.conn,
#                     queues=self.queue,
#                     callbacks=[process_message],
#                     accept=["json"]):
#                 self.conn.drain_events()


if __name__ == '__main__':
    # RABBIT = RabbitThread()
    # add threaded=True for using RabbitThread()
    app.secret_key = SECRET_KEY
    app.run(debug=True, host='0.0.0.0')
