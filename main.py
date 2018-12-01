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
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from werkzeug.exceptions import InternalServerError
from http import HTTPStatus
from pymongo import MongoClient
from kombu import Connection, Consumer, Exchange, Queue

app = Flask(__name__)
app.config['videos'] = f'{os.getcwd()}/videos'
SIO = socketio.Server(async_mode='threading')
app.wsgi_app = socketio.Middleware(SIO, app.wsgi_app)
CORS(app)


MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')

RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
RABBIT_PORT = int(os.getenv('RABBIT_PORT', 5672))

VALID_VID_EXT = set(['.mpeg4', '.mp4', '.avi', '.wmv',
                     '.mpegps', '.flv', '.3gpp'])
VALID_IMG_EXT = set(['.jpg', '.jpeg', '.png'])


mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)

db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]

LOG = logging
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


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
    img_data = request.files.get('img')

    # Check uploaded Video and Image are None Type, or Not
    if video_data == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    if img_data == None:
        return jsonify({'success': False, 'error': 'Image Not Found'})

    video_filename = video_data.filename
    video_id = hash_key(uid, name)
    img_name = img_data.filename

    # Check valid vid using ext
    _, vid_ext = os.path.splitext(video_filename)
    if vid_ext not in VALID_VID_EXT:
        return jsonify({'success': False, 'error': 'Unsupported Media Type'})

    _, img_ext = os.path.splitext(img_name)
    if img_ext not in VALID_IMG_EXT:
        return jsonify({'success': False, 'error': 'Unsupported Image Type'})

    # Make directory using keyID and save uploaded file
    path = os.path.join(app.config['videos'], f'{video_id}/')

    try:
        os.makedirs(path)
        new_vid_filename = name+vid_ext
        video_data.save(path+new_vid_filename)
    except (OSError, IOError):
        return jsonify({'success': False, 'error': 'Save Video File Error'})

    try:
        # Save the image in the same path as video
        new_img_filename = name+img_ext
        img_data.save(path+new_img_filename)
    except (OSError, IOError):
        return jsonify({'success': False, 'error': 'Save Image File Error'})

    data = {
        'video_id': video_id,
        'name': name,
        'filename': new_vid_filename,
        'img': new_img_filename,
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


@app.route('/video/<video_id>', methods=['GET'])
def get_vid_status(video_id):
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    normalized_data = {
        'name': search_result['name'],
        'video_id': search_result['video_id'],
        'filename': search_result['filename'],
        'uid': search_result['uid'],
        'img': search_result['img'],
        'source': search_result['source'],
        'likes': len(search_result['likes']),
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
            'img': doc['img'],
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
    app.run(host='0.0.0.0')
