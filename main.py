#!/usr/bin/env python3
import os
from time import time
import json
import logging
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import hashlib
import io
from werkzeug.exceptions import InternalServerError
from http import HTTPStatus
from flask_socketio import SocketIO
from pymongo import MongoClient
import string
import random
import pika
import time


app = Flask(__name__)
app.config['videos'] = f'{os.getcwd()}/videos'
CORS(app)
socketio = SocketIO(app)

MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')

RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
RABBIT_PORT = int(os.getenv('RABBIT_PORT', 5672))

VALID_EXT = ['.mpeg4', '.mp4', '.avi', '.wmv', '.mpegps', '.flv', '.3gpp']

mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)

db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]

LOG = logging
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# Utils


def hash_key(username, name):
    concat_data = username + name + str(time.time())
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
    username = request.form.get('username')
    video_data = request.files.get('file')
    filename = video_data.filename
    video_id = hash_key(username, name)

    # Check valid vid using ext
    _, ext = os.path.splitext(filename)
    if ext not in VALID_EXT:
        return jsonify({'success': False, 'error': 'Unsupported Media Type'})

    # Make directory using keyID and save uploaded file
    path = os.path.join(app.config['videos'], f'{video_id}/')
    os.makedirs(path)
    new_filename = name+ext
    video_data.save(path+new_filename)

    data = {
        'video_id': video_id,
        'name': name,
        'username': username,
        'likes': [],
        'comments': [],
        'resolutions': {},
    }

    collection.insert_one(data)

    return jsonify({'success': True, 'error': ''})


@app.route('/video/<video_id>', methods=['GET'])
def get_vid_status(video_id):
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    normalized_data = {
        'name': search_result['name'],
        'video_id': search_result['video_id'],
        'username': search_result['username'],
        'resolutions': search_result['resolutions'],
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
            'username': doc['username'],
        }
        data.append(info)
    return jsonify({'success': True, 'error': '', 'data': data})


@app.route('/comment', methods=['PUT'])
def comment():
    video_id = request.form.get('video_id')
    comment = request.form.get('comment')
    username = request.form.get('username')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'vid_id': video_id,
        'username': username,
        'comment': comment,
    })
    send_job('comment', json_packed)
    return jsonify({'success': True, 'error': ''})


@app.route('/like', methods=['POST'])
def like():
    video_id = request.form.get('video_id')
    username = request.form.get('username')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'video_id': video_id,
        'username': username,
        'like': True,
    })

    send_job('like', json_packed)
    return jsonify({'success': True, 'error': ''})


@app.route('/unlike', methods=['POST'])
def unlike():
    video_id = request.form.get('video_id')
    username = request.form.get('username')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return jsonify({'success': False, 'error': 'Video Not Found'})

    json_packed = json.dumps({
        'video_id': video_id,
        'username': username,
        'like': False,
    })

    send_job('like', json_packed)
    return jsonify({'success': True, 'error': ''})


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
    # socketio.run(app, host='0.0.0.0')
