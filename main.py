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


def generate_key(size):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for i in range(size))


def hash_key(sid, title):
    concat_data = sid+ time.time() +title
    return hashlib.md5(concat_data).hexdigest()

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

@app.route("/upload", methods=['POST'])
def upload_vid():

    name = request.form.get('name')
    video_id = ''
    sid = ''
    vid_data = request.get_data()
    md5 = hashlib.md5(vid_data).hexdigest()
    # Cannot find the way to get session_id for now

    # Check valid vid using ext
    _, ext = os.path.splitext(name)
    if ext not in VALID_EXT:
        return Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

    # Check keys aleady exist
    keyID = generate_key(6)
    while collection.count_documents({"vid_id": keyID}) > 0:
        keyID = generate_key(6)

    # Make directory using keyID and save uploaded file
    path = os.path.join(app.config['videos'], f'{keyID}/')
    os.makedirs(path)
    with open(path+name, "wb") as f:
        f.write(vid_data)

    # store data in mongodb
    data = {
        'name' : name,
        'video_id' : video_id,
        'sid' : sid,
        'md5' : md5,
        'likes' : [],
        'comments' : [],
        'resolutions' : {},
    }

    collection.insert_one(data)

    return Response(status=HTTPStatus.OK)


@app.route('/video/<video_id>', methods=['GET'])
def get_vid_status(video_id):
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return Response(status=HTTPStatus.BAD_REQUEST)

    normalized_data = {
        'name' : search_result['name'],
        'video_id' : search_result['video_id'], 
        'sid' : search_result['sid'],
        'resolutions' : search_result['resolutions'],
        'likes' : len(search_result['likes']),
        'comments' : search_result['comments'],
    }
    return jsonify({'data' : normalized_data})


@app.route('/video')
def get_all_vid():
    vids = collection.find({})
    data = []
    for doc in vids:
        info = {
            'name' : doc['name'],
            'video_id' : doc['video_id'],
            'sid' : doc['sid'],
        }
        data.append(info)
    return jsonify({'data' : data})


@app.route('/comment', methods=['PUT'])
def comment():
    video_id = request.form.get('video_id')
    comment = request.form.get('comment')
    sid = request.form.get('sid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return Response(status=HTTPStatus.BAD_REQUEST)
    json_packed = json.dumps({
        'vid_id': video_id,
        'sid' : sid,
        'comment': comment,
    })
    send_job('comment', json_packed)
    return Response(json_packed, status=HTTPStatus.OK)


@app.route('/like', methods=['POST'])
def like():
    video_id = request.form.get('video_id')
    sid = request.form.get('sid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return Response(status=HTTPStatus.BAD_REQUEST)
    json_packed = json.dumps({
        'video_id': video_id,
        'sid' : sid,
        'like' : True,
    })
    send_job('like', json_packed)
    return Response(json_packed, status=HTTPStatus.OK)


@app.route('/unlike', methods=['POST'])
def unlike():
    video_id = request.form.get('video_id')
    sid = request.form.get('sid')
    search_result = collection.find_one({'video_id': video_id})
    if search_result == None:
        return Response(status=HTTPStatus.BAD_REQUEST)
    json_packed = json.dumps({
        'video_id': video_id,
        'sid' : sid,
        'like' : False, 
    })
    send_job('like', json_packed)
    return Response(json_packed, status=HTTPStatus.OK)



if __name__ == '__main__':
    app.run(host='0.0.0.0')
    # socketio.run(app, host='0.0.0.0')
