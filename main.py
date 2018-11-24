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

app = Flask(__name__)
app.config['videos'] = f'{os.getcwd()}/videos'
CORS(app)
socketio = SocketIO(app)

MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', 27017)
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')

VALID_EXT = ['.mpeg4','.mp4','.avi','.wmv','.mpegps', '.flv', '.3gpp']

mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)

db = mongoClient["my_db"]
collection = db["my_collection"]

LOG = logging
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Utils
def generate_key(size):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for i in range(size))

# Flask Route
@app.route("/")
def hello():
    return "Hello World!"


@app.route("/api/upload_vid", methods=['POST'])
def upload_vid():
    vid_name = request.args.get('filename')
    vid_title = request.args.get('title')
    vid_data = request.get_data()
    md5 = hashlib.md5(vid_data).hexdigest()

    #Check valid vid using ext
    _, ext = os.path.splitext(vid_name)
    print(ext)
    if ext not in VALID_EXT:
        return Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

    #Check keys aleady exist
    keyID= generate_key(6)  
    while collection.count_documents({"vid_id" : keyID}) > 0:
        keyID = generate_key(6)
    
    #Make directory using keyID and save uploaded file
    path = os.path.join(app.config['videos'], f'{keyID}/')
    os.makedirs(path)
    with open(path+vid_name, "wb") as f:
        f.write(vid_data)
    
    #store data in mongodb
    data = {
        'vid_id': keyID,
        'vid_name': vid_name,
        'vid_title' : vid_title,
        'vid_md5' : md5,
        'vid_comments' : [],
        'vid_like' : 0,
        'vid_dislike' : 0,
        }

    collection.insert_one(data)
    
    return Response(status=HTTPStatus.OK)

@app.route('/api/get_vid_status', methods=['GET'])
def get_vid_status():
    vid_id = request.args.get('vid_id')
    search_result = collection.find({'vid_id' : vid_id})
    if search_result.count() == 0:
        return Response(status=HTTPStatus.BAD_REQUEST)
    data = search_result.next()
    comments = data['vid_comments']
    like = data['vid_like']
    dislike = data['vid_dislike']
    json_packed = json.dumps({
        'comments' : comments,
        'like' : like,
        'dislike' : dislike
        })
    return Response(json_packed, status=HTTPStatus.OK)

@app.route('/api/get_all_vid')
def get_all_vid():
    vids = collection.find({})
    json_packed = json.dumps({
        'vids' : str(list(vids))
    })
    print(json_packed)
    return Response(json_packed, status=HTTPStatus.OK)

if __name__ == '__main__':
    app.run()
    # socketio.run(app, host='0.0.0.0')
