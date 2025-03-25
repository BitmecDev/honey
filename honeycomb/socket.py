import json
from os import environ
import socketio

sio = socketio.Client()

sio.connect('https://socketio.bitmec.com:2096')


def send_socket_message(channel, message):
    sio.emit("publish", {"channel": f"{channel}", "message": json.dumps(message)})