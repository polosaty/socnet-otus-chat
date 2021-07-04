import asyncio

from aiohttp import web

import socketio

sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

messages = [{
  'id': 0,
  'authorId': "perborgen",
  'content': "who'll win?"
}]

async def background_task():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        await sio.sleep(10)
        count += 1
        await sio.emit('message', {
                          'id': count,
                          'authorId': "perborgen",
                          'content': "who'll win?"
                        })


# async def index(request):
#     with open('app.html') as f:
#         return web.Response(text=f.read(), content_type='text/html')



# @sio.event
# async def message_get(sid):
#     await sio.emit('messages', messages)


@sio.event
async def message_add(sid, message):
    messages.append(message)
    await sio.emit('chat_response', {'data': message['content']}, room=sid)
    await sio.emit('message', message, room=sid)



@sio.event
async def my_event(sid, message):
    await sio.emit('chat_response', {'data': message['data']}, room=sid)


@sio.event
async def my_broadcast_event(sid, message):
    await sio.emit('chat_response', {'data': message['data']})


@sio.event
async def join(sid, message):
    sio.enter_room(sid, message['room'])
    await sio.emit('chat_response', {'data': 'Entered room: ' + message['room']},
                   room=sid)


@sio.event
async def leave(sid, message):
    sio.leave_room(sid, message['room'])
    await sio.emit('chat_response', {'data': 'Left room: ' + message['room']},
                   room=sid)


@sio.event
async def close_room(sid, message):
    await sio.emit('chat_response',
                   {'data': 'Room ' + message['room'] + ' is closing.'},
                   room=message['room'])
    await sio.close_room(message['room'])


@sio.event
async def my_room_event(sid, message):
    await sio.emit('chat_response', {'data': message['data']},
                   room=message['room'])


@sio.event
async def disconnect_request(sid):
    await sio.disconnect(sid)


@sio.event
async def connect(sid, environ):
    await sio.emit('chat_response', {'data': 'Connected', 'count': 0}, room=sid)


@sio.event
def disconnect(sid):
    print('Client disconnected')


# app.router.add_static('/static', 'static')
# app.router.add_get('/', index)


if __name__ == '__main__':
    sio.start_background_task(background_task)
    web.run_app(app)
