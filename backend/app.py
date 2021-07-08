import asyncio
import base64
import datetime
from collections import namedtuple
from typing import Dict
from typing import List

import aiomysql
import anyio
from aiohttp import web
import socketio
from utils import extract_database_credentials, close_db_pool
import logging
from cryptography import fernet
import aiohttp_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import os
logger = logging.getLogger(__name__)


sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)


Message = namedtuple('Message', ['write_shards', 'timestamp', 'author_id', 'chat_id', 'content', 'chat_key'])
ChatSession = namedtuple('ChatSession', ['chat_id', 'chat_key', 'user_id', 'q', 'sid'])
Chat = namedtuple('Chat', ['chat_id', 'users', 'sessions', 'shards'])


@sio.event
async def message_get(sid):
    if sid not in app['sessions']:
        await sio.emit('error', {'data': 'sid not in sessions'}, room=sid)
        return

    session: ChatSession = app['sessions'][sid]
    chat_id = session.chat_id
    chat_key = session.chat_key

    if not chat_id or not chat_key:
        await sio.emit('error', {'data': 'wrong chat_id in session'}, room=sid)
        return

    chat = app['chats'][chat_key] or {}
    chat_shards = chat.get('shards', {})
    read_shards = chat_shards.get('read')

    if not read_shards:
        await sio.emit('error', {'data': 'no read_shards for chat'}, room=sid)
        return
    messages = []
    for shard_id in read_shards:
        shard = app['shards'][shard_id]

        async with shard.acquire() as conn:
            cur: aiomysql.cursors.DictCursor
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%dT%%TZ') as timestamp, author_id, content, chat_id "
                    "FROM chat_message "
                    "WHERE chat_id = %(chat_id)s ORDER BY timestamp desc LIMIT 20",
                    dict(shard_id=chat_id, chat_id=chat_id)
                )
                messages.extend(await cur.fetchall())
    messages.sort(key=lambda x: x.get('timestamp'))
    await sio.emit('messages', messages, room=sid)


async def insert_messages_from_queue(messages_queue: asyncio.Queue):
    while True:
        msg: Message = await messages_queue.get()

        for shard_id in msg.write_shards:
            shard = app['shards'][shard_id]
            async with shard.acquire() as conn:
                cur: aiomysql.cursors.Cursor
                async with conn.cursor() as cur:
                    await cur.execute(
                        'INSERT INTO chat_message(shard_id, chat_id, timestamp, author_id, content) '
                        'VALUES(%(shard_id)s, %(chat_id)s, %(timestamp)s, %(author_id)s, %(content)s) ',
                        dict(shard_id=shard_id, chat_id=msg.chat_id, timestamp=msg.timestamp, author_id=msg.author_id,
                             content=msg.content)
                    )

        # await sio.emit('message', message, room=sid)
        await sio.emit('message', dict(content=msg.content, timestamp=msg.timestamp,
                                       chat_id=msg.chat_id, author_id=msg.author_id),
                       room=msg.chat_key)
        await update_shards_stats()


async def collect_messages(messages_queue: asyncio.Queue):
    while True:
        message = None
        session: ChatSession
        for session in app['sessions'].values():
            message = None

            try:
                message = session.q.get_nowait()
                messages_queue.put_nowait(message)
                messages_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            except asyncio.QueueFull:
                session.q.put_nowait(message)
        if not message:
            # чтобы при пустых очередях не съесть 100% CPU
            await asyncio.sleep(1)


@sio.event
async def message_add(sid, message):
    # TODO: queue

    if sid not in app['sessions']:
        await sio.emit('error', {'data': 'sid not in sessions'}, room=sid)
        return

    session = app['sessions'][sid]
    chat_id = session.chat_id
    chat_key = session.chat_key

    if not chat_id or not chat_key:
        await sio.emit('error', {'data': 'wrong chat_id in session'}, room=sid)
        return

    chat = app['chats'][chat_key] or {}
    chat_shards = chat.get('shards', {})
    write_shards = chat_shards.get('write')

    if not write_shards:
        await sio.emit('error', {'data': 'no write_shards for chat'}, room=sid)
        return

    author_id = session.user_id

    timestamp = datetime.datetime.utcnow().isoformat()
    msg = Message(
        write_shards=write_shards,
        timestamp=timestamp,
        author_id=author_id,
        chat_id=chat_id,
        content=message['content'],
        chat_key=chat_key)

    q: asyncio.Queue = session.q
    await q.put(msg)


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
async def disconnect_request(sid):
    await sio.disconnect(sid)


async def disconect_with_error(sid, error):
    await sio.emit('error', {'data': error}, room=sid)
    await sio.disconnect(sid)


@sio.event
async def connect(sid, environ):
    chat_key = environ['aiohttp.request'].query.get('chat_key')
    session = environ['aiohttp.request'].query.get('session')
    if not (chat_key and session):
        return await disconect_with_error(sid, 'chat_key and session required')

    # check session and get user id
    user_session = await aiohttp_session.get_session(environ['aiohttp.request'])
    if not user_session:
        return await disconect_with_error(sid, 'wrong session')

    user_id = user_session['uid']


    db = app['db']

    if chat_key not in app['chats']:
        async with db.acquire() as conn:
            cur: aiomysql.cursors.DictCursor
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT username, firstname, lastname, u.id as user_id, chat_id "
                    "FROM user u "
                    "JOIN chat_user cu on cu.user_id = u.id "
                    "JOIN chat c on c.id = cu.chat_id "
                    "WHERE c.key = %(chat_key)s",
                    dict(chat_key=chat_key)
                )

                for row in await cur.fetchall():
                    if chat_key not in app['chats']:
                        app['chats'][chat_key] = {
                            'chat_id': row['chat_id'],
                            'users': {},
                            'sessions': set(),
                            'shards': await get_shards(row['chat_id'], conn=conn)
                        }
                    else:
                        app['chats'][chat_key]['sessions'].add(sid)

                    chat = app['chats'][chat_key]
                    chat_user_id = row['user_id']
                    # if chat_user_id not in app['users']:
                    #     app['users'][chat_user_id] = {
                    #         'firstname': row['firstname'],
                    #         'lastname': row['lastname'],
                    #         'username': row['username'],
                    #     }

                    chat['users'][chat_user_id] = {
                        'firstname': row['firstname'],
                        'lastname': row['lastname'],
                        'username': row['username'],
                    }

    app['sessions'][sid] = ChatSession(
        sid=sid,
        chat_key=chat_key,
        chat_id=app['chats'][chat_key]['chat_id'],
        user_id=user_id,
        q=asyncio.Queue()  # limit queue and handle overflow
    )
    sio.enter_room(sid, room=chat_key)
    chat_users = app['chats'].get(chat_key, {}).get('users', {})

    await sio.emit('connected', {
        'users': chat_users
    }, room=sid)

    logger.debug('Client connected %r', sid)


async def get_shards(chat_id, conn=None) -> Dict[str, List[int]]:
    shards = {'read': [], 'write': []}
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            'SELECT shard_id, `read`, `write` FROM chat_shard WHERE chat_id = %(chat_id)s',
            dict(chat_id=chat_id)
        )
        for shard in await cur.fetchall():
            if shard['read']:
                shards['read'].append(shard['shard_id'])
            if shard['write']:
                shards['write'].append(shard['shard_id'])

    return shards


async def update_shards_stats():
    stats = {}
    for shard_id, shard in app['shards'].items():
        async with shard.acquire() as conn:
            cur: aiomysql.cursors.DictCursor
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # await cur.execute('select coalesce(max(id), 0) as max_id from chat_message')
                await cur.execute(
                    "select TABLE_ROWS from information_schema.TABLES "
                    "where TABLES.TABLE_SCHEMA = 'socnet' and TABLE_NAME = 'chat_message'")
                stats[shard_id] = (await cur.fetchone())['TABLE_ROWS']

    if stats:
        async with app['db'].acquire() as conn:
            cur: aiomysql.cursors.Cursor
            async with conn.cursor() as cur:
                for shard_id, size in stats.items():
                    await cur.execute(
                        "UPDATE shard SET size = %(size)s "
                        " WHERE id = %(shard_id)s",
                       dict(shard_id=shard_id, size=size)
                    )


@sio.event
def disconnect(sid):
    session: ChatSession = app['sessions'].pop(sid, None)
    logger.debug('Client disconnected %r', sid)

    # clear chat if empty
    if not session:
        return

    chat = app['chats'].get(session.chat_key, {})
    if not chat:
        return

    chat_sessions: set = chat.get('sessions', set())
    chat_sessions.discard(sid)
    if not chat_sessions and chat:
        app['chats'].pop(session.chat_key)
        logger.debug('Clear empty chat %r', session.chat_key)


class EncryptedSessionStorage(EncryptedCookieStorage):
    def load_cookie(self, request):
        return request.query.get('session')


async def migrate_schema(pool):
    conn: aiomysql.connection.Connection
    async with pool.acquire() as conn:
        cur: aiomysql.cursors.Cursor
        async with conn.cursor() as cur:
            try:
                await cur.execute("SELECT 1 FROM chat_message LIMIT 1")
                await cur.fetchone()
            except Exception:
                with open("shard_schema.sql") as f:
                    schema = f.read()
                    await cur.execute(schema)


async def make_app():
    database_url = os.getenv('DATABASE_URL', None)

    fernet_key = os.getenv('FERNET_KEY', fernet.Fernet.generate_key())
    secret_key = base64.urlsafe_b64decode(fernet_key)
    aiohttp_session.setup(app, EncryptedSessionStorage(secret_key))
    # sio.start_background_task(background_task)

    pool = await aiomysql.create_pool(
        **extract_database_credentials(database_url),
        maxsize=50,
        autocommit=True)
    app['db'] = pool
    app.on_shutdown.append(lambda _app: close_db_pool(_app['db']))

    app['shards'] = {}

    for i in range(int(os.getenv('SHARDS_COUNT', 0))):
        shard_url = os.getenv(f'SHARD_{i + 1}_URL')
        if not shard_url:
            continue
        pool = await aiomysql.create_pool(
            **extract_database_credentials(shard_url),
            maxsize=20,
            autocommit=True)
        app['shards'][i + 1] = pool
        await migrate_schema(pool)
        app.on_shutdown.append(lambda _app: close_db_pool(pool))

    app['sessions'] = {}
    # app['users'] = {}
    app['chats'] = {}
    app['tasks'] = []
    app.on_startup.append(start_background_task)
    app.on_shutdown.append(stop_tasks)
    return app


async def start_background_task(app):
    app['tasks'].append(asyncio.create_task(background_task(app)))


async def stop_tasks(app):
    t: asyncio.Task
    for t in app['tasks']:
        t.cancel()

    await asyncio.gather(*app['tasks'])


async def background_task(app):
    messages_queue = asyncio.Queue()
    while True:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(insert_messages_from_queue, messages_queue)
                tg.start_soon(collect_messages, messages_queue)
        except asyncio.CancelledError:
            logger.info('background_task canceled')
            return
        except Exception:
            logger.exception('Exception in background task', exc_info=True)


def main():
    logging.basicConfig(level=os.getenv('LOG_LEVEL', logging.DEBUG))
    web.run_app(make_app(), port=int(os.getenv('PORT', 8080)))


if __name__ == '__main__':
    main()
