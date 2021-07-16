import asyncio
import base64
from collections import namedtuple
import datetime
import logging
import os

from aiohttp import web
import aiohttp_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import aiomysql
import anyio
from cryptography import fernet
import socketio

from models import Chat
from models import Message
from utils import close_db_pool
from utils import extract_database_credentials

logger = logging.getLogger(__name__)

sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

ChatSession = namedtuple('ChatSession', ['chat_id', 'chat_key', 'user_id', 'q', 'sid'])


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
    read_shards = chat.shards.get('read')

    if not read_shards:
        await sio.emit('error', {'data': 'no read_shards for chat'}, room=sid)
        return

    messages = []
    for shard_id in read_shards:
        shard = app['shards'][shard_id]

        messages.extend(await Message.load_many(shard, chat_id, limit=20))
    messages.sort(key=lambda x: x.get('timestamp'))
    await sio.emit('messages', messages, room=sid)


async def insert_messages_from_queue(messages_queue: asyncio.Queue):
    while True:
        try:
            msg: Message = await messages_queue.get()

            for shard_id in msg.write_shards:
                shard = app['shards'][shard_id]
                async with shard.acquire() as conn:
                    await msg.save(shard_id=shard_id, conn=conn)

            await sio.emit('message', dict(content=msg.content, timestamp=msg.timestamp,
                                           chat_id=msg.chat_id, author_id=msg.author_id),
                           room=msg.chat_key)
            await update_shards_stats()
        except asyncio.CancelledError:
            logger.debug('insert_messages_from_queue canceled')
            break


async def collect_messages(messages_queue: asyncio.Queue):
    while True:
        try:
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
        except asyncio.CancelledError:
            logger.debug('collect_messages canceled')
            break


@sio.event
async def message_add(sid, message):
    if sid not in app['sessions']:
        await sio.emit('error', {'data': 'sid not in sessions'}, room=sid)
        return

    session = app['sessions'][sid]
    chat_id = session.chat_id
    chat_key = session.chat_key

    if not chat_id or not chat_key:
        await sio.emit('error', {'data': 'wrong chat_id in session'}, room=sid)
        return

    chat: Chat = app['chats'][chat_key] or {}
    write_shards = chat.shards.get('write')

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
            chat = await Chat.load(chat_key, conn)
            app['chats'][chat_key] = chat
    else:
        chat = app['chats'][chat_key]

    chat.sessions.add(sid)
    app['sessions'][sid] = ChatSession(
        sid=sid,
        chat_key=chat_key,
        chat_id=chat.chat_id,
        user_id=user_id,
        q=asyncio.Queue()  # limit queue and handle overflow
    )
    sio.enter_room(sid, room=chat_key)
    chat_users = chat.users

    await sio.emit('connected', {
        'users': chat_users
    }, room=sid)

    logger.debug('Client connected %r', sid)


async def update_shards_stats():
    stats = {}
    for shard_id, shard in app['shards'].items():
        async with shard.acquire() as conn:
            cur: aiomysql.cursors.DictCursor
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # await cur.execute('select coalesce(max(id), 0) as max_id from chat_message')
                await cur.execute(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES "
                    "WHERE TABLES.TABLE_SCHEMA = 'socnet' AND TABLE_NAME = 'chat_message'")
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

    chat = app['chats'].get(session.chat_key)
    if not chat:
        return

    chat_sessions: set = chat.sessions
    chat_sessions.discard(sid)
    if not chat_sessions:
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

    app.on_shutdown.append(stop_tasks)
    app.on_shutdown.append(stop_sessions)

    pool = await aiomysql.create_pool(
        **extract_database_credentials(database_url),
        maxsize=50,
        autocommit=True)
    app['db'] = pool
    app.on_shutdown.append(lambda _app: close_db_pool(_app['db']))

    app['shards'] = {}

    for i in range(1, int(os.getenv('SHARDS_COUNT', 0)) + 1):
        shard_url = os.getenv(f'SHARD_{i}_URL')
        if not shard_url:
            continue
        pool = await aiomysql.create_pool(
            **extract_database_credentials(shard_url),
            maxsize=20,
            autocommit=True)
        app['shards'][i] = pool
        await migrate_schema(pool)
        logger.debug('pool init: %r', pool)

        def make_closer(pool):
            async def close_shard(app):
                await close_db_pool(pool)
            return close_shard

        app.on_shutdown.append(make_closer(pool))

    app['sessions'] = {}
    # app['users'] = {}
    app['chats'] = {}
    app['tasks'] = []
    app.on_startup.append(start_background_task)

    # app.on_shutdown.append(close_shards)
    return app


async def close_shards(app):
    logger.debug('close_shards')

    pool: aiomysql.pool.Pool
    for pool in app['shards'].values():
        # logger.debug('%r', pool.close())
        # logger.debug('%r', await pool.wait_closed())
        logger.debug('pool %r closed %r', pool, pool._closed)


async def stop_sessions(app):
    logger.debug('stop_sessions')
    session: ChatSession
    for session in app['sessions'].values():
        await sio.disconnect(session.sid)
    logger.debug('stop_sessions [OK]')


async def start_background_task(app):
    logger.debug('start_background_task')
    messages_queue = asyncio.Queue()
    app['tasks'].append(asyncio.create_task(insert_messages_from_queue(messages_queue)))
    app['tasks'].append(asyncio.create_task(collect_messages(messages_queue)))

    # app['tasks'].append(asyncio.create_task(background_task(app)))


async def stop_tasks(app):
    logger.debug('stoping tasks')
    t: asyncio.Task
    for t in app['tasks']:
        logger.debug('cancel task: %r', t)
        t.cancel()
        await t
        logger.debug('cancel task: %r [OK]', t)

    # await asyncio.gather(*app['tasks'])
    logger.debug('stoping tasks finished')


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


async def run_app(port, host='0.0.0.0'):
    try:
        runner = web.AppRunner(await make_app())
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()

        import signal
        loop = asyncio.get_event_loop()
        state = dict(running=True)

        def stop():
            async def async_stop():
                await runner.cleanup()
                state['running'] = False
            asyncio.create_task(async_stop())

        loop.add_signal_handler(signal.SIGTERM, stop)

        while state['running']:
            try:
                await asyncio.sleep(3)
            except (asyncio.CancelledError, KeyboardInterrupt):
                break
        logger.debug('shutdown')
        await runner.cleanup()

        return runner
    except asyncio.CancelledError as ex:
        logger.exception('run_app: %r', ex)
    except Exception as ex:
        logger.exception('run_app: %r', ex)


def main():
    logging.basicConfig(level=os.getenv('LOG_LEVEL', logging.DEBUG))
    web.run_app(make_app(), port=int(os.getenv('PORT', 8080)))
    # anyio.run(run_app, int(os.getenv('PORT', 8080)))
    # runner: web.AppRunner = asyncio.run(run_app(port=int(os.getenv('PORT', 8080))), debug=True)
    # logger.debug('App shutdown')
    # asyncio.run(runner.cleanup())
    logger.debug('App stopped')

if __name__ == '__main__':
    main()
