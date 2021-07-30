import asyncio
import logging
from typing import Callable, Coroutine, List, Optional
from urllib.parse import urljoin

from models import Chat
from models import Message
from models import User

logger = logging.getLogger(__name__)


class BaseSaga:

    async def retry(self, step: Callable[..., Coroutine]):
        for retry_turn in range(10):
            try:
                await step()
                logger.info('Saga step %r done', step)
                return
            except Exception as ex:
                logger.error('Saga step %r error %r', step, ex)
                await asyncio.sleep(0.5 + 1.5 ** retry_turn)


class MessageAddSaga(BaseSaga):
    def __init__(self, app, msg: Message) -> None:
        self.chat = None
        self.user = None
        self.msg = msg
        self.app = app
        self.old_unread_messages = 0
        self.new_unread_messages = 0
        self.message_id = None

    async def do(self):
        undo = []
        try:
            await self.collect_data()

            await self.update_counters()
            undo.append(self.update_counters_undo)

            await self.save_message()
            undo.clear()

            await self.retry(self.update_chat_user)
            return True
        except Exception as ex:
            logger.exception('Saga %r failed %r', self, ex)
            logger.info('Saga %r undo %r', self, undo)
            if undo:
                for step in reversed(undo):
                    await self.retry(step)

            return False

    async def collect_data(self):
        self.chat = self.app['chats'][self.msg.chat_key]
        assert len(self.chat.users) == 2, 'пока поддерживаются только peer2peer чаты'
        self.user, = [user for user_id, user in self.chat.users.items() if user.user_id != self.msg.author_id]
        self.old_unread_messages = self.user.unread_message_count
        self.new_unread_messages = self.user.unread_message_count + 1

    async def update_counters(self):
        assert (await update_counter(
            self.app, msg=self.msg, chat=self.chat, user_id=self.user.user_id,
            unread_messages=self.new_unread_messages))

    async def update_counters_undo(self):
        assert (await update_counter(
            self.app, msg=self.msg, chat=self.chat, user_id=self.user.user_id,
            unread_messages=self.old_unread_messages))

    async def save_message(self):
        for shard_id in self.msg.write_shards:
            shard = self.app['shards'][shard_id]
            async with shard.acquire() as conn:
                self.message_id = await self.msg.save(shard_id=shard_id, conn=conn)

    async def update_chat_user(self):
        self.user.unread_message_count = self.new_unread_messages


class MessagesReadSaga(BaseSaga):
    # не помечаем прочитанными если не удается обновить счетчик

    def __init__(self, app, chat: Chat, user: User, message_ids: List[int]) -> None:
        self.chat = chat
        self.user = user
        self.friend = None
        self.app = app
        self.message_ids = message_ids
        self.old_unread_messages = 0
        self.new_unread_messages = 0

    async def do(self):
        undo = []
        try:
            await self.collect_data()

            await self.update_counters()
            undo.append(self.update_counters_undo)

            await self.mark_read()
            undo.clear()

            await self.retry(self.update_chat_user)
            return True
        except Exception as ex:
            logger.exception('Saga %r failed %r', self, ex)
            logger.info('Saga %r undo %r', self, undo)
            if undo:
                for step in reversed(undo):
                    await self.retry(step)

            return False

    async def collect_data(self):
        assert len(self.chat.users) == 2, 'пока поддерживаются только peer2peer чаты'
        self.friend, = [user for user_id, user in self.chat.users.items() if user.user_id != self.user.user_id]
        self.old_unread_messages = self.user.unread_message_count
        self.new_unread_messages = await self.chat.get_unread_message_count(
            self.user.user_id, self.app['shards'], exclude_ids=tuple(self.message_ids))

    async def update_counters(self):
        assert (await update_counter(
            self.app, chat=self.chat, user_id=self.user.user_id, friend_id=self.friend.user_id,
            unread_messages=self.new_unread_messages))

    async def update_counters_undo(self):
        assert (await update_counter(
            self.app, chat=self.chat, user_id=self.user.user_id, friend_id=self.friend.user_id,
            unread_messages=self.old_unread_messages))

    async def mark_read(self):
        write_shards = self.chat.shards.get('write')
        for shard_id in write_shards:
            shard = self.app['shards'].get(shard_id)

            async with shard.acquire() as conn:
                await Message.mark_read_many(self.user.user_id, self.chat.chat_id, self.message_ids, conn)

    async def update_chat_user(self):
        self.user.unread_message_count = self.new_unread_messages


async def update_counter(
        app,
        msg: Optional[Message] = None,
        chat: Optional[Chat] = None,
        user_id: Optional[int] = None,
        friend_id: Optional[int] = None,
        unread_messages: Optional[int] = None,
        unread_messages_add: Optional[int] = None):

    assert msg or chat and user_id and friend_id, 'msg or chat and user_id and friend_id required'
    assert unread_messages is not None or unread_messages_add is not None, (
        'unread_messages or unread_messages_add required')

    counters_rest_url = app.get('counters_rest_url')
    client_session = app.get('client_session')

    if not (counters_rest_url and client_session):
        return True

    counters_rest_url_ = urljoin(counters_rest_url, 'update_counter/')

    chat: Chat = chat or app['chats'].get(msg.chat_id)
    if not chat:
        db = app['db']
        async with db.acquire() as conn:
            chat = await Chat.load(msg.chat_key, conn, app['shards'])

    if not chat:
        return False

    results = []
    user: User = None
    if msg:
        for user_ in chat.users.values():
            if msg and user_.user_id == msg.author_id:
                continue
            user = user_
        counters_update_data = dict(
            user_id=user.user_id,
            friend_id=msg.author_id,
            chat_id=msg.chat_id,
        )
    else:
        user = chat.users[user_id]
        counters_update_data = dict(
            user_id=user.user_id,
            friend_id=friend_id,
            chat_id=chat.chat_id,
        )

    if unread_messages is not None:
        counters_update_data['unread_messages'] = unread_messages
    else:
        counters_update_data['unread_messages'] = user.unread_message_count + unread_messages_add

    resp = await client_session.post(
        counters_rest_url_,
        json=counters_update_data)
    logger.debug('Counters rest response: %r', resp.status)

    success = (await resp.json()).get('success', False)

    results.append(success)

    return all(results)
