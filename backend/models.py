from collections import namedtuple
from typing import Dict, Iterable, List

import aiomysql

BaseMessage = namedtuple('Message', ['write_shards', 'timestamp', 'author_id', 'chat_id', 'content', 'chat_key'])
BaseChat = namedtuple('Chat', ['chat_id', 'users', 'sessions', 'shards'])
BaseUser = namedtuple('User', ['firstname', 'lastname', 'username', 'user_id'])


class User(BaseUser):
    pass


class Message(BaseMessage):

    async def save(self, shard_id, conn):
        cur: aiomysql.cursors.Cursor
        async with conn.cursor() as cur:
            await cur.execute(
                'INSERT INTO chat_message(shard_id, chat_id, timestamp, author_id, content) '
                'VALUES (%(shard_id)s, %(chat_id)s, %(timestamp)s, %(author_id)s, %(content)s) ',
                dict(shard_id=shard_id, chat_id=self.chat_id, timestamp=self.timestamp,
                     author_id=self.author_id, content=self.content)
            )

    @classmethod
    async def save_many(cls, shard_id, msgs: Iterable['Message'], conn):
        cur: aiomysql.cursors.Cursor
        async with conn.cursor() as cur:
            await cur.executemany(
                'INSERT INTO chat_message(shard_id, chat_id, timestamp, author_id, content) '
                'VALUES (%(shard_id)s, %(chat_id)s, %(timestamp)s, %(author_id)s, %(content)s) ',
                [dict(shard_id=shard_id, chat_id=msg.chat_id, timestamp=msg.timestamp,
                      author_id=msg.author_id, content=msg.content)
                 for msg in msgs]
            )

    @classmethod
    async def load_many(cls, pool, chat_id, limit=20, before_timestamp=None):
        # TODO: реализовать пагинацию
        before_timestamp_sql = ''
        if before_timestamp:
            pass
        async with pool.acquire() as conn:
            cur: aiomysql.cursors.DictCursor
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%dT%%TZ') AS timestamp, author_id, content, chat_id "
                    f"FROM chat_message "
                    f"WHERE chat_id = %(chat_id)s {before_timestamp_sql} "
                    f"ORDER BY timestamp DESC LIMIT %(limit)s",
                    dict(chat_id=chat_id, limit=limit)
                )
                return await cur.fetchall()


class Chat(BaseChat):

    @staticmethod
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

    @classmethod
    async def load(cls, chat_key, conn) -> 'Chat':
        chat = None
        cur: aiomysql.cursors.DictCursor
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT username, firstname, lastname, u.id AS user_id, chat_id "
                "FROM user u "
                "JOIN chat_user cu ON cu.user_id = u.id "
                "JOIN chat c ON c.id = cu.chat_id "
                "WHERE c.key = %(chat_key)s",
                dict(chat_key=chat_key)
            )

            for row in await cur.fetchall():
                if not chat:
                    chat = Chat(
                        chat_id=row['chat_id'],
                        users={},
                        sessions=set(),
                        shards=await cls.get_shards(row['chat_id'], conn=conn),
                    )

                chat_user_id = row['user_id']
                chat.users[chat_user_id] = User(
                    user_id=chat_user_id,
                    firstname=row['firstname'],
                    lastname=row['lastname'],
                    username=row['username'],
                )
        return chat

    @classmethod
    async def get_or_create(cls, user_id, friend_id, conn: aiomysql.Connection):
        # pool: aiomysql.pool.Pool = request.app['db_pool']
        # conn: aiomysql.Connection
        # async with pool.acquire() as conn:
        cur: aiomysql.cursors.DictCursor
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                'SELECT chat_id FROM chat_user cu WHERE user_id = %(friend_id)s '
                ' AND EXISTS(SELECT 1 FROM chat_user WHERE user_id = %(uid)s '
                ' AND chat_id = cu.chat_id) '
                ' ORDER BY chat_id DESC LIMIT 1',
                dict(
                    friend_id=friend_id,
                    uid=user_id
                )
            )
            chat_row = await cur.fetchone()
            if chat_row:
                chat_id = chat_row['chat_id']
            else:
                # smallest_shard_id get by max(chat_message.id) from shards
                await cur.execute("SELECT id FROM shard ORDER BY size LIMIT 1")
                smallest_shard_id = (await cur.fetchone())['id']

                await conn.begin()
                await cur.execute(
                    "INSERT INTO chat (`type`, `key`) VALUES ('peer2peer', uuid()); "
                )
                chat_id = cur.lastrowid
                await cur.execute(
                    "INSERT INTO chat_user (user_id, chat_id) "
                    " VALUES (%(friend_id)s, %(chat_id)s), (%(uid)s, %(chat_id)s);",
                    dict(
                        friend_id=friend_id,
                        uid=user_id,
                        chat_id=chat_id
                    )
                )
                await cur.execute(
                    "INSERT INTO chat_shard (chat_id, shard_id, `read`, `write`) "
                    "VALUES (%(chat_id)s, %(shard_id)s, 1, 1);",
                    dict(
                        shard_id=smallest_shard_id,
                        chat_id=chat_id
                    )
                )
                await conn.commit()

            await cur.execute("SELECT `key` FROM chat WHERE id = %(chat_id)s", dict(chat_id=chat_id))
            chat_key = (await cur.fetchone())['key']

        return chat_key
