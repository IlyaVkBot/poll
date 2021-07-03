import asyncio
import aioschedule

import yaml
import time
import os

from copy import deepcopy
from datetime import datetime

from pathlib import Path

from pyrogram import Client
from pyrogram.types import InputMediaDocument
from pyrogram.raw import functions
from pyrogram.raw.types import InputMessagesFilterEmpty

LIMIT = 600
POLLS_IDS = [929897]
CHAT_ID = -1001518322456


BLOCKLIST = {
    "Евгения Колташова": 1028275690,
    "Александр": 1845006993,
    "Товарищ Троцкий": 1395767435,
    "Prount Goodday": 493169260,
    "Daniel Zakharov": 795449748,
    "Наиль Гумбатов": 719073935,
    "Vyacheslaw Udintsev": 387290727,
    "Ever": 129618541
}


class Config:
    def __init__(self, config_path="config.yml", **kwargs):
        try:
            with open(config_path, encoding="UTF-8") as file:
                self.config = yaml.full_load(file.read())
        except Exception:
            self.config = {}

        self.environ = os.environ

        for param in kwargs:
            value = self.get(param, default=kwargs[param])

    def get(self, param, default=None):
        globals()[param.upper()] = (
            self.environ.get(param.upper()) or
            self.config.get(param, default))


config_path = os.environ.get("CONFIG_PATH", "config.yml")
Config(
    api_id=None,
    api_hash=None,
    path=""
)


async def get_msg_count(client, chat, user):
    user = await client.resolve_peer(user)

    return await client.send(
      functions.messages.Search(peer=chat, from_id=user,
                                q="", add_offset=0, limit=0, max_id=0, 
                                min_id=0, hash=0, min_date=0,
                                max_date=0, offset_id=0,
                                filter=InputMessagesFilterEmpty()))


async def get_poll(client, chat, poll_id, offset=""):
    return await client.send(
        functions.messages.GetPollVotes(peer=chat, id=poll_id, limit=10000,
                                        offset=offset))


async def calc_poll_results(client, chat, options, votes, users):
    votes_cleared = [[] for _ in range(len(options))]
    votes_dirty = [[] for _ in range(len(options))]

    for user in votes:
        user = votes[user]

        try:
            count = (await get_msg_count(client, chat, user["user_id"])).count
        except Exception:
            time.sleep(15)
            count = (await get_msg_count(client, chat, user["user_id"])).count

        user_info = users[user["user_id"]]

        if user["user_id"] != user_info.id:
            user_info = await client.get_users(user["user_id"])

        try:
            username = " ".join([user_info.first_name, user_info.last_name]) 
        except Exception:
            username = user_info.first_name

        voter = {
                    "username": username,
                    "user_id": user["user_id"],
                    "count": count
                }

        if count >= LIMIT:
            for option in user["options"]:
                if BLOCKLIST[options[option].text] != user["user_id"]:
                    votes_cleared[option].append(voter)
        else:
            for option in user["options"]:
                votes_dirty[option].append(voter)

    return votes_cleared, votes_dirty


def get_calc_log(options, votes, votes_dirty):
    message = ""

    s = "    "
    s2 = "        "

    for num, _ in enumerate([votes, votes_dirty]):
        message += ["Прошедшие отсев\n\n", "Отсеянные\n\n"][num]

        for num, option in enumerate(_):
            message += s + f"{options[num].text.upper()} ({len(option)})\n"

            for user in sorted(option, key=lambda x: x["count"])[::-1]:
                message += s2 + user["username"] + " - " + str(user["count"]) + "\n"
            
            message += "\n"

    return message


async def save_log(client, log, options, votes):
    now = datetime.now()
    name = f"ВЫБОРЫ {now.hour:02}:{now.minute:02}.txt"

    with open(PATH + name, "w", encoding="UTF-8") as file:
        file.write(log)

    results = [[options[num].text, len(_)] for num, _ in enumerate(votes)]
    results = sorted(results, key=lambda x: x[1])[::-1]
    caption = f"<b>ПРЕДВАРИТЕЛЬНЫЕ ИТОГИ ВЫБОРОВ НА {now.hour:02}:{now.minute:02}</b>\n\n"

    for num, _ in enumerate(results):
        caption += f"<i>{num + 1}.</i> <b>{_[0]}</b> <code>({_[1]})</code>\n"

    await client.send_media_group(CHAT_ID, [
        InputMediaDocument(name, caption=caption, parse_mode="HTML")
    ])

    os.remove(name.split(":")[0])


async def get_full_poll(client, chat_id, poll_id):
    chat = await client.resolve_peer(chat_id)
    poll = await client.get_messages(chat_id, poll_id, replies=0)
    options = poll.poll.options

    poll_results = await get_poll(client, chat, poll_id)

    COUNT = poll_results.count
    count = len(poll_results.votes)

    while count != COUNT:
        new_poll = await get_poll(client, chat, poll_id,
                                  poll_results.next_offset)

        COUNT = new_poll.count

        poll_results.votes.extend(new_poll.votes)
        poll_results.users.extend(new_poll.users)
        poll_results.next_offset = new_poll.next_offset

        count += len(new_poll.votes)

    return chat, poll, poll_results


async def get_clear_poll():
    chat = await client.resolve_peer(chat_id)
    poll = await client.get_messages(chat_id, poll_id, replies=0)
    options = poll.poll.options

    poll_results = await get_poll(client, chat, poll_id)

    COUNT = poll_results.count
    count = len(poll_results.votes)

    while count != COUNT:
        new_poll = await get_poll(client, chat, poll_id,
                                  poll_results.next_offset)

        COUNT = new_poll.count

        poll_results.votes.extend(new_poll.votes)
        poll_results.users.extend(new_poll.users)
        poll_results.next_offset = new_poll.next_offset

        count += len(new_poll.votes)

    return chat, poll, poll_results


def combine_polls(poll, poll2, results, results2):
    k = len(poll.poll.options)

    # extend options
    for num, opt in enumerate(poll2.poll.options):
        poll2.poll.options[num].data = int(opt.data) + k

    poll.poll.options.extend(poll2.poll.options)

    # extend user votes
    votes = deepcopy(results)
    _votes = {}
    _users = {}

    for num, vote in enumerate(results.votes):
        _votes[vote.user_id] = {
            "user_id": vote.user_id,
            "options": [int(vote.option)] if vote.QUALNAME == "types.MessageUserVote" else [int(_) for _ in vote.options]
        }
        _users[vote.user_id] = results.users[num]

    for vote in results2.votes:
        if _votes.get(vote.user_id) is None:
            _votes[vote.user_id] = {
                "user_id": vote.user_id,
                "options": [int(vote.option) + k] if vote.QUALNAME == "types.MessageUserVote" else [int(_) + k for _ in vote.options]
            }
            _users[vote.user_id] = results.users[num]
        else:
            _votes[vote.user_id]["options"].extend([int(vote.option) + k] if vote.QUALNAME == "types.MessageUserVote" else [int(_) + k for _ in vote.options])

    return poll, _votes, _users


def prepare_one_poll(poll, results):
    k = len(poll.poll.options)

    # extend user votes
    votes = deepcopy(results)
    _votes = {}
    _users = {}

    for num, vote in enumerate(results.votes):
        _votes[vote.user_id] = {
            "user_id": vote.user_id,
            "options": [int(vote.option)] if vote.QUALNAME == "types.MessageUserVote" else [int(_) for _ in vote.options]
        }
        _users[vote.user_id] = results.users[num]

    return poll, _votes, _users


async def main():
    client = Client("schizo", API_ID, API_HASH)
    await client.start()

    aioschedule.every(30).minutes.do(startpoll, client)

    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(10)


async def startpoll(client):
    chat, poll, poll_results = await get_full_poll(client, "@katz_bots", POLLS_IDS[0])
    # chat, poll2, poll_results2 = await get_full_poll(client, "@katz_bots", POLLS_IDS[1])
    chat2, poll2, poll_results2 = {}, {}, {}

    poll, votes, users = prepare_one_poll(poll, poll_results)

    # poll, votes, users = combine_polls(poll, poll2, poll_results,
    #                                    poll_results2)

    votes_cleared, votes_dirty = await calc_poll_results(client,
                                                         chat,
                                                         poll.poll.options,
                                                         votes, users)

    message = get_calc_log(poll.poll.options, votes_cleared, votes_dirty)
    await save_log(client, message, poll.poll.options, votes_cleared)


asyncio.run(main())
