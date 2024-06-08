import asyncio
import aioschedule
import aiosqlite

import yaml
import json
import time
import os

from copy import deepcopy
from datetime import datetime

from pathlib import Path

from pyrogram import Client
from pyrogram.types import InputMediaDocument
from pyrogram.raw import functions
from pyrogram.raw.types import InputMessagesFilterEmpty
from pyrogram import enums

from dotenv import load_dotenv
load_dotenv()

LIMIT = int(os.getenv("LIMIT"))
TIME_LIMIT = int(os.getenv("TIME_LIMIT"))
POLLS_IDS = json.loads(os.getenv("POLLS_IDS").replace("'", ""))
POLLS_IDS_REPEAT = json.loads(os.getenv("POLLS_IDS_REPEAT").replace("'", ""))
try: CHAT_ID = int(os.getenv("SEND_CHAT_ID"))
except: CHAT_ID = os.getenv("SEND_CHAT_ID")
try: POLL_CHAT_ID = int(os.getenv("POLL_CHAT_ID"))
except: POLL_CHAT_ID = os.getenv("POLL_CHAT_ID")

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()

# inactive
BLOCKLIST = {
    "Ника": 815423834,
    "Grajdanin Svoboda": 284419593,
    "Aaron Kagan": 1191726271,
    "Sergey Plotnikov": 1017092559,
    "Sergey": 135066376,
    "Георгий Тимофеевский": 98736263,
    "Снимщиков Илья": 344316097,
    "Аня Сапронова": 799774740,
    "Товарищ Троцкий": 1395767435,
    "Nikolai Mareev": 127535925,
    "Георгий Глуховский": 1575820809,
    "Svetlana": 1601862671,
    "Vyacheslaw Udintsev": 387290727,
    "Артем Семин": 220139586,
    "Gottlieb Hoffmann": 365996935,
    "Crash Bandicoot": 104489510,
    "Бубахан Бабаев": 430270337,
    "O_o (@SaturnZoda)": 523131471,
    "Павел Простой": 1072318246,
    "Enot": 380850112,
    "Наиль Гумбатов": 719073935,
    "Максим Лыпкань": 920397947,
    "Dmitry (@Tadimon)": 387565571,
    "S (@LanaNikiforowa)": 1292397566,
    "Антон (@Ahnenschrein)": 136187093,
    "Евгений Румянцев": 62411782,
    "Faber Ion": 1543095684,
    "Георгий Попов": 652652360,
    "Prount Godday": 493169260,
    "коля (@kolyaTch31)": 1128006859
}

BLOCKED = [650030828, 
           98736263,
           #380850112, #Енот,
           1117540782, #йуля
           214816035, #пачирису
	   1812649478, #Выборы
           ]
ALLOWED = []

OLD_ALLOWED= [169790456, 
           330339396, 
           1320606352, 
           141058218, 
           1237659719, 
           # 1117540782, #йуля 
           503696074, 
           819865210, 
           352356942, 
           315838946, 
           1813762392, 
           135066376,
           5112529401, #Я ем
           443012478, #Сандри
           1152211694, #Нет
           1926801217, #Лыпкань
           1926801217, #Владики
           370385610, #Юрия Логинова
           1017092559, #Плотников
           1399298420, #Дзи
           ]

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
    poll_path=""
)


async def get_msg_count(client, chat, user):
    async with aiosqlite.connect("db/bot.db") as db:
        cursor = await db.execute( f"""SELECT SUM(message_count) as mc
                    FROM message_counter
                    WHERE user_id = {user} and timestamp>={TIME_LIMIT}
		    GROUP BY user_id""" )
        row = await cursor.fetchone() 
        try:
            return row[0]
        except:
            return 0
    return 0

    return await client.invoke(
      functions.messages.Search(peer=chat, from_id=user,
                                q="", add_offset=0, limit=0, max_id=0, 
                                min_id=0, hash=0, min_date=0,
                                max_date=0, offset_id=0,
                                filter=InputMessagesFilterEmpty()))


async def get_poll(client, chat, poll_id, offset=""):
    return await client.invoke(
        functions.messages.GetPollVotes(peer=chat, id=poll_id, limit=10000,
                                        offset=offset))


async def calc_poll_results(client, chat, options, votes, users):
    votes_cleared = [[] for _ in range(len(options))]
    votes_dirty = [[] for _ in range(len(options))]

    for user in votes:
        user = votes[user]

        try:
            count = (await get_msg_count(client, chat, user["user_id"]))
        except Exception:
            time.sleep(15)
            count = (await get_msg_count(client, chat, user["user_id"]))

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

        if user["user_id"] not in BLOCKED and (count >= LIMIT or user["user_id"] in ALLOWED):
            for option in user["options"]:
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
    name = f"ВЫБОРЫ" + (f"{now.hour:02}{now.minute:02}" if now.hour + now.minute > 0 else "") + ".txt"

    with open(POLL_PATH + name, "w", encoding="UTF-8") as file:
        file.write(log)

    results = [[options[num].text, len(_)] for num, _ in enumerate(votes)]
    results = sorted(results, key=lambda x: x[1])[::-1]
    caption = f"<b>ПРЕДВАРИТЕЛЬНЫЕ ИТОГИ ВЫБОРОВ" + (f" НА {now.hour:02}:{now.minute:02}" if now.hour + now.minute > 0 else "") + "</b>\n\n"

    for num, _ in enumerate(results):
        _[0] = _[0].replace("@", "@\u200c").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        caption += f"<i>{num + 1}.</i> <b>{_[0]}</b> <code>({_[1]})</code>\n"
    
    if type(POLL_CHAT_ID) is int:
        chat_link = f"https://t.me/c/{POLL_CHAT_ID}/".replace("-100", "")
    else:
        chat_link = f"https://t.me/" + POLL_CHAT_ID.replace("@", "") + "/"

    caption += f"\n<a href=\"{chat_link}{POLLS_IDS[0]}\">Проголосовать</a>"

    await client.send_media_group("-1001176998310", [
        InputMediaDocument(name, caption=caption, parse_mode=enums.ParseMode.HTML)
    ])

    os.remove(name.split(":")[0])


async def get_full_poll(client, chat_id, poll_id):
    chat, poll, poll_results = await get_individual_poll(client, chat_id, poll_id)
    if str(poll_id) in POLLS_IDS_REPEAT:
        for poll_id_rep in POLLS_IDS_REPEAT[str(poll_id)]:
            chat1, poll1, poll_results_1 = await get_individual_poll(client, chat_id, poll_id_rep)
            poll_results.votes.extend(poll_results_1.votes)
            poll_results.users.extend(poll_results_1.users)
    return chat, poll, poll_results
async def get_individual_poll(client, chat_id, poll_id):
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
    client = Client("session/schizo", API_ID, API_HASH)
    await client.start()

    aioschedule.every().hour.at(":0").do(startpoll, client)
    aioschedule.every().hour.at(":30").do(startpoll, client)
    await startpoll(client)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(10)


async def startpoll(client):
    chat, poll, poll_results = await get_full_poll(client, POLL_CHAT_ID, POLLS_IDS[0])
    #chat2, poll2, poll_results2 = {}, {}, {}

    poll, votes, users = prepare_one_poll(poll, poll_results)

    for poll_id in POLLS_IDS[1:]:
        chat, poll2, poll_results2 = await get_full_poll(client, POLL_CHAT_ID, poll_id)
        poll, votes, users = combine_polls(poll, poll2, poll_results,
                                        poll_results2)

    votes_cleared, votes_dirty = await calc_poll_results(client,
                                                         chat,
                                                         poll.poll.options,
                                                         votes, users)

    message = get_calc_log(poll.poll.options, votes_cleared, votes_dirty)
    await save_log(client, message, poll.poll.options, votes_cleared)


asyncio.run(main())
