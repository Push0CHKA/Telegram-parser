from celery import Celery
from telethon.sync import TelegramClient
from telethon import types
from telethon.tl.functions.messages import GetHistoryRequest
import time



api_id = 4771835
api_hash = "ae11aa708a224d61e1881cad4c830c20"
phone = "+79512603017"
MAX_TRY_MSG_COUNT = 10


celery = Celery(
    __name__,
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0"
)


@celery.task
def parsComments(channels_list: list):
    client = TelegramClient(phone, api_id, api_hash)
    client.start()
    ans_list = []
    for channel in channels_list:
        channel_name = channel[0]  # название сообщества
        source_id    = channel[1]  # уникальный id ресурса
        post_id_flag = channel[2]  # комментарии с определенного поста
        post_id      = channel[3]  # id поста
        channel_link = channel[4]  # ссылка
        comments_cnt = channel[5]  # колличество комментариев для сбора
        reverse_flag = channel[6]  # 
        channel_data = client.get_entity(channel_link)
        # определяем группа это или канал
        if channel_data.megagroup is True or channel_data.gigagroup is True:
            source_type = 'group'
        else:
            source_type = 'channel'
        channel_dict = {'url': channel_link,
                        'source_id': source_id,
                        'source_type': source_type}
        user_comm_list = []
        comm_cnt = 0
        if post_id_flag:  # обработка ссылки на определенный пост
            for message in client.iter_messages(channel_name, reply_to=post_id, reverse=reverse_flag):
                if comm_cnt >= comments_cnt:
                    break
                user_comm = {}
                if isinstance(message.sender, types.User):  # комментарий оставляет пользователь
                    user_comm['user'] = {'tg_id': message.sender.id,
                                         'first_name': message.sender.first_name,
                                         'last_name': message.sender.last_name}
                    user_comm['text'] = message.text
                    user_comm['date'] = message.date
                else:  # комментарий оставляет бот
                    user_comm['user'] = {'tg_id': message.sender.id,
                                         'sender_title': message.sender.title}
                    user_comm['text'] = message.text
                    user_comm['date'] = message.date
                user_comm_list.append(user_comm)
                comm_cnt += 1
                time.sleep(3)  # иначе банят на 24 часа
        else: # обработка ссылки на сообщество (не на определенный пост)
            offset_id = 0
            limit = 100
            total_messages = 0
            total_count_limit = 0
            try_msg_count = 0  # 
            while comm_cnt < comments_cnt:
                try_msg_count += 1
                if try_msg_count > MAX_TRY_MSG_COUNT:  # если в сообщениях нет комментов
                    break
                history = client(GetHistoryRequest(
                                                    peer=channel_link,
                                                    offset_id=offset_id,
                                                    offset_date=None,
                                                    add_offset=0,
                                                    limit=limit,
                                                    max_id=0,
                                                    min_id=0,
                                                    hash=0
                                                    ) 
                                )
                if not history.messages:
                    break
                messages = history.messages
                end_flag = False
                for message in messages:
                    if end_flag:
                        break
                    try:
                        for msg in client.iter_messages(channel_name, reply_to=message.id, reverse=reverse_flag):
                            if comm_cnt >= comments_cnt:
                                end_flag = True
                                break
                            user_comm = {}
                            if isinstance(msg.sender, types.User):  # комментарий оставляет пользователь
                                user_comm['user'] = {'tg_id': msg.sender.id,
                                                     'first_name': msg.sender.first_name,
                                                     'last_name': msg.sender.last_name}
                                user_comm['text'] = msg.text
                                user_comm['date'] = msg.date
                            else:  # комментарий оставляет бот
                                user_comm['user'] = {'tg_id': msg.sender.id,
                                                     'sender_title': msg.sender.title}
                                user_comm['text'] = msg.text
                                user_comm['date'] = msg.date
                            user_comm_list.append(user_comm)
                            comm_cnt += 1
                            time.sleep(3)
                    except:
                        time.sleep(3)
                offset_id = messages[len(messages) - 1].id
                if total_count_limit != 0 and total_messages >= total_count_limit:
                    break 
                time.sleep(3)         
        channel_dict['comments'] = user_comm_list
        ans_list.append(channel_dict)
    return ans_list