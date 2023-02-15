# 230215
import telebot


def f_telegram_send_message(tlg_bot_token='', tlg_chat_id=None, txt_to_send='', txt_mode=None, txt_type='', txt_name=''):
    '''
    Функция отправляет в указанный чат телеграма текст
    Входные параметры: токен, чат, текст, тип форматирования текста (HTML, MARKDOWN)
    '''
    if txt_type == 'ERROR':
        txt_type = '❌'
        # txt_type = '\u000274C'
    elif txt_type == 'WARNING':
        txt_type = '⚠'
        # txt_type = '\U0002757'
    elif txt_type == 'INFO':
        txt_type = 'ℹ'
        # txt_type = '\U0002755'
    elif txt_type == 'SUCCESS':
        txt_type = '✅'
        # txt_type = '\U000270'
    else:
        txt_type = ''
    txt_to_send = f"{txt_type} {txt_name} | {txt_to_send}"
    try:
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode=None)
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode='MARKDOWN')
        dv_tlg_bot = telebot.TeleBot(tlg_bot_token, parse_mode=txt_mode)
        # отправляем текст
        tmp_out = dv_tlg_bot.send_message(tlg_chat_id, txt_to_send[0:3999])
        return f"chat_id = {tlg_chat_id} | message_id = {tmp_out.id} | html_text = '{tmp_out.html_text}'"
    except Exception as error:
        return f"ERROR: {error}"
