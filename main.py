import telebot
from telebot import types
import requests
import sqlite3
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    """
    Обрабатывает команду /start, создает базу данный, выводит приветствие и обращается к функции главного меню
    """
    db = sqlite3.connect('moex_bot.sql')
    cur = db.cursor()
    sql = """
    CREATE TABLE IF NOT EXISTS portfolio (id int, code text, stock text, number int)
    """
    cur.execute(sql)
    db.commit()
    cur.close()
    db.close()

    mess = (
        f'Привет, {message.from_user.first_name}! Я бот - помощник инвестора, умею выводить состав и текущую стоимость '
        f'Вашего потрефеля акций на Московской Бирже. В первую очередь буду полезен, если у Вас портфель '
        f'разбросан по разным брокерам, помогу отображать его общий состав и стоимость')
    bot.send_message(message.chat.id, mess)
    main_menu(message)


# @bot.message_handler(commands=['menu'])
def main_menu(message):
    """
    Создает и выывдит главное меню
    """
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    show_portfolio = types.KeyboardButton('Состав и стоимость портфеля')
    add_to_portfolio = types.KeyboardButton('Купил акции, добваить в портфель')
    delete_from_portfolio = types.KeyboardButton('Продал акции, удалить из портфеля')
    markup.add(show_portfolio, add_to_portfolio, delete_from_portfolio)
    bot.send_message(message.chat.id, 'Выберите действие', reply_markup=markup)
    bot.register_next_step_handler(message, menu_text)

@bot.message_handler(content_types=["text"])
def menu_text(message):
    """
    Обрабатывает кнопки (сообщения) главного меню
    """
    global quotes_csv_text
    if message.text == 'Состав и стоимость портфеля':
        url = "https://iss.moex.com/iss/engines/stock/markets/" \
            "shares/boards/TQBR/securities.csv?iss.meta=off&iss." \
            "only=marketdata&marketdata.columns=SECID,LAST"
        quotes_csv_text = requests.get(url).text.split('\n')
        if len(quotes_csv_text[2].split(';')[1]) == 0:

            bot.send_message(message.chat.id, f'{message.from_user.first_name}, извините, сейчас нет доступа '
                                                    f'к серверу биржи, поробуйте, пожалуйста, позже')
        else:
            id = message.chat.id
            text = 'Код       Кол-во      Котировка     Стоимость\n'
            total = 0

            db = sqlite3.connect('moex_bot.sql')
            cur = db.cursor()

            sql = """
            SELECT * FROM portfolio WHERE id = ? ORDER BY code
            """
            cur.execute(sql, (id,))
            for i in cur.fetchall():
                total += last_price_stock(i[1]) * i[3]
                text += i[1].ljust(13 - len(str(i[1]))) + \
                    str(i[3]).ljust(17 - len(str(i[3]))) + \
                    str(last_price_stock(i[1])).ljust(26 - len(str(last_price_stock(i[1])))) + \
                    str(round(last_price_stock(i[1]) * i[3], 2)).ljust(40 - len(str(round(last_price_stock(i[1]) * i[3], 2)))) + '\n'
            text += '\n' + f'Общая стоимость портфеля: {total} руб'
            cur.close()
            db.close()
            bot.send_message(message.chat.id, text, parse_mode='HTML')
        main_menu(message)

    elif message.text == 'Купил акции, добваить в портфель':
        bot.send_message(message.chat.id, 'Введите через пробел код акции и купленное количество, например: SBER 200')
        bot.register_next_step_handler(message, stock_buy_add)
    elif message.text == 'Продал акции, удалить из портфеля':
        bot.send_message(message.chat.id, 'Введите через пробел код акции и проданное количество, например: GAZP 150')
        bot.register_next_step_handler(message, stock_sell_delete)


def last_price_stock(code):
    """
    Принимает на вход код акции, находит в цикле соответсвующиую котировку и возвращает ее
    """
    for quotes_csv_line in quotes_csv_text:
        quotes_csv_tabl = quotes_csv_line.split(';')
        if quotes_csv_tabl[0] == code:
            return float(quotes_csv_tabl[1])


def stock_buy_add(message):
    """
    Принимает на вход сообщение, отправленное полсе "Купил акции", проверяет сообщение на ошибки воода
    и добавляет акции в портфель (БД)
    """
    stock_code = pd.read_excel("stock_code.xlsx")

    id = message.chat.id
    code = message.text.strip().split(' ')[0].upper()

    if len(message.text.strip().split(' ')) != 2:
        bot.send_message(message.chat.id, f'{message.from_user.first_name}, нужно ввести два значения, '
                                                f'код и количество, пожалуйста, попробуйте еще раз')
    elif stock_code[stock_code['code'] == code].shape[0] == 0:
        bot.send_message(message.chat.id, f'{message.from_user.first_name}, такого кода акции не существует, '
                                                f'пожалуйста, попробуйте еще раз')
    else:
        try:
            number = int(message.text.strip().split(' ')[1])
            stock = stock_code[stock_code['code'] == code]['stock'].to_string(index=False)
            db = sqlite3.connect('moex_bot.sql')
            cur = db.cursor()
            sql = """
            SELECT code FROM portfolio WHERE id = ? AND code = ?
            """
            cur.execute(sql, (id, code))

            if len(cur.fetchall()) == 0:
                sql = """
                INSERT INTO portfolio VALUES (?, ?, ?, ?)
                """
                cur.execute(sql, (id, code, stock, number))
                db.commit()
            else:
                sql = """
                SELECT number FROM portfolio WHERE id = ? AND code = ?
                """
                cur.execute(sql, (id, code))
                number_new = number + cur.fetchall()[0][0]
                sql = """
                UPDATE portfolio SET number = ? WHERE id = ? AND code = ?
                """
                cur.execute(sql, (number_new, id, code))
                db.commit()

            db.commit()
            cur.close()
            db.close()

            bot.send_message(message.chat.id, f'{number} акций {stock} добавлены в портфель!')
        except:
            bot.send_message(message.chat.id, f'{message.from_user.first_name}, вторым значением должно '
                                                    f'быть число акций, пожалуйста, попробуйте еще раз')
    main_menu(message)


# @bot.message_handler(content_types=['text'])
def stock_sell_delete(message):
    """
    Принимает на вход сообщение, отправленное полсе "Продал акции", проверяет сообщение на ошибки воода
    и удаляет акции из портфеля (БД)
    """
    stock_code = pd.read_excel("stock_code.xlsx")
    code = message.text.strip().split(' ')[0].upper()
    if len(message.text.strip().split(' ')) != 2:
        bot.send_message(message.chat.id, f'{message.from_user.first_name}, нужно ввести два значения, код и '
                                                f'количество, пожалуйста, попробуйте еще раз')
    elif stock_code[stock_code['code'] == code].shape[0] == 0:
        bot.send_message(message.chat.id, f'{message.from_user.first_name}, такого кода акции не существует, '
                                                f' пожалуйста, попробуйте еще раз')
    else:
        try:
            number = int(message.text.strip().split(' ')[1])
            stock = stock_code[stock_code['code'] == code]['stock'].to_string(index=False)
            id = message.chat.id

            db = sqlite3.connect('moex_bot.sql')
            cur = db.cursor()
            sql = """
            SELECT code FROM portfolio WHERE id = ? AND code = ?
            """
            cur.execute(sql, (id, code))

            if len(cur.fetchall()) == 0:
                bot.send_message(message.chat.id, f'{message.from_user.first_name}, таких акций нет в Вашем '
                                                  f'портфеле, пожалуйста, попробуйте еще раз')
            else:
                sql = """
                SELECT number FROM portfolio WHERE id = ? AND code = ?
                """
                cur.execute(sql, (id, code))
                number_old = cur.fetchall()[0][0]
                if number_old < number:
                    bot.send_message(message.chat.id, f'{message.from_user.first_name}, количество проданных акций '
                                                      f'больше, чем есть в Вашем портфеле '
                                                      f', пожалуйста, попробуйте еще раз')
                elif number_old == number:
                    sql = """
                    DELETE FROM portfolio WHERE id = ? AND code = ?
                    """
                    cur.execute(sql, (id, code))
                    bot.send_message(message.chat.id, f'{number} акций {stock} удалены из портфеля!')
                else:
                    number_new = number_old - number
                    sql = """
                    UPDATE portfolio SET number = ? WHERE id = ? AND code = ?
                    """
                    cur.execute(sql, (number_new, id, code))
                    bot.send_message(message.chat.id, f'{number} акций {stock} удалены из портфеля!')

            db.commit()
            cur.close()
            db.close()
        except:
            bot.send_message(message.chat.id, f'{message.from_user.first_name}, вторым значением должно быть число '
                                                    f'акций, пожалуйста, попробуйте еще раз')
    main_menu(message)


bot.polling(none_stop=True)
