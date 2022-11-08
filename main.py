import logging
import random
import sys
import time
from secrets import *

import mysql.connector as mariadb
from mysql.connector import Error
from prettytable import PrettyTable
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

import help


def make_url(sku_id, shop_id):
    return 'https://aliexpress.ru/item/' + str(shop_id) + '.html?sku_id=' + str(sku_id)


def get_price(url, is_float=False):
    driver.get(url)
    if url != driver.current_url:
        logging.info(
            f'{time.strftime("%d-%m-%Y %H:%M:%S")}: '
            f"The product does not exist or the product is out of stock '{url}'."
        )
        return 0
    price = 0
    elements = driver.find_elements(By.CLASS_NAME, "product-price-current")
    if not elements:
        elements = driver.find_elements(
            By.CSS_SELECTOR, "[class*='Product_UniformBanner__uniformBannerBoxPrice__']"
        )
    if not elements:
        elements = driver.find_elements(
            By.CSS_SELECTOR, "[class*='snow-price_SnowPrice__mainS__']"
        )

    if elements:
        log_ = f"Found {len(elements)} elements: "
        for element in elements:
            price_text = element.text
            price_cut = price_text[: price_text.find(",")].replace(" ", "")
            log_ += f"e.text: {element.text}, cut: {price_cut}, int: "
            if price_cut.isdigit():
                if is_float:
                    price = round(
                        float(
                            price_text[: price_text.find(",") + 3]
                            .replace(" ", "")
                            .replace(",", ".")
                        ),
                        2,
                    )
                else:
                    price = int(price_cut)
                log_ += str(price)
                break
            else:
                log_ += "none"
    else:
        log_ = "Elements not found ({url})"

    logging.info(f'{time.strftime("%d-%m-%Y %H:%M:%S")}: {log_}, url: {url}')
    return price


def print_report_table(db_sku, db_price, db_minmax, db_exchange):
    report_table = PrettyTable()
    # Сортированное множество уникальных дат
    date_set = sorted(set(db_row[0] for db_row in db_price))
    # Словарь соответствия sku_id -> name
    name_id_dict = {db_row[0]: db_row[1] for db_row in db_sku}
    price = ['*** Exchange USD/RUB ***']
    for date_ in date_set:
        price.append(
            "".join([str(db_row[1]) for db_row in db_exchange if db_row[0] == date_])
        )
    price.append('*')
    report_table.add_row(price)

    for sku_id in name_id_dict.keys():
        price = [name_id_dict[sku_id]]
        # ToDo: Выбрать последние 5 существующих дат, а не 5 начиная с текущей
        for date_ in date_set:
            price.append(
                "".join(
                    [
                        str(db_row[1])
                        for db_row in db_price
                        if db_row[0] == date_ and db_row[2] == sku_id
                    ]
                )
            )
        price.append(f'{db_minmax[sku_id][0]}/{db_minmax[sku_id][1]}')
        report_table.add_row(price)

    date_set.insert(0, "")
    date_set.append("MIN/MAX")
    report_table.field_names = date_set
    print(report_table)


def wait_command():
    while True:
        command = input("Enter command, help - for help, enter - for exit: ")
        com_list = command.split()
        print(com_list)
        if not len(com_list):
            exit(0)
        if com_list[0] == 'help':
            print(help.help_topic)


if __name__ == "__main__":
    logging.basicConfig(
        encoding="utf-8",
        level=logging.INFO,
        handlers=[logging.FileHandler("aliparse.log"), logging.StreamHandler()],
    )

    today = time.strftime("%Y-%m-%d")
    connection = None
    cursor = None
    try:
        connection = mariadb.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            database=DATABASE,
        )
    except Error as e:
        logging.error(
            f'{time.strftime("%d-%m-%Y %H:%M:%S")}: '
            f'Error connecting to MariaDB server: {e}.'
        )
        sys.exit("Error connecting to MariaDB server.")

    cursor = connection.cursor(buffered=True)

    sql_query = (
        "SELECT sku_id, shop_id, pk, name FROM sku WHERE in_use AND pk NOT IN "
        f"(SELECT sku_pk FROM price WHERE date = '{today}')"
    )

    cursor.execute(sql_query)
    logging.info(f"Recieve {cursor.rowcount} rows.")
    data = cursor.fetchall()

    if len(data):
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.binary_location = (
            "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
        )
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )
        # Обменный курс на Aliexpress
        sql_query = f"SELECT price FROM exchange WHERE date='{today}'"
        cursor.execute(sql_query)

        if not cursor.rowcount:
            exchange = get_price(
                'https://aliexpress.ru/item/4000939906574.html?sku_id=10000011334711491',
                is_float=True,
            )
            if exchange:
                sql_query = (
                    f"INSERT INTO exchange (date, price) VALUES ('{today}', {exchange})"
                )
                cursor.execute(sql_query)

        row_count = 0
        for row in data:
            if row_count != 0 and row_count != len(data):
                rnd_int = random.randint(2, 10)
                logging.info(f"Random sleep: {rnd_int} sec...")
                time.sleep(rnd_int)
            row_count += 1
            current_price = get_price(make_url(row[0], row[1]))
            if current_price:
                sql_query = (
                    "INSERT INTO price (sku_pk, date, price) "
                    f"VALUES ({row[2]}, '{today}', {current_price})"
                )
                cursor.execute(sql_query)

        connection.commit()
        driver.close()

    # Создаем набор уникального номера товара и имя товара
    sql_query = "SELECT pk, name FROM sku WHERE in_use ORDER BY name"
    cursor.execute(sql_query)
    sku_data = cursor.fetchall()

    # Создаем таблицу цен за последние 5 дней
    sql_query = (
        "SELECT date, price, sku_pk FROM price "
        "WHERE date > NOW() - INTERVAL 5 DAY ORDER BY date"
    )
    cursor.execute(sql_query)
    price_data = cursor.fetchall()

    # Создаем словарь с максимальной и минимальной ценой по каждому SKU
    sql_query = "SELECT sku_pk, MIN(price),MAX(price) FROM price GROUP BY sku_pk"
    cursor.execute(sql_query)
    minmax_data = {db_row[0]: (db_row[1], db_row[2]) for db_row in cursor.fetchall()}

    # Создаем таблицу курса USD/RUB за последние 5 дней
    sql_query = f"SELECT date, price FROM exchange WHERE date > NOW() - INTERVAL 5 DAY ORDER BY date"
    cursor.execute(sql_query)
    exchange_data = cursor.fetchall()

    print_report_table(sku_data, price_data, minmax_data, exchange_data)
    # wait_command()
    cursor.close()
    connection.close()
