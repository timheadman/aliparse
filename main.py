import logging
import random
import sys
import time
from secrets import *
import help

import mysql.connector as mariadb
from mysql.connector import Error
from prettytable import PrettyTable
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


def make_url(sku_id, shop_id):
    return 'https://aliexpress.ru/item/' + str(shop_id) + '.html?sku_id=' + str(sku_id)


def get_price(url):
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
                price = int(price_cut)
                log_ += str(price)
                break
            else:
                log_ += "none"
    else:
        log_ = "Elements not found ({url})"

    logging.info(f'{time.strftime("%d-%m-%Y %H:%M:%S")}: {log_}, url: {url}')
    return price


def print_report_table(db_sku, db_price, price_minmax):
    report_table = PrettyTable()
    # Сортированное множество уникальных дат
    date_set = sorted(set(db_row[0] for db_row in db_price))
    # Словарь соответствия sku_id -> name
    name_id_dict = {db_row[0]: db_row[1] for db_row in db_sku}

    for sku_id in name_id_dict.keys():
        price = [name_id_dict[sku_id]]
        for date_ in date_set:
            price.append(
                "".join([str(db_row[1]) for db_row in db_price
                         if db_row[0] == date_ and db_row[2] == sku_id]))
        price.append(f'{price_minmax[sku_id][0]}/{price_minmax[sku_id][1]}')
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
            f"Error connecting to MariaDB server: {e}."
        )
        sys.exit("Error connecting to MariaDB server.")

    cursor = connection.cursor(buffered=True)

    sql_query = (
        "SELECT sku_id, shop_id, pk FROM sku WHERE in_use AND pk NOT IN "
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

    sql_query = "SELECT pk, name FROM sku WHERE in_use ORDER BY name"
    cursor.execute(sql_query)
    sku_list = cursor.fetchall()

    sql_query = (
        "SELECT date, price, sku_pk FROM price "
        "WHERE date > NOW() - INTERVAL 5 DAY ORDER BY date"
    )
    cursor.execute(sql_query)
    price_data = cursor.fetchall()

    sql_query = "SELECT sku_pk, MIN(price),MAX(price) FROM price GROUP BY sku_pk"
    cursor.execute(sql_query)
    minmax_dict = {db_row[0]: (db_row[1], db_row[2]) for db_row in cursor.fetchall()}
    print_report_table(sku_list, price_data, minmax_dict)
    wait_command()
    # import matplotlib.pyplot as plt

    cursor.close()
    connection.close()
