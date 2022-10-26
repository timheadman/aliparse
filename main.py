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


def get_price(url):
    rnd_int = random.randint(2, 10)
    logging.info(f"Random sleep: {rnd_int} sec...")
    time.sleep(rnd_int)
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
            By.CLASS_NAME, "Product_UniformBanner__uniformBannerBoxPrice__o5qwb"
        )
    if not elements:
        elements = driver.find_elements(
            By.CLASS_NAME, "snow-price_SnowPrice__mainS__ugww0l"
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


def print_report_table(db_price, db_sku):
    report_table = PrettyTable()
    date_set = sorted(set(db_row[0] for db_row in db_price))
    name_id_dict = {db_row[0]: db_row[1] for db_row in db_sku}

    for name_id in name_id_dict.keys():
        price = []
        for date_ in date_set:
            price.append(
                "".join(
                    [
                        str(db_row[2])
                        for db_row in db_price
                        if db_row[0] == date_ and db_row[3] == name_id
                    ]
                )
            )
        price.insert(0, name_id_dict[name_id])
        report_table.add_row(price)

    date_set.insert(0, "")
    report_table.field_names = date_set
    print(report_table)


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
        "SELECT id, url FROM sku WHERE id NOT IN "
        f"(SELECT sku_id FROM price WHERE date = '{today}')"
    )
    cursor.execute(sql_query)
    logging.info(f"Recieve {cursor.rowcount} rows.")
    data = cursor.fetchall()

    if len(data):
        options = webdriver.ChromeOptions()
        options.binary_location = (
            "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
        )
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )

        for row in data:
            current_price = get_price(row[1])
            if current_price:
                sql_query = (
                    "INSERT INTO price (sku_id, date, price) "
                    f"VALUES ({row[0]}, '{today}', {current_price})"
                )
                cursor.execute(sql_query)

        connection.commit()
        driver.close()

    sql_query = (
        "SELECT date, name, price, sku_id FROM price "
        "INNER JOIN sku ON sku.id = price.sku_id "
        "WHERE date > NOW() - INTERVAL 5 DAY ORDER BY date "
    )
    cursor.execute(sql_query)
    price_data = cursor.fetchall()
    sql_query = "SELECT id, name FROM sku ORDER BY name"
    cursor.execute(sql_query)
    sku_list = cursor.fetchall()
    print_report_table(price_data, sku_list)

    cursor.close()
    connection.close()
