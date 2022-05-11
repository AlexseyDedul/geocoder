import random
import sys
import threading
from threading import Thread

from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import re
import psycopg2
import openpyxl

sys.setrecursionlimit(10**7) # max depth of recursion
threading.stack_size(2**20)

engine_from = create_engine("postgresql+psycopg2://dedul:dedul@localhost:15432/gis")


def __sort_results(results):
    swapped = True
    while swapped:
        swapped = False
        for i in range(len(results) - 1):
            l = int(re.search(r'\d+', results[i]['housenumb']).group()) if results[i]['housenumb'] is not None and \
                                                                           re.search(r'\d+', results[i][
                                                                               'housenumb']) is not None else 0
            s = int(re.search(r'\d+', results[i + 1]['housenumb']).group()) if results[i + 1][
                                                                                   'housenumb'] is not None and \
                                                                               re.search(r'\d+', results[i + 1][
                                                                                   'housenumb']) is not None else 0
            if l < s:
                results[i], results[i + 1] = results[i + 1], results[i]
                swapped = True
    return results


def __search_address(error_address_before, error_address_after, result, housenumber):
    if not housenumber:
        return result
    list_addr = []
    for index, dict_ in enumerate(result):
        try:
            s = int(re.search(r'\d+', dict_['housenumb']).group()) - int(re.search(r'\d+', housenumber).group())
            if error_address_before <= s <= error_address_after:
                list_addr.append(dict_)
        except:
            pass
    return list_addr


def correctly_address(results, housenumber):
    full_compare_numb_address = None
    compare_numb_address = None
    for index, dict_ in enumerate(results):
        numb_out = int(re.search(r'\d+', dict_['housenumb']).group()) if dict_['housenumb'] is not None \
                                                                         and re.search(r'\d+', dict_[
            'housenumb']) is not None else 0
        numb_input = int(re.search(r'\d+', housenumber).group()) if housenumber is not None \
                                                                    and re.search(r'\d+',
                                                                                  housenumber) is not None else 0

        if str(dict_['housenumb']) == str(housenumber.strip()):
            full_compare_numb_address = dict_
        elif numb_out == numb_input:
            if full_compare_numb_address is None:
                compare_numb_address = dict_

    if full_compare_numb_address is not None:
        return full_compare_numb_address
    elif compare_numb_address is not None:
        return compare_numb_address
    else:
        return None


def not_correctly_address(n, m, results, housenumber, street):
    if not results:
        return {}

    if housenumber is None or street is None:
        return results[0]

    if len(results) <= 5:
        return results[0]

    if len(results) > 5:
        result = __search_address(n, m, results, housenumber)
        return not_correctly_address(n + 4, m - 3, result, housenumber, street)


def get_correct_housenumber(number):
    housenumber = ''
    if number != '' and number is not None:
        number = number.strip()
        if number.find('-') > 0:
            for n in number.split('-'):
                if n.isnumeric():
                    housenumber = n
        else:
            housenumber = number
        return housenumber.strip()
    else:
        return None


def __check_symbols_drop_in_word(addr):
    if addr.find(".") > 0:
        s = re.findall(r'\.\w+$', addr)
        return s[0].replace(".", '') if s else addr
    return addr


def __check_symbols_dash_in_word(addr):
    if addr.find('-') == 1:
        return re.findall(r'^\w+[\-]', addr)[0].replace("-", '')
    else:
        return __check_symbols_drop_in_word(addr)


def get_correct_address(address):
    if address == "Неизвестная":
        address = None
    if isinstance(address, str) and address != "":
        address = address.strip().replace(",", " ")
        correct_addr_list = []
        if address.find(' ') > 0:
            for addr in address.split(' '):
                s = __check_symbols_drop_in_word(__check_symbols_dash_in_word(addr))
                correct_addr_list.append(s)
            return correct_addr_list
        else:
            s = __check_symbols_drop_in_word(__check_symbols_dash_in_word(address))
            return s
    else:
        return None


def __create_sql_query_for_city(city):
    city_query = ""
    if city:
        if isinstance(city, str):
            city_query = "WHERE translate(name, 'ё', 'е') like translate('%" + city \
                         + "%', 'ё', 'е') "
        elif isinstance(city, list):
            city_query = "WHERE translate(name, 'ё', 'е') like translate('%" + city[0] \
                         + "%', 'ё', 'е') "
            for i in range(1, len(city)):
                city_query += "and translate(name, 'ё', 'е') like translate('%" + city[i] \
                              + "%', 'ё', 'е')  "
        return city_query
    else:
        return None


def __create_sql_query_for_street(street):
    street_query = ""
    if street:
        if isinstance(street, str):
            street_query = f"WHERE translate(lower(tags->'addr:street'), 'ёо', 'еа') " \
                           f"like translate(lower('%{street}%'), 'ёо', 'еа') "
        elif isinstance(street, list):
            if street:
                street_query = f"WHERE translate(lower(tags->'addr:street'), 'ёо', 'еа') " \
                               f"like translate(lower('%{street[0]}%'), 'ёо', 'еа') "
                for i in range(1, len(street)):
                    street_query += f"and translate(lower(tags->'addr:street'), 'ёо', 'еа') " \
                                    f"like translate(lower('%{street[i]}%'), 'ёо', 'еа') "
        return street_query
    else:
        return ""


def __get_addresses_list_from_db(city, street):
    results = []
    city_query = __create_sql_query_for_city(city)
    street_query = __create_sql_query_for_street(street)
    if city_query:
        sql = text("select lat, long, housenumber, housename, regionname from ( \
                            select way_o, way_ul, lat, long, housenumber, housename, tags, regionname, ST_Contains(way_o,way_ul) ch  from ( \
                            SELECT way way_o, name as regionname\
                            FROM planet_osm_polygon \
                            " + city_query + " \
                            ) t1, \
                            (SELECT tags, \"addr:housenumber\" housenumber, way way_ul, \
                            replace(ST_X(ST_TRANSFORM(ST_Centroid(way),4674))::varchar(255),',','.') AS long, \
                            replace(ST_Y(ST_TRANSFORM(ST_Centroid(way), 4674))::varchar(255), ',', '.') AS lat, tags->'addr:street' as housename \
                            FROM planet_osm_polygon \
                            " + street_query + " \
                            ) t2 \
                            ) t5 \
                            where ch is True")
        result = engine_from.execute(sql)
        for row in result:
            results.append(dict(zip(['lat', 'lon', 'housenumb', 'housename', 'regionname'], row)))

        return results
    else:
        return []


def get_address_from_db(city, street, housenumber):
    addr = {}
    street_correct = get_correct_address(street)
    city_correct = get_correct_address(city)
    housenumber = get_correct_housenumber(housenumber)
    results = __get_addresses_list_from_db(city_correct, street_correct)

    if street_correct is not None:
        results = __sort_results(results)

    correctly = True

    if housenumber is not None and street_correct is not None:
        addr = correctly_address(results, housenumber)

    if not addr:
        addr = not_correctly_address(-30, 30, results, housenumber, street_correct)
        correctly = False

    if addr:
        return {
            'current_address': {
                'current_city': city,
                'current_street': street,
                'current_housenumber': housenumber
            },
            'found_address': {
                'lat': addr['lat'],
                'lan': addr['lon'],
                'found_city': addr['regionname'],
                'found_street': addr['housename'],
                'found_housenumber': addr['housenumb']
            },
            'correctly': correctly
        }
    else:
        return {
            'current_address': {
                'current_city': city,
                'current_street': street,
                'current_housenumber': housenumber},
            'found_address': {
                'address': 'not found'
            }
        }


def work_with_data(city, street, housenumber):
    # print(f"{city} {street} {housenumber}")
    result = get_address_from_db(city, street, housenumber)
    return result


def work_with_files(path):
    count = 0
    countNFList = []
    countFalse = 0
    countTrue = 0
    column = path['number'] + 9
    workbook = openpyxl.load_workbook('input/' + path['url'])
    worksheet = workbook.active

    # for i in range(1, 9):
    for i in range(1, worksheet.max_row):
        city = worksheet.cell(i, path['city']).value
        street = worksheet.cell(i, path['street']).value
        housenumber = worksheet.cell(i, path['number']).value
        result = work_with_data(city, street, housenumber)
        print(result)
        try:
            worksheet.cell(row=i, column=column).value = f"({result['found_address']['lat']}, {result['found_address']['lan']})"
            worksheet.cell(row=i, column=column + 1).value = f"({result['correctly']})"
        except KeyError:
            pass

        try:
            if not result['correctly']:
                countFalse += 1
            else:
                countTrue += 1
        except KeyError:
            pass

        try:
            if result['found_address']['address']:
                countNFList.append(result)
                count += 1
        except KeyError:
            pass
    workbook.save(filename="output/" + path['url'])

    print(f"Not Found: {count}")
    print(f"False: {countFalse}")
    print(f"True: {countTrue}")

    f = open('out2.txt', 'w')
    for l in countNFList:
        f.write(f"print(get_address_from_db('{l['current_address']['current_city']}', "
                f"'{l['current_address']['current_street']}', "
                f"'{l['current_address']['current_housenumber']}'))\n")


def main():
    filenames = [
        {
            'url': "Белкоопсоюз-Торговый-реестр.xlsx",
            'city': 7,
            'street': 8,
            'number': 9
        },
        {
            'url': "Белкоопсоюз-Торговый-реестр-Общепит.xlsx",
            'city': 7,
            'street': 8,
            'number': 9
        },
        {
            'url': "Белпочта-Торговый-реестр.xlsx",
            'city': 7,
            'street': 8,
            'number': 9
        },
        {
            'url': "Белсоюзпечать-Торговый-реестр.xlsx",
            'city': 7,
            'street': 8,
            'number': 9
        },
        {
            'url': "Santa_read.xlsx",
            'city': 5,
            'street': 6,
            'number': 7
        },
        {
            'url': "ЭНЕРГО-ОИЛ-Торговый-реестр-новые.xlsx",
            'city': 5,
            'street': 6,
            'number': 7
        },
        {
            'url': "Сведения_Торгового_реестра_Республики_Беларусь_на_25042022_текущий_2022_загрузка.xlsx",
            'city': 22,
            'street': 23,
            'number': 24
        }
    ]

    for i in filenames:
        th = Thread(target=work_with_files, args=(i,))
        th.start()
        # work_with_files(i)


if __name__ == '__main__':
    main()
