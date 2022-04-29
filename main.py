from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import re
import psycopg2
import openpyxl


engine_from = create_engine("postgresql+psycopg2://dedul:dedul@localhost:15432/gis")


def __sort_results(results):
    for x in range(0, len(results)):
        for y in range(0, len(results) - 1):
            l = int(re.search(r'\d+', results[x]['housenumb']).group()) if results[x]['housenumb'] is not None and \
                                                                           re.search(r'\d+', results[x]['housenumb']) is not None else 0
            s = int(re.search(r'\d+', results[y]['housenumb']).group()) if results[y]['housenumb'] is not None and \
                                                                           re.search(r'\d+', results[y]['housenumb']) is not None  else 0
            if l > s:
                results[x], results[y] = results[y], results[x]
    return results


def __search_address(error_address_before, error_address_after, result, housenumber):
    list_addr = []
    for index, dict_ in enumerate(result):
        try:
            s = int(re.search(r'\d+', dict_['housenumb']).group()) - int(re.search(r'\d+', housenumber).group())
            if error_address_before < s < error_address_after:
                list_addr.append(dict_)
        except:
            pass
    return list_addr


def correctly_address(results, housenumber):
    full_compare_numb_address = None
    compare_numb_address = None
    for index, dict_ in enumerate(results):
        numb_out = int(re.search(r'\d+', dict_['housenumb']).group()) if dict_['housenumb'] is not None\
                                                                         and re.search(r'\d+', dict_['housenumb']) is not None else 0
        numb_input = int(re.search(r'\d+', housenumber).group()) if housenumber is not None else 0

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


def not_correctly_address(results, housenumber):
    results_sort = __sort_results(results)
    last_addr_dict = {}
    list_addr = __search_address(-10, 10, results_sort, housenumber)

    if list_addr:
        if len(list_addr) < 4:
            last_addr_dict = list_addr[0]
        else:
            list_addr = __search_address(-5, 5, list_addr, housenumber)
            if len(list_addr) < 4:
                last_addr_dict = list_addr[0]
            else:
                list_addr = __search_address(-3, 3, list_addr, housenumber)
                last_addr_dict = list_addr[0]

    return last_addr_dict


def get_correct_housenumber(number):
    if number.find('-') > 0:
        housenumber = number.split('-')[0]
    else:
        housenumber = number
    return housenumber


def get_addresses_list_from_db(city, street):
    results = []
    sql = text("select lat, long, housenumber, housename, regionname from ( \
                    select way_o, way_ul, lat, long, housenumber, housename, tags, regionname, ST_Contains(way_o,way_ul) ch  from ( \
                    SELECT way way_o, name as regionname\
                    FROM planet_osm_polygon \
                    WHERE translate(name, 'ё', 'е') = translate('" + city.strip().split(" ")[0] + "', 'ё', 'е') and place in ('city', 'town') \
                    ) t1, \
                    (SELECT tags, \"addr:housenumber\" housenumber, way way_ul, \
                    replace(ST_X(ST_TRANSFORM(ST_Centroid(way),4674))::varchar(255),',','.') AS long, \
                    replace(ST_Y(ST_TRANSFORM(ST_Centroid(way), 4674))::varchar(255), ',', '.') AS lat, tags->'addr:street' as housename \
                    FROM planet_osm_polygon \
                    WHERE translate(lower(tags->'addr:street'), 'ё', 'е') like translate(lower('%" +
               street.strip().split(" ")[0] + "%'), 'ё', 'е') \
                    ) t2 \
                    ) t5 \
                    where ch is True")

    result = engine_from.execute(sql)
    for row in result:
        results.append(dict(zip(['lat', 'lon', 'housenumb', 'housename', 'regionname'], row)))

    return results


def get_address_from_db(city, street, housenumber):
    results = get_addresses_list_from_db(city, street)
    housenumber = get_correct_housenumber(housenumber)

    correctly = True
    addr = correctly_address(results, housenumber)

    if not addr:
        addr = not_correctly_address(results, housenumber)
        correctly = False

    if addr:
        return {
                    'current_address': {
                        'current_city': city,
                        'current_street': street,
                        'current_housenumber': housenumber},
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


def main():
    df = pd.read_excel(r"./Santa_read.xlsx")

    for z in df.values.tolist():
        print(get_address_from_db(z[4], z[5], z[6]))


if __name__ == '__main__':
    main()
