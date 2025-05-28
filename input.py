import pandas as pd
import requests
import xml.etree.ElementTree as et
import numpy as np

#Формирование списка дат, валют и суммы из файла payments, фильтрация списка по рублям

pay = pd.read_csv ('payments.csv', usecols=['Дата оплаты', 'Валюта','Сумма'], parse_dates = ['Дата оплаты'])
rub = pay[pay['Валюта'] != 'RUB']
dates = rub['Дата оплаты'].unique()
tdates = dates[:1]

#Формирование парсера валют по заданным датам и валютам

rates = []
for d in dates:
    dt = pd.to_datetime(d).strftime('%d.%m.%Y')
    print (dt)
    url = 'http://cbr.ru/scripts/XML_daily.asp?date_req=' + dt
    r = requests.get(url)
    xml_usd = et.fromstring(r.text)[10]
    usd = float(str.replace(xml_usd[4].text, ',' , '.'))
    xml_eur = et.fromstring(r.text)[11]
    eur = float(str.replace(xml_eur[4].text, ',' , '.'))
    dt_rate = [pd.to_datetime(d), usd, eur]
    rates.append(dt_rate)

#Фильтрация курса валют к рублю по заданным датам и выведеня его в csv-файл

rates_df = pd.DataFrame(data = rates, columns = ['Дата' , 'USD', 'EUR'])
rates_df ['RUB'] = 1.0
rates_df.to_csv('my_rates.csv', index = False)

#Формирование списка ФИО и даты рождения из файла payments

pay = pd.read_csv ('payments.csv', usecols=['ФИО', 'Дата рождения'], parse_dates = ['Дата рождения'])

#Добавления пола с помощью фильтрации по окончанию

users = pay.drop_duplicates()

users['Последняя буква'] = users['ФИО'].str[-1]

users['Пол'] = np.where(users['Последняя буква'] == 'ч', 'Мужчины', 'Женщины')

#Получние заданых значений в csv-файл

users[['ФИО','Пол']].to_csv('my_gender.csv', index = False)

#Добавление токена и ключа из сервиса Dadata для анализа геоданных

from dadata import Dadata
token = "cb7e884ab38ed471d394b66bbb1960fb4605bee3"
secret = "0d867753d788b6ff4ff3da722693c2402145fd43"

dadata = Dadata(token, secret)

#Создание списка из файла payments с адресом доставки

pay = pd.read_csv ('payments.csv', usecols=['Адрес доставки'])

#Проверка списка на дубликаты и пустые значения

pay.drop_duplicates(inplace = True)

pay.reset_index(inplace = True, drop = True)

points = []
for rowIndex, row in pay.iterrows():
    address = pay.iloc[rowIndex]['Адрес доставки']
    result = dadata.suggest('address', address)
    
    if result:  # Проверяем, что список не пустой
        r = result[0]['data']
        lat = r.get('geo_lat')  # Используем get на случай отсутствия ключа
        lon = r.get('geo_lon')
        city = r.get('city')
        point = [address, city, lat, lon]
    else:
        # Если результат пустой, можно добавить None или другие значения
        point = [address, None, None, None]
    
    points.append(point)

#Создание списка с широтой, долготой и адресом

geo_df = pd.DataFrame(data=points, columns=['Адрес', 'Город', 'Широта', 'Долгота'])

#Создание csv-файла с геоданными

geo_df.to_csv('my_geo.csv', index = False)