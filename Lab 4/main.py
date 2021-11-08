import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fnc1 = '''SELECT 
                film.title title,
                language.name languge,
                category.name category
            FROM 
                film
            INNER JOIN 
                language USING(language_id)
            INNER JOIN 
                film_category USING(film_id)
            INNER JOIN 
                category USING(category_id)
            WHERE
                category.category_id = {id}
            ORDER BY 
                film.title, language.name ASC;'''.format(id = category_id)
    
    SQL = pd.read_sql(fnc1, con=connection)
    
    if not isinstance(category_id, int):
        return None
    
    return SQL

    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn2 = '''SELECT 
                category.name category,
                COUNT(film_category.film_id) amount_of_movies
            FROM 
                category
            INNER JOIN 
                film_category USING(category_id)
            GROUP BY
                category.category_id 
            HAVING 
                category.category_id IN(
                    {id});'''.format(id = category_id)
    
    SQL = pd.read_sql(fcn2, con=connection)
    
    if not isinstance(category_id, int):
        return None
    
    return SQL

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn3 = '''SELECT 
                film.length length,
                COUNT(film.film_id) amount_of_movies
            FROM 
                film
            WHERE 
                length BETWEEN {min_length} and {max_length}
            GROUP BY
                length;'''.format(min_length = min_length, max_length = max_length)
    
    SQL = pd.read_sql(fcn3, con=connection)
    
    if not isinstance(min_length, (int, float)):
        return None
    if not isinstance(max_length, (int, float)):
        return None
    if min_length > max_length:
        return None
    
    return SQL

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn4 = '''SELECT 
                city.city,
                customer.first_name,
                customer.last_name
            FROM 
                city
            INNER JOIN 
                address USING(city_id)
            INNER JOIN 
                customer USING(address_id)
            WHERE
                city.city IN('{city}')
            GROUP BY 
                customer.last_name, customer.first_name, city.city'''.format(city = city)
    
    SQL = pd.read_sql(fcn4, con=connection)
    
    if not isinstance(city, str):
        return None
    
    return SQL


def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn5 = '''SELECT 
                film.length,
                AVG(payment.amount)
            FROM 
                film
            INNER JOIN 
                inventory USING(film_id)
            INNER JOIN 
                rental USING(inventory_id)
            INNER JOIN 
                payment USING(rental_id)
            GROUP BY 
                film.length
            HAVING 
                film.length = {length};'''.format(length = length)
    
    SQL = pd.read_sql(fcn5, con=connection)
    
    if not isinstance(length, (int, float)):
        return None
    
    return SQL

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn6 = '''SELECT 
                customer.first_name, 
                customer.last_name, 
                SUM(film.length)
            FROM 
                film 
            JOIN 
                inventory USING(film_id)
            JOIN 
                rental USING(inventory_id)
            JOIN 
                customer USING(customer_id)
            GROUP BY 
                customer.first_name, customer.last_name
            HAVING 
                SUM(film.length) > {sum_min}
            ORDER BY 
                SUM(film.length), customer.last_name, customer.first_name;'''.format(sum_min = sum_min)
    
    SQL = pd.read_sql(fcn6, con=connection)
    
    if not isinstance(sum_min, (int, float)):
        return None
    
    return SQL

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    fcn7 = '''SELECT 
                category.name category,
                AVG(film.length),
                SUM(film.length),
                MIN(film.length),
                MAX(film.length)
            FROM 
                category
            INNER JOIN 
                film_category USING(category_id)
            INNER JOIN 
                film USING(film_id)
            GROUP BY 
                category.name
            HAVING 
                category.name = '{name}';'''.format(name = name)
    
    SQL = pd.read_sql(fcn7, con=connection)
    
    if not isinstance(name, (str)):
        return None
    
    return SQL