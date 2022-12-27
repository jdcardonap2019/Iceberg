from typing import Union
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup
from datetime import datetime, time
from csv import writer
import pandas as pd
from io import StringIO 
import requests
app = FastAPI()
@app.get("/hotels/{city}", response_class = StreamingResponse)
def find_hotels(city):
    citi = city.lower()
    #Variable for enumerate the hotels in the order of the pages
    actual_index = 0
    #Variable for the number of iterations
    counter = 0
    #Variable for change the page 
    aumento = 0
    #Variable for the limit to iterate - in the first iteration it will change to the number of hotels that tripadvisor has 
    limit = 9999999
    #Url of tripadvisor
    url = 'https://www.tripadvisor.com'
    newURl = '/Hotels-g294073-Colombia-Hotels.html'
    #Url of the hotels of tripadvisor
    #Dictionary created to save the name of the hotels and do not repeat any of them
    dict_names_hotels = {}
    dict_url_cities = {}
    #Writer for the csv file
    with open('iceberg_test_api.csv', 'w', encoding = 'utf-8', newline ='') as f: 
        thewriter = writer(f)
        header = ['Name', 'Rank', 'ID', 'URL Tripadvisor','Avg Price', 'Phone number', 
                'Address', 'Rate Tripadvisor', 'Location Rate', 'Cleanliness Rate', 'Service Rate', 'Value Rate',
                'Property Amenities', 'Room Features', 'Room Types']
        thewriter.writerow(header)
        #Cycle for iterate above all the pages for bogota hotels
        cityUrl = ''
        while counter < limit:
            #Set the headers
            headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
            #Make the HTTP Request for cities
            html_cities = requests.get(url+newURl, headers=headers)
            html__cities_text = html_cities.text
            #Instance the first BeautifulSoup object to search in the cities page
            soup = BeautifulSoup(html__cities_text, 'lxml')
            #Finding all the cities divs
            div__city_tags = soup.find_all('div', class_='geo_wrap')
            #Save the URLs of the cities in the dict
            for city in div__city_tags:
                dict_url_cities[city.a.text.replace('Hotels', '').lower().rstrip()] = city.a['href']
            print(dict_url_cities)
            #Make the HTTP Request for the city needed. If aumento != 0 it means that we already visited the 1st city page and 
            # we are in another one. So, we set the URL with the aumento variable.
            if aumento == 0:
                cityUrl = dict_url_cities[citi]
            else:
                url_fixed = cityUrl.split('-')
                url_fixed.insert(2, 'oa'+str(aumento))
                cityUrl = '-'.join(url_fixed)
            html = requests.get(url+cityUrl, headers=headers)
            html_text = html.text
            #Instance the first BeautifulSoup object to search in the listing page
            soup2 = BeautifulSoup(html_text, 'lxml')
            div_tags = soup2.find_all('div', class_='meta_listing ui_columns large_thumbnail_mobile')
            #Find the total of hotels that Bogota has. This will help us with the iteration
            limit = int(soup2.find('span', class_='qrwtg').text.split()[0].replace(',',''))
            for div in div_tags:
                #Array to save the info
                info = []
                #title_text = title.text.split('.')[1].lstrip(' ') if title != None else None
                #Add 1 to the index counter
                actual_index += 1
                #Add 1 to the counter
                counter += 1
                print('Counter', counter)
                #Here, we get the Hotel's URL from TripAdvisor, Hotel ID and Hotel rank
                rank = actual_index
                info.append(rank)
                hotel_id = div['data-listingkey']
                info.append(hotel_id)
                url_actual = div['data-url']
                #We call it details due to the information that we will get from that link
                url_details = url + url_actual
                info.append(url_details)
                #Then, we make the HTTP Request again of that URL to get the HTML code
                details = requests.get(url_details, headers=headers)
                details_text = details.text
                #Construct another instance of Beautiful Soup in order to navigate the Hotel detailed page
                soup3 = BeautifulSoup(details_text, 'lxml')
                #Search the title - name of the hotel
                title = soup3.find('h1', class_='QdLfr b d Pn')
                title_text = title.text if title != None else None
                if title_text in dict_names_hotels:
                    continue
                else:
                    dict_names_hotels[title_text] = True
                info.insert(0, title_text)
                #Search the price of the hotel
                price = soup3.find('span', class_='YiGCY')
                price_final = int(price.text.split()[-1].replace(',','')) if price != None and price.text.split()[-1]!='deals' else None
                info.append(price_final)
                #Search the phone number of the hotel
                phoneNumber = soup3.find('span', class_='zNXea NXOxh NjUDn')
                phoneNumber_text = phoneNumber.text if phoneNumber != None else None
                info.append(phoneNumber_text)
                #Search the address of the hotel
                address = soup3.find('span', class_='fHvkI PTrfg')
                address_text = address.text if address != None else None
                info.append(address_text)
                #Search the general rating of the hotel
                rate = soup3.find('span', class_='uwJeR P')
                rate_text = float(rate.text) if rate != None else None
                info.append(rate_text)
                #Search other ratings like Location, Cleanliness, etc.
                otherRatings = soup3.find_all('div', class_='WdWxQ')
                for rating in otherRatings:
                    rate_value_text = float(rating.find_next('span').find_next('span').text)
                    info.append(rate_value_text)
                if len(otherRatings) <4 or not otherRatings:
                    for i in range(0, 4 - len(otherRatings) if otherRatings else 4, 1):
                        info.append(None)
                #Search differents properties of the hotel
                #The next line is to get the properties titles: Amenities, Room features and Room types
                properties = soup3.find_all('div', class_='aeQAp S5 b Pf ME')
                #Have to minimize the array because not all of the divs returned are of our interest
                final_properties = properties[0:4]
                #Search the properties in a loop. As the titles case, we have to stop at the third iteration because the other div's content
                #won't be saved
                features = soup3.find_all('div', class_='OsCbb K')
                i = 0
                for feature in features:
                    actual = ''
                    real_features = feature.find_all_next('div', class_='yplav f ME H3 _c')
                    indexx = 0
                    lenFeatures = len(real_features)
                    for j in real_features:
                        #Index to know the last feature
                        actual += j.text +', ' if indexx < lenFeatures - 2 else j.text
                    info.append(actual)
                    i+=1
                    if i == 3:
                        break
                if len(features) <3 or not features:
                    for i in range(0, 3 - len(features) if features else 3, 1):
                        info.append(None)
                #Write the row of the hotel
                print('---------------------------------------------------------------------------------')
                print(info)
                print('---------------------------------------------------------------------------------')
                thewriter.writerow(info)
            #Set the incremental variable for search hotels in the other page. In tripadvisor case, each page is +30 from the previous 
            #one in the URL. Also, the sintaxis of the URL changes as the following string
            aumento += 30
            print('AAAA', cityUrl)
            print(actual_index)
            print(limit)
            #new counter to equalize to limit
    #Make the csv file as response
    df = pd.read_csv('iceberg_test_api.csv')
    # output file
    outFileAsStr = StringIO()
    df.to_csv(outFileAsStr, index = False)
    response = StreamingResponse(
        iter([outFileAsStr.getvalue()]),
        media_type='text/csv',
        headers={
            'Content-Disposition': 'attachment;filename=dataset.csv',
            'Access-Control-Expose-Headers': 'Content-Disposition'
        }
    )
    # return
    return response 