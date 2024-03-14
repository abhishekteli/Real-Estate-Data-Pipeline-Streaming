from confluent_kafka import Producer
import requests
import logging
import json
import time
import os
from dotenv import load_dotenv
from StreamingExtract import streamingGold

load_dotenv()
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class streamingBronze:

    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'auto.offset.reset': 'earliest'
        }
        self.producer = Producer(self.conf)
        self.topic = 'realEstates'

    def getRequest(self, location, hometype, currentPage=1):
        try:
            url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"

            querystring = {"location": location, "page": currentPage, "home_type": hometype}

            headers = {
                "X-RapidAPI-Key": os.getenv('X-RAPIDAPI-KEY'),
                "X-RapidAPI-Host": os.getenv('X-RAPIDAPI-HOST')
            }

            response = requests.get(url, headers=headers, params=querystring)
            return response
        except requests.exceptions.RequestException as req_err:
            logging.error(f'Request error occurred: {req_err}')

        except ValueError as json_err:
            logging.error(f'JSON decode error: {json_err}')

        except Exception as e:
            logging.error(f'An unexpected error occurred: {e}')

    def getTotalpages(self, location, hometype):

        response = self.getRequest(location, hometype, 1)
        totalPages = response.json()['totalPages']
        return totalPages

    def extractAndSaveRawData(self, location, hometype, currentPage, totalPages):

        while currentPage <= totalPages:
            print(f"Extracting Data for location : {location} from page no: {currentPage}...", end='')
            response = self.getRequest(location, hometype, 1)
            data = response.json()['props']
            self.producer.produce(self.topic, json.dumps(data).encode('utf-8'))
            print('Done')
            currentPage = currentPage + 1
            time.sleep(15)
        self.producer.flush()


if __name__ == "__main__":
    sb = streamingBronze()
    sg = streamingGold()
    sQuery = sg.processData()
    location = input("\nEnter the location: ")
    hometype = 'Apartments_Condos_Co-ops'
    while True:
        totalPages = sb.getTotalpages(location, hometype)
        sb.extractAndSaveRawData(location, hometype, 1,totalPages)
        moreLocation = input("Do you want to check for new location(Y/n): ")
        if moreLocation == 'Y':
            location = input("Enter the new location: ")
        else:
            break
    sQuery.stop()
