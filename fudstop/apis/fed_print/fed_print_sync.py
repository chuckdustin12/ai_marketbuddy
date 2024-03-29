import aiohttp
import asyncio
import os
import pandas as pd
import requests
from typing import List

from .fedprint_models import Series, Search, Item, SingleItem

class FedPrintSync:
    """
    Queries Federal Reserve documents 

    
    """
    def __init__(self):
        self.headers = {'X-API-Key': os.environ.get('YOUR_FED_PRINT_KEY')}

        self.session = requests.session()

    def series(self):
        url = "https://fedinprint.org/api/series"
        
     
        data = self.session.get(url).json()
        return Series(data)
                

    def search(self, filter:str, limit:str='10', page:str='1'):
        """
        Search Fed Reserve published papers

        ARGUMENTS:

        >>> page: the page to start on (optional - default 1)
        >>> limit: the number of results returned (optional - default 10)
        >>> filter: filter by: (optional - default year)

            - year 
            - provider
            - series
            - contenttype
            - jel
        """
    

  
        url = f"https://fedinprint.org/api/item/search?page={page}&limit={limit}&filter={filter}"
        print(url)

        data = self.session.get(url, headers=self.headers).json()
        print(data)
        search_object = Search(data)  # create a Search object
        return search_object

    
    def get_series_id(self):
        """
        Returms a list of series IDs to be used with the 
        get_item function.
        
        """
        get_series = self.series()
        return get_series.id



    def get_series(self, id):
        """
        Uses the IDS in the series ID list returned from get_series_id

        >>> Argument: id - the id to query
        
        """
       
        url = f"https://fedinprint.org/api/series/{id}/items"
        print(url)
        

        try:
            data = self.session.get(url, headers=self.headers).json()
            series_data = Series(data)  # assuming Series is a class you've defined
            return series_data
        except Exception as e:
            print(f"An error occurred: {e}")
  

    def get_item(self, page:str='1', limit:str='25'):
        """
        Returns a list of all items and the associated item record metadata.

        Arguments:

        >>> page: optional
        >>> limit: optional

        
        """


        url = f"https://fedinprint.org/api/item?page={page}&limit={limit}"
        

        data = self.session.get(url, headers=self.headers).json()
             
        records = data.get('records')

        data = Item(records)
        return data

    def print_file_urls(self, limit:str='25'):
        """
        Returns the URLs to the documents

        Arguments:

        >>> limit: the amount of URLs to return (optional - default 25)
        
        """
        file_urls = self.get_item(limit)
      
        return file_urls.fileurl
