
'''
code for read api restcountries, return data json 

'''


import requests 
import json
import pandas as pd
def read_api(api_url) :

    response = requests.get(api_url)
    if response.status_code == 200 :               
        response_json = response.json()
        print("Read is completed!!")
    else:
        print(f"Something went wrong --> code : {response.status_code}")
    return response_json



if __name__ == "__main__":

    print("start of get values API")
    api_url = "https://restcountries.com/v2/all"
    json_result = read_api(api_url)

    with open('all.json', 'w') as outfile:
        outfile.write(json_result)

