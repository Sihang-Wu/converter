
import requests

# url = "https://webhook.site/23d705b0-273d-467a-b2e4-ad3877fe884b"
# url = "https://global.juheapi.com/weather/v1/city?q=London,uk&apikey=AUZmcJiswzhjSWl4CbDZwu1Rqrzwf8OJ"
# params = {"user":"sihang","age":18}
# #query parameters

# response = requests.get(url)
# print(response.text)

url2 = "https://global.juheapi.com/aqi/v1/city?apikey=AUZmcJiswzhjSWl4CbDZwu1Rqrzwf8OJ&q=beijing"
response2 = requests.get(url2)
print(response2.text)

params = {"apikey":"AUZmcJiswzhjSWl4CbDZwu1Rqrzwf8OJ","q":"beijing"}
response3 = requests.get(url2,params)
#







audienceinfo1 = {
    "id": "xxx",
    "name": [],
    "description": [],
    "scale": 0,
    "URL": [],
    "price": []
}



params = {
    "dmpID": "xxx",
    "audiences": [audienceinfo1],
    "userID": "xxx",
    "siteID": 0
}




