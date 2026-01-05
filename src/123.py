import requests


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Connection": "keep-alive",
    "Referer": "https://egp.shop.caep.cn/noticeMainController/noticeMain.htm?ggpars=JPgi21SNkSP/tPaEDcocjA==",
    "Sec-Fetch-Dest": "iframe",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}
cookies = {
    "name": "value",
    "JSESSIONID": "C99843C5EBFE21F593DD9130B4E7F3D9"
}
url = "https://egp.shop.caep.cn/noticeData/showGgnrMain.htm"
params = {
    "noticeRecordId": "13320",
    "viewType": "other"
}
response = requests.get(url, headers=headers, params=params)

print(response.text)
print(response)