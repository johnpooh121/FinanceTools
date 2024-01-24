import time
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

stocklist = pd.read_csv("data_0543_20240115.csv", encoding='CP949')
stock_codes = list(stocklist['종목코드'])
df = pd.DataFrame()

for code in stock_codes:
    a = np.random.randint(3)
    time.sleep(a+1)
    # url 정의
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    # requests 요청
    resp = requests.get(url)
    # html 변환
    html = BeautifulSoup(resp.text, 'html.parser')
    # 종목명
    stock = html.select('div.wrap_company a')[0].text
    print(f"<{stock}>")
    try:
        price = html.select('p.no_today')[0].text.strip().split('\n')[0]
        print("{}의 현재가는 {}원 입니다.".format(stock, price))
        price = int(price.replace(',', ''))
    except:
        print("{}의 현재가는 N/A원 입니다.")
        price = np.nan
    # 시가총액, 외국인소진률, PER, PBR
    # 시가총액
    try:
        market_sum = html.select('em#_market_sum')[0].text.strip().split()
        if len(market_sum) == 1:
            print(f"{stock}의 시가총액은 {market_sum[0]}억원 입니다.")
            if len(market_sum[0]) <= 3:
                ms = int(market_sum[0])
            else:
                ms = int(market_sum[0].split(',')[0]) * 1000 + int(market_sum[0].split(',')[1])
        else:
            print(f"{stock}의 시가총액은 {market_sum[0] + ' ' + market_sum[1]}억원 입니다.")
            if len(market_sum[1]) <= 4:
                ms = int(market_sum[0][:-1]) * 10000 + int(market_sum[1])
            else:
                ms = int(market_sum[0][:-1]) * 10000 + int(market_sum[1].split(',')[0]) * 1000 + int(market_sum[1].split(',')[1])
    except:
        print(f"{stock}의 시가총액은 N/A원 입니다.")
        ms = np.nan
    # 외국인소진율(경로셀렉터)
    try:
        foreign = html.select('table.lwidth em')[2].text
        print(f"{stock}의 외국인소진율은 {foreign} 입니다.")
        frn = float(foreign[:-1])
    except:
        print(f"{stock}의 외국인소진율은 N/A% 입니다.")
        frn = np.nan
    # per
    try:
        per = float(html.select('table.per_table em#_per')[0].text)
        print(f"{stock}의 PER은 {per}배 입니다.")
    except:
        print(f"{stock}의 PER은 N/A배 입니다.")
        per = np.nan
    # pbr
    try:
        pbr = float(html.select('table.per_table em#_pbr')[0].text)
        print(f"{stock}의 PBR은 {pbr}배 입니다.")
    except:
        print(f"{stock}의 PBR은 N/A배 입니다.")
        pbr = np.nan
    print("=" * 50)
    series = pd.Series([code, price, ms, frn, per, pbr], index=['종목코드', '현재가', '시가총액(억원)', '외국인소진율(%)', 'PER', 'PBR'])
    df[stock] = series
def get():
    url = 'https://finance.yahoo.com'

    return None