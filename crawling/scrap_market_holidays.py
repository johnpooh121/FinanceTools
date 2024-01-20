import csv
import os

import cloudscraper
import pandas as pd
from crawling.util import *

# otp_url = 'https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp'
otp_url = 'https://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
base_path = os.path.dirname(os.path.abspath(__file__))

# def sendquery_holidays(year,verbose=False): # year : string
#     otp_form_data = {
#         'name': 'fileDown',
#         'filetype': 'xls',
#         'url': 'MKD/01/0110/01100305/mkd01100305_01',
#         'search_bas_yy': '2024',
#         'gridTp': 'KRX',
#         'pagePath': '/contents/MKD/01/0110/01100305/MKD01100305.jsp',
#     }
#     scraper = cloudscraper.create_scraper()
#     response = scraper.get(otp_url, params=otp_form_data)
#
#     otp = response.text
#
#     # if verbose:
#     #     # print("result: ", response)
#     #     # print("otp: ", otp)
#
#     download_url = 'https://file.krx.co.kr/download.jspx'
#     download_data = scraper.get(download_url, params={'code': otp})
#     download_data.encoding = 'EUC-KR'
#
#     fname = 'kor_holidays_' + str(year) + '.xls'
#     path = base_path+'/data/holidays/' + fname
#     with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
#         newfile.write(download_data.text)
#     if verbose:
#         print("saved to ", path, "\n")
#     return
#
if __name__ == '__main__':
    # sendquery_holidays('2024',verbose=True)
    past_span=pd.read_csv(base_path+"/data/holidays/data_005930_adj_20100104_20240115.csv",encoding='EUC-KR')
    holiday_2024=pd.read_excel(base_path+"/data/holidays/holiday_2024.xls")
    curr=dt.datetime(2010,1,4)
    last_2020=dt.datetime(2023,12,31)

    past_span_date=past_span['일자']
    date_list=past_span_date.values
    data_set=set(map(lambda x: dt.datetime.strptime(x,"%Y/%m/%d"),date_list))
    week_holiday=[]
    while curr <= last_2020:
        if curr.weekday()<=4 and curr not in data_set:
            week_holiday.append(curr)
        curr += dt.timedelta(days=1)
    holiday_2024_list=holiday_2024.iloc[:,0].values
    holiday_2024_list=list(map(lambda x: dt.datetime.strptime(x,"%Y-%m-%d"),holiday_2024_list))
    week_holiday+=holiday_2024_list
    week_holiday_str=list(map(lambda x: x.strftime('%Y-%m-%d'),week_holiday))
    ret_df=pd.DataFrame({'week holiday':week_holiday_str})
    ret_df.to_csv(base_path+"/data/holidays/holiday_total.csv",index=False)
    pass