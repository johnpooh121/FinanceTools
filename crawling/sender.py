import csv
import os

import cloudscraper
import pandas as pd
from crawling.util import *

otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
base_date = dt.datetime(year=2010, month=1, day=4)
base_path = os.path.dirname(os.path.abspath(__file__))


def sendquery(type='basic', date=dt.datetime.now(), target_code='KR7005930003', start_date=base_date,
              end_date=dt.datetime.now(), verbose=False, adj=True):
    if type == 'span':
        otp_form_data = {
            'locale': 'ko_KR',
            "share": '1',
            "csvxls_isNo": 'false',
            "name": 'fileDown',
            "url": 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'strtDd': start_date.strftime('%Y%m%d'),
            'endDd': end_date.strftime('%Y%m%d'),
            'adjStkPrc': 2 if adj else 1,  # adj price
            'adjStkPrc_check': 'Y' if adj else 'N',  # adj price
            'isuCd': target_code
        }
        scraper = cloudscraper.create_scraper()
        response = scraper.post(otp_url, params=otp_form_data)

        otp = response.text
        if response.status_code != 200:
            print('response code: ', response.status_code, "\n")
            return False, None
        if verbose:
            print("result: ", response)
            print("otp: ", otp)

        download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        download_data = scraper.post(download_url, params={'code': otp})

        if download_data.status_code != 200:
            print('response code: ', download_data.status_code, "\n")
            return False, None

        download_data.encoding = 'EUC-KR'

        fname = file_name(type='span', adj=adj,target_code=target_code, start_date=start_date, end_date=end_date)
        path = base_path + '/data/query_results/' + fname
        with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
            newfile.write(download_data.text)
        if verbose:
            print("saved to ", path, "\n")
        return True, path

    if type == 'basic':
        otp_form_data = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            "share": '1',
            "csvxls_isNo": 'false',
            "name": 'fileDown',
            "url": 'dbms/MDC/STAT/standard/MDCSTAT01901',
        }
        scraper = cloudscraper.create_scraper()
        response = scraper.post(otp_url, params=otp_form_data)

        otp = response.text
        if response.status_code != 200:
            print('response code: ', response.status_code, "\n")
            return False, None
        if verbose:
            print("result: ", response)
            print("otp: ", otp)

        download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        download_data = scraper.post(download_url, params={'code': otp})

        if download_data.status_code != 200:
            print('response code: ', download_data.status_code, "\n")
            return False, None
        download_data.encoding = 'EUC-KR'

        fname = file_name(type='basic')
        path = base_path + '/data/basic/' + fname
        with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
            newfile.write(download_data.text)
        if verbose:
            print("saved to ", path, "\n")
        return True, path

    if type == 'daily':
        otp_form_data = {
            'locale': 'ko_KR',
            "mktId": "ALL",
            "trdDd": date.strftime("%Y%m%d"),
            "share": '1',
            "money": "1",
            "csvxls_isNo": 'false',
            "name": 'fileDown',
            "url": 'dbms/MDC/STAT/standard/MDCSTAT01501',
        }
        scraper = cloudscraper.create_scraper()
        response = scraper.post(otp_url, params=otp_form_data)

        otp = response.text
        if response.status_code != 200:
            print('response code: ', response.status_code, "\n")
            return False, None
        if verbose:
            print("result: ", response)
            print("otp: ", otp)

        download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        download_data = scraper.post(download_url, params={'code': otp})

        if download_data.status_code != 200:
            print('response code: ', download_data.status_code, "\n")
            return False, None
        download_data.encoding = 'EUC-KR'

        fname = file_name(type='daily', date=date)
        path = base_path + '/data/daily/' + fname
        with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
            newfile.write(download_data.text)
        if verbose:
            print("saved to ", path, "\n")
        return True, path


if __name__ == '__main__':
    if not os.path.exists(base_path + file_name(type='basic')) and dt.datetime.now().hour >= 11:  # 장 열릴때까지
        sendquery(type='basic', verbose=True)
    # sendquery(type='daily',date=dt.datetime(2024,1,16),verbose=True)
    # sendquery(type='span',start_date=dt.datetime(2023,12,20),verbose=True)
    pass
