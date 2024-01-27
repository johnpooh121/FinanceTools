import csv
import os

import cloudscraper
import pandas as pd
from crawling.kor.util import *
import time

otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
base_date = dt.datetime(year=2010, month=1, day=4)
base_path = os.path.dirname(os.path.abspath(__file__))
count = 0

def sendpacket(otp_form_data, verbose=False):
    """
    send packet with the given otp, and download the data
    :param otp_form_data: otp
    :param verbose: bool
    :return: data text string
    """
    scraper = cloudscraper.create_scraper()
    response = scraper.post(otp_url, params=otp_form_data)

    otp = response.text
    if response.status_code != 200:
        print('response code: ', response.status_code, "\n")
        return None
    if verbose:
        print("result: ", response)
        print("otp: ", otp)

    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    download_data = scraper.post(download_url, params={'code': otp})

    if download_data.status_code != 200:
        print('response code: ', download_data.status_code, "\n")
        return None

    download_data.encoding = 'EUC-KR'

    return download_data.text


def sendquery_span(target_code='KR7005930003', start_date=base_date,
                   end_date=dt.datetime.now(), verbose=False, adj=True):
    """
    send query for span type
    :param target_code: stock code string
    :param start_date: datetime
    :param end_date: datetime
    :param verbose: bool
    :param adj: bool
    :return: saved path
    """
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
    download_text = sendpacket(otp_form_data, verbose=verbose)

    fname = file_name(type='span', adj=adj, target_code=target_code, start_date=start_date, end_date=end_date)
    path = base_path + '/data/query_results/' + fname
    with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
        newfile.write(download_text)
    if verbose:
        print("saved to ", path, "\n")
    return path


def sendquery_basic(verbose=False):
    """
    Send query for basic type
    :param verbose: bool
    :return: path
    """
    otp_form_data = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        "share": '1',
        "csvxls_isNo": 'false',
        "name": 'fileDown',
        "url": 'dbms/MDC/STAT/standard/MDCSTAT01901',
    }
    download_text = sendpacket(otp_form_data, verbose=verbose)

    fname = file_name(type='basic')  # 정확한 수정 시각을 모르기 때문에 그냥 그날 날짜로 표기하는게 합리적
    path = base_path + '/data/basic/' + fname
    with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
        newfile.write(download_text)
    if verbose:
        print("saved to ", path, "\n")
    fname = 'kor_basic.csv'  # 기본 이름으로 한번 더 저장
    path = base_path + '/data/basic/' + fname
    with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
        newfile.write(download_text)
    return path


def sendquery_daily(date=dt.datetime.now(), verbose=False):
    """
    Send query for daily type
    :param date: datetime
    :param verbose: bool
    :return: path
    """
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
    download_text = sendpacket(otp_form_data, verbose=verbose)

    fname = file_name(type='daily', date=date)
    path = base_path + '/data/daily/' + fname
    with open(path, 'w', newline='', encoding='EUC-KR') as newfile:
        newfile.write(download_text)
    if verbose:
        print("saved to ", path, "\n")
    return path


def sendquery(type='basic', date=dt.datetime.now(), target_code='KR7005930003', start_date=base_date,
              end_date=dt.datetime.now(), verbose=False, adj=True):
    """
    send query
    :param type: string
    :param date: datetime
    :param target_code: stock code string
    :param start_date: datetime
    :param end_date: datetime
    :param verbose: bool
    :param adj: bool
    :return: path to the saved file
    """
    global count;count += 1
    if count > 20:
        print("long sleep\n")
        time.sleep(15)
        count = 0


    if type == 'span':
        time.sleep(5)
        return sendquery_span(target_code=target_code, start_date=start_date, end_date=end_date, adj=adj,
                              verbose=verbose)

    if type == 'basic':
        time.sleep(3)
        return sendquery_basic(verbose=verbose)

    if type == 'daily':
        time.sleep(3)
        return sendquery_daily(date=date, verbose=verbose)


if __name__ == '__main__':
    # if not os.path.exists(base_path + file_name(type='basic')) and dt.datetime.now().hour >= 11:  # 장 열릴때까지
    #     sendquery(type='basic', verbose=True)
    # sendquery(type='daily',date=dt.datetime(2024,1,16),verbose=True)
    # sendquery(type='span',start_date=dt.datetime(2023,12,20),verbose=True)
    pass
