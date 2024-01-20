# generate.cmd 요청 주소
import cloudscraper
import pandas as pd

otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

# form data 
otp_form_data = {
    'locale': 'ko_KR',
    "share": '1',
    "csvxls_isNo": 'false',
    "name": 'fileDown',
    "url": 'dbms/MDC/STAT/standard/MDCSTAT01701',
    'strtDd': "20230323",  # 다운로드 받고싶은 날짜
    'endDd': "20230331",
    'adjStkPrc': 2,  # 수정 주가
    'adjStkPrc_check': 'Y',
    'isuCd': "KR733626K013"
}

# scraper 객체 
scraper = cloudscraper.create_scraper()

# form data와 함께 요청 
response = scraper.post(otp_url, params=otp_form_data)

# response의 text 에 담겨있는 otp 코드 가져오기  
otp = response.text

# 결과 확인 
print("result: ", response)
print("otp: ", otp)

download_url='http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
download_data=scraper.post(download_url,params={'code':otp})
download_data.encoding='EUC-KR'

contents=[]
for row in download_data.text.split('\n'):
    contents.append(row.split(','))
download_csv=pd.DataFrame(contents[1:],columns=contents[0])

date=["2024","0115"]

download_csv.to_csv(f'{date[0]}_{date[1]}.csv',index=None,encoding='EUC-KR')
# download_csv.to_csv(f'{date[0]}_{date[1]}_utf.csv',index=None,encoding='UTF-8')