# Crawling Finance Information

## 1. KOSPI / KOSDAQ / KONEX

There are three options to get finance information.

- get every stock's price-related information for a certain day (전종목 시세)
- get every stock's basic information of today (전종목 기본정보) 
- get a certain stock's price-related information for a certain time span (개별종목 시세 추이) 
#### Warning : Adj Close is only possible in second form query.

To process query, it first checks whether it already exists in the
directory. If not, it sends query to the KRX, get data and save locally.

 ---

File name's format of saved data is following:
 - daily(전종목 시세) :
   - if year<=2020 : {date}dt  (date : YYYYMMDD)
   - kor_daily_{requested date}
 - basic (전종목 기본정보) :
   - kor_basic
 - specific target's data (개별종목 시세) : 
   - {target_code}.csv

> for newly created, every date is in YYMMDD form

> every file is saved in csv form

Catalog file "catalog.csv" will manage each target's latest date

## 2. My Learnings

Adj Close is only available in `개별종목 시세`

E.g. 삼성전자(005930) is 2,000,000 won in daily query in 2018.4.20. This means
that we don't have to update daily query result in past, because
it doesn't change.

MS Excel doesn't show leading zero when opening csv file.

KRX의 수정주가는 배당락은 고려하지 않고 액면분할만 고려하는 듯 하다  
또한 개별종목 시세에서 쿼리를 액면분할 이전으로 잡으면 수정주가에 현재의
액면분할이 반영이 안된다. 따라서 쿼리를 한번에 과거부터 현재까지 끊지 않고
보내도록 수정했다.

KRX의 거래량 수치는 1주 단위이고 수정주가의 적용 여부와 상관 없이 일정하다.
액면분할의 경우 전 후로 거래량의 크기가 분할비율만큼 조정되는 것을 볼 수 있다.
따라서 실제로 현재 기준의 거래량 수치를 얻으려면 액면분할의 비를 알아야 하는데,
이를 얻는 방법으로 그냥 종가(수정x)와 다음 날 시가를 비교하는 방법과 액면분할
전후의 상장주식 수를 비교하는 방법이 있다. 그러나 상장주식 수는 액면분할이 아니어도
감자 등의 이유로 변할 수 있다.

## 3. Todo for future

 - storing each day's total query count
 - 수정주가 반영 없이 raw로 top400 모으기
 - 연속일 간 종가와 다음날 시가 비교로 액면분할 감지해서 따로 /data/adj에 저장
 - db와 연계