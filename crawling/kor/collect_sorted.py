from crawling.kor import sender
from crawling.kor.util import *

def collect_targets_sorted(date=nearest_updateable_date_before_now()):
    """
    Return stock information table sorted by market cap on a given date
    :param date:
    :return: sorted stock information
    """
    fname = file_name('daily', date=date)
    path = base_path + '/data/daily/' + fname
    cnt = 0
    while not os.path.isfile(path) and cnt < 10:  # if already exists, don't send query again
        cnt += 1
        print(str(cnt) + "th trying..\n")
        sender.sendquery('daily', date=date)
        if os.path.isfile(path):
            break

    if not os.path.isfile(path):
        print("build failed!\n")
        return None
    daily_df = pd.read_csv(path, encoding='euc-kr', dtype='str')
    daily_df = daily_df.astype({'시가총액': 'int64'})
    sorted_df = daily_df.sort_values(by=['시가총액'], ascending=False)
    return sorted_df