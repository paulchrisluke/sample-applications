# =============================================================================
# IMPORT
# =============================================================================
import datetime
import googleads
import logging
import os
import pandas as pd

# =============================================================================
# INITIALIZE
# =============================================================================
file_pathname = __file__
directory_pathname = os.path.dirname(os.path.dirname(file_pathname))
setting_pathname = directory_pathname + '/settings'
authentication_pathname = setting_pathname + '/googleads.yaml'
accountlist_pathname = setting_pathname + '/goog_accountlist.csv'
data_pathname = directory_pathname + '/data'
logger_pathname = data_pathname + '/logger'
report_pathname = data_pathname + '/report'
current_datetime = str(datetime.datetime.today().strftime('DATE[%Y-%m-%d] TIME[%HH%MM%SS]'))
handler_pathname = logger_pathname + '/' + current_datetime + '.log'
if not os.path.exists(logger_pathname):
    os.makedirs(logger_pathname)
if not os.path.exists(report_pathname):
    os.makedirs(report_pathname)

# =============================================================================
# LOGGER
# =============================================================================
file_name = __name__
logger = logging.getLogger(file_name)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(handler_pathname)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('APPLICATION INITIALIZED')

# =============================================================================
# AUTHENTICATION
# =============================================================================
adwords_client = googleads.adwords.AdWordsClient.LoadFromStorage(authentication_pathname)

# =============================================================================
# ACCOUNT LIST
# =============================================================================
def date_range_list(start_date, end_date, date_delta):
    current_date = start_date
    if not isinstance(date_delta, datetime.timedelta):
        date_delta = datetime.timedelta(**date_delta)
    while current_date <= end_date:
        yield current_date
        current_date += date_delta
try:
    my_account_id = []
    my_account_client = []
    my_account_name = []
    my_date_list = []
    df_accountlist = pd.read_csv(accountlist_pathname).fillna(0)
    for row in df_accountlist.itertuples():
        data_boolean = getattr(row, 'pull_account_data')
        date_boolean = getattr(row, 'pull_dateauto')
        if date_boolean == 1:
            current_date = datetime.datetime.today()
            start_date = datetime.date(current_date.year, current_date.month, current_date.day) - datetime.timedelta(days=4)
            end_date = datetime.date(current_date.year, current_date.month, current_date.day) - datetime.timedelta(days=1)
        else:
            start_date_manual = getattr(row, 'pull_datemanualstart')
            end_date_manual = getattr(row, 'pull_datemanualend')
            start_date_strip = datetime.datetime.strptime(start_date_manual, '%m/%d/%Y')
            end_date_strip = datetime.datetime.strptime(end_date_manual, '%m/%d/%Y')
            start_date = datetime.date(start_date_strip.year, start_date_strip.month, start_date_strip.day)
            end_date = datetime.date(end_date_strip.year, end_date_strip.month, end_date_strip.day)
        if data_boolean == 1:
            my_account_id.append(getattr(row, 'goog_account_id'))
            my_account_client.append(getattr(row, 'goog_account_client'))
            my_account_name.append(getattr(row, 'goog_account_name'))
            date_delta = datetime.timedelta(days=1)
            my_date_range = []
            for date in date_range_list(start_date, end_date, date_delta):
                my_date_range.append(str(date))
            my_date_list.append(my_date_range)
    logger.info('ACCOUNT LIST SUCCESSFUL')
except:
    logger.error('ACCOUNT LIST FAILED')

# =============================================================================
# API
# =============================================================================
def goog_report_basic(client, report, date):
    report_downloader = client.GetReportDownloader(version='v201806')
    report_query = (googleads.adwords.ReportQueryBuilder()
                    .Select('Date', 'Impressions', 'Clicks', 'Cost', 'VideoViews',
                            'VideoQuartile25Rate', 'VideoQuartile50Rate', 'VideoQuartile75Rate', 'VideoQuartile100Rate',
                            'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Id')
                    .From('AD_PERFORMANCE_REPORT')
                    .During(date)
                    .Build())
    report_downloader.DownloadReportWithAwql(report_query, 'CSV', report, skip_report_header=True,
                                             skip_column_header=False, skip_report_summary=True,
                                             include_zero_impressions=False)
for account in my_account_id:
    client = my_account_client[my_account_id.index(account)]
    name = my_account_name[my_account_id.index(account)]
    date_list = my_date_list[my_account_id.index(account)]
    adwords_client.SetClientCustomerId(account)
    for date in date_list:
        current_date = datetime.datetime.strptime(date, '%Y-%m-%d')
        my_year = datetime.datetime.strftime(current_date, '%Y')
        my_month = datetime.datetime.strftime(current_date, '%m')
        my_day = datetime.datetime.strftime(current_date, '%d')
        my_file_name = 'goog_' + account + '_' + str(my_year) + str(my_month) + str(my_day) + '.csv'
        my_file_pathname = report_pathname + '/' + my_file_name        
        digit = date.replace('-', '')
        adwords_date = str(digit) + ", " + str(digit)
        with open(my_file_pathname, 'w') as adwords_report:
            goog_report_basic(adwords_client, adwords_report, adwords_date)
        if os.path.getsize(my_file_pathname) == 0:
            os.remove(my_file_pathname)
        else:
            df = pd.read_csv(my_file_pathname)
            if df.empty:
                os.remove(my_file_pathname)
            else:
                df['Cost'] = df['Cost']/1000000
                df['Video played to 25%'] = df['Video played to 25%'].map(lambda x: x.rstrip('%')).astype(float)/100*df['Impressions']
                df['Video played to 50%'] = df['Video played to 50%'].map(lambda x: x.rstrip('%')).astype(float)/100*df['Impressions']
                df['Video played to 75%'] = df['Video played to 75%'].map(lambda x: x.rstrip('%')).astype(float)/100*df['Impressions']
                df['Video played to 100%'] = df['Video played to 100%'].map(lambda x: x.rstrip('%')).astype(float)/100*df['Impressions']
                df.to_csv(my_file_pathname, encoding='utf-8', index=False)
                logger.info('GENERATED ' + my_file_name)

# =============================================================================
# LOGGER
# =============================================================================
logger.info('EXITING APPLICATION')
logger.removeHandler(handler)
logging.shutdown()
del logger, handler