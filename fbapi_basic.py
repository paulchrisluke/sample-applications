# =============================================================================
# IMPORT
# =============================================================================
import datetime
import json
import logging
import os
import pandas as pd
import time
import warnings
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

# =============================================================================
# INITIALIZE
# =============================================================================
file_pathname = __file__
# file_pathname == 'C:/Users/USER/Desktop/sample/sample-applications/fbapi.py'
directory_pathname = os.path.dirname(os.path.dirname(file_pathname))
# directory_pathname == 'C:/Users/USER/Desktop/sample'
setting_pathname = directory_pathname + '/settings'
# setting_pathname == 'C:/Users/USER/Desktop/sample/settings'
authentication_pathname = setting_pathname + '/fb_authentication.json'
# authentication_pathname == 'C:/Users/USER/Desktop/sample/settings/fb_authentication.json'
accountlist_pathname = setting_pathname + '/fb_accountlist.csv'
# accountlist_pathname == 'C:/Users/USER/Desktop/sample/settings/fb_accountlist.csv'
data_pathname = directory_pathname + '/data'
# data_pathname == 'C:/Users/USER/Desktop/sample/data'
logger_pathname = data_pathname + '/logger'
# logger_pathname == 'C:/Users/USER/Desktop/sample/data/logger'
report_pathname = data_pathname + '/report'
# report_pathname == 'C:/Users/USER/Desktop/sample/data/report'
current_datetime = str(datetime.datetime.today().strftime('DATE[%Y-%m-%d] TIME[%HH%MM%SS]'))
# current_datetime == '[YYYY-MM-DD] [00H00M00S]'
handler_pathname = logger_pathname + '/' + current_datetime + '.log'
# logger_handler == 'C:/Users/USER/Desktop/sample/data/logger/[YYYY-MM-DD] [00H00M00S].log'
if not os.path.exists(logger_pathname):
    os.makedirs(logger_pathname)
# CREATE DIRECTORY PATH TO WRITE LOGGER TO
if not os.path.exists(report_pathname):
    os.makedirs(report_pathname)
# CREATE DIRECTORY PATH TO WRITE REPORT TO

# =============================================================================
# LOGGER
# =============================================================================
file_name = __name__
# file_name == '__main__'
logger = logging.getLogger(file_name)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(handler_pathname)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('APPLICATION INITIALIZED')
# WRITE FIRST LINE TO LOGGER

# =============================================================================
# AUTHENTICATION
# =============================================================================
try:
    with open(authentication_pathname) as authentication_file:
        authentication_result = json.load(authentication_file)
    # READ AUTHENTICATION JSON FILE AND EXTRACT VALUES
    my_app_id = authentication_result['my_app_id']
    # my_app_id == <<MY_APP_ID>>
    my_app_secret = authentication_result['my_app_secret']
    # my_app_secret == <<MY_APP_SECRET>>
    my_access_token = authentication_result['my_access_token']
    # my_access_token == <<MY_ACCESS_TOKEN>>
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    # AUTHENTICATE FACEBOOK API CALL WITH APP/USER CREDENTIALS
    logger.info('AUTHENTICATION SUCCESSFUL')
except:
    logger.error('AUTHENTICATION FAILED')

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
# DEFINE FUNCTION TO EXTRACT LIST OF DATES FROM START/END SUPPLIED DATES
try:
    my_account_id = []
    my_account_client = []
    my_account_name = []
    my_date_list = []
    df_accountlist = pd.read_csv(accountlist_pathname).fillna(0)
    # READ ACCOUNT LIST CSV FILE AND EXTRACT VALUES
    for row in df_accountlist.itertuples():
        data_boolean = getattr(row, 'pull_account_data')
        # data_boolean == 0|1
        date_boolean = getattr(row, 'pull_dateauto')
        # date_boolean == 0|1
        if date_boolean == 1:
            current_date = datetime.datetime.today()
            # current_date == 'YYYY-MM-DD HH:mm:ss'
            start_date = datetime.date(current_date.year, current_date.month, current_date.day) - datetime.timedelta(days=4)
            end_date = datetime.date(current_date.year, current_date.month, current_date.day) - datetime.timedelta(days=1)
        else:
            start_date_manual = getattr(row, 'pull_datemanualstart')
            # start_date_manual == 'M/D/YYYY'
            end_date_manual = getattr(row, 'pull_datemanualend')
            # end_date_manual == 'M/D/YYYY'
            start_date_strip = datetime.datetime.strptime(start_date_manual, '%m/%d/%Y')
            end_date_strip = datetime.datetime.strptime(end_date_manual, '%m/%d/%Y')
            # EXCEL CSV DEFAULTS TO THIS DATE FORMAT
            start_date = datetime.date(start_date_strip.year, start_date_strip.month, start_date_strip.day)
            end_date = datetime.date(end_date_strip.year, end_date_strip.month, end_date_strip.day)
            # start_date == 'YYYY-MM-DD'
            # end_date == 'YYYY-MM-DD'
        if data_boolean == 1:
            my_account_id.append(getattr(row, 'fb_account_id'))
            my_account_client.append(getattr(row, 'fb_account_client'))
            my_account_name.append(getattr(row, 'fb_account_name'))
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
try:
    my_fields = [AdsInsights.Field.date_start,
                 AdsInsights.Field.date_stop,
                 AdsInsights.Field.impressions,
                 AdsInsights.Field.inline_link_clicks,
                 AdsInsights.Field.spend,
                 AdsInsights.Field.account_id,
                 AdsInsights.Field.account_name,
                 AdsInsights.Field.campaign_id,
                 AdsInsights.Field.campaign_name,
                 AdsInsights.Field.adset_id,
                 AdsInsights.Field.adset_name,
                 AdsInsights.Field.ad_id,
                 AdsInsights.Field.ad_name]
                # DEFINE REPORT FIELDS TO PULL
    my_col = ['date_start', 'date_stop',
              'impressions', 'inline_link_clicks', 'spend',
              'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name']
            # DEFINE REPORT FIELD LABELS
    for act in my_account_id:
        act_id = act
        act_client = my_account_client[my_account_id.index(act)]
        act_name = my_account_name[my_account_id.index(act)]
        date_list = my_date_list[my_account_id.index(act)]
        # LOOP THROUGH ACCOUNTS
        for date in date_list:
            my_date = {'since': str(date), 'until': str(date)}
            current_date = datetime.datetime.strptime(date, '%Y-%m-%d')
            my_year = datetime.datetime.strftime(current_date, '%Y')
            my_month = datetime.datetime.strftime(current_date, '%m')
            my_day = datetime.datetime.strftime(current_date, '%d')
            my_file_name = 'fb_' + act_id + '_' + str(my_year) + str(my_month) + str(my_day) + '.csv'
            my_file_pathname = report_pathname + '/' + my_file_name
            # LOOP THROUGH DATE RANGE
            my_params = {'level': AdsInsights.Level.ad,
                         'limit': 1000,
                         'time_increment': 1,
                         'time_range': my_date}
                        # DEFINE PARAMETERS FOR ACTIVE ADS
            my_params_deleted = {'level': AdsInsights.Level.ad,
                                 'limit': 1000,
                                 'time_increment': 1,
                                 'time_range': my_date,
                                 'filtering': [{'field': 'ad.effective_status',
                                                'operator': 'IN',
                                                'value': ['DELETED',
                                                          'ARCHIVED']}]}
                                # DEFINE PARAMETERS FOR DELETED ADS
            warnings.simplefilter('ignore')
            insights_cursor = AdAccount(act_id).get_insights(fields=my_fields, params=my_params, async=True)
            deleted_cursor = AdAccount(act_id).get_insights(fields=my_fields, params=my_params_deleted, async=True)
            # SEND REQUEST FOR ADS INSIGHTS
            timer_base = 5
            timer_increment = 1
            timer_start = 0
            insights_error = False
            deleted_error = False
            while True:
                job = insights_cursor.remote_read()
                job_percent = job['async_percent_completion']
                job_status = job['async_status']
                if job_percent == 100 and job_status == 'Job Completed':
                    break
                elif job_status == 'Job Failed':
                    insights_error = True
                    break
                time.sleep(timer_base + timer_start)
                timer_start = timer_start + timer_increment
                # WAIT FOR REQUEST JOB TO FINISH BEFORE DOWNLOADING DATA
            timer_start = 0
            while True:
                job = deleted_cursor.remote_read()
                job_percent = job['async_percent_completion']
                job_status = job['async_status']
                if job_percent == 100 and job_status == 'Job Completed':
                    break
                elif job_status == 'Job Failed':
                    deleted_error = True
                    break
                time.sleep(timer_base + timer_start)
                timer_start = timer_start + timer_increment
                # WAIT FOR REQUEST JOB TO FINISH BEFORE DOWNLOADING DATA
            if insights_error == True and deleted_error == True:
                result_insights = []
                result_deleted = []
                logger.info('INSIGHTS|DELETED >> ERROR|ERROR')
            elif insights_error == True and deleted_error == False:
                result_insights = []
                deleted_async = deleted_cursor.get_result(params={'limit': 1000})
                result_deleted = [delete for delete in deleted_async]
                logger.info('INSIGHTS|DELETED >> ERROR|PULLED')
            elif insights_error == False and deleted_error == True:
                insights_async = insights_cursor.get_result(params={'limit': 1000})
                result_insights = [insight for insight in insights_async]
                result_deleted = []
                logger.info('INSIGHTS|DELETED >> PULLED|ERROR')
            else:
                insights_async = insights_cursor.get_result(params={'limit': 1000})
                result_insights = [insight for insight in insights_async]
                deleted_async = deleted_cursor.get_result(params={'limit': 1000})
                result_deleted = [delete for delete in deleted_async]
                logger.info('INSIGHTS|DELETED >> PULLED|PULLED')
            result_ads = result_insights + result_deleted
            # COMBINE JOB RESULTS
            if result_ads == []:
                logger.info('RESULTS >> EMPTY, SKIPPING ' + my_file_name)
                continue
            else:
                output_insight = []
                for data in result_ads:
                    date_start = data['date_start']
                    date_stop = data['date_stop']
                    impressions = data['impressions']
                    inline_link_clicks = data['inline_link_clicks']
                    spend = data['spend']
                    account_id = data['account_id']
                    account_name = data['account_name']
                    campaign_id = data['campaign_id']
                    campaign_name = data['campaign_name']
                    adset_id = data['adset_id']
                    adset_name = data['adset_name']
                    ad_id = data['ad_id']
                    ad_name = data['ad_name']
                    output_insight.append([date_start, date_stop, impressions, inline_link_clicks, spend,
                                           account_id, account_name, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name])
                df = pd.DataFrame(output_insight, columns=my_col)
                df['account_id'] = 'act_' + df['account_id']
                df['campaign_id'] = 'cg:' + df['campaign_id']
                df['adset_id'] = 'c:' + df['adset_id']
                df['ad_id'] = 'a:' + df['ad_id']
                df.to_csv(my_file_pathname, encoding='utf-8', index=False)                
                logger.info('GENERATED ' + my_file_name)
                # EXTRACT VALUES FROM JSON FORMAT AND GENERATE CSV FILE
    logger.info('API SUCCESSFUL')
except:
    logger.error('API FAILED')

# =============================================================================
# LOGGER
# =============================================================================
logger.info('EXITING APPLICATION')
logger.removeHandler(handler)
logging.shutdown()
del logger, handler
# SHUT DOWN LOGGER