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
directory_pathname = os.path.dirname(os.path.dirname(file_pathname))
setting_pathname = directory_pathname + '/settings'
authentication_pathname = setting_pathname + '/fb_authentication.json'
accountlist_pathname = setting_pathname + '/fb_accountlist.csv'
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
try:
    with open(authentication_pathname) as authentication_file:
        authentication_result = json.load(authentication_file)
    my_app_id = authentication_result['my_app_id']
    my_app_secret = authentication_result['my_app_secret']
    my_access_token = authentication_result['my_access_token']
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
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
    my_breakdowns_list = [['age', 'gender'], ['country', 'region']]
    my_breakdowns_label = ['demo', 'geo']
    # BREAKDOWNS ARE SEPARATE REPORT PULLS AND CANNOT BE COMBINED WITH EACH OTHER
    my_fields = [AdsInsights.Field.date_start,
                 AdsInsights.Field.date_stop,
                 AdsInsights.Field.impressions,
                 AdsInsights.Field.inline_link_clicks,
                 AdsInsights.Field.spend,
                 AdsInsights.Field.actions,
                 AdsInsights.Field.video_10_sec_watched_actions,
                 AdsInsights.Field.video_30_sec_watched_actions,
                 AdsInsights.Field.video_p25_watched_actions,
                 AdsInsights.Field.video_p50_watched_actions,
                 AdsInsights.Field.video_p75_watched_actions,
                 AdsInsights.Field.video_p100_watched_actions,
                 AdsInsights.Field.reach,
                 AdsInsights.Field.account_id,
                 AdsInsights.Field.account_name,
                 AdsInsights.Field.campaign_id,
                 AdsInsights.Field.campaign_name,
                 AdsInsights.Field.adset_id,
                 AdsInsights.Field.adset_name,
                 AdsInsights.Field.ad_id,
                 AdsInsights.Field.ad_name]
    my_action_breakdowns = ['action_type']
    for breakdown in my_breakdowns_list:
        list_x = breakdown[0]
        list_y = breakdown[1]
        breakdown_label = my_breakdowns_label[my_breakdowns_list.index(breakdown)]
        my_breakdowns = [list_x, list_y]
        my_col = ['date_start', 'date_stop',
                  'impressions', 'inline_link_clicks', 'spend',
                  'video_view_3sec', 'video_view_10sec', 'video_view_30sec',
                  'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions', 'video_p100_watched_actions',
                  'comment', 'like', 'post', 'post_reaction', 'reach',
                  list_x, list_y,
                  'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name']
        for act in my_account_id:
            act_id = act
            act_client = my_account_client[my_account_id.index(act)]
            act_name = my_account_name[my_account_id.index(act)]
            date_list = my_date_list[my_account_id.index(act)]
            for date in date_list:
                my_date = {'since': str(date), 'until': str(date)}
                current_date = datetime.datetime.strptime(date, '%Y-%m-%d')
                my_year = datetime.datetime.strftime(current_date, '%Y')
                my_month = datetime.datetime.strftime(current_date, '%m')
                my_day = datetime.datetime.strftime(current_date, '%d')
                my_file_name = 'fb_' + act_id + '_' + str(my_year) + str(my_month) + str(my_day) + '_' + breakdown_label + '.csv'
                my_file_pathname = report_pathname + '/' + my_file_name
                my_params = {'action_breakdowns': my_action_breakdowns,
                             'breakdowns': my_breakdowns,
                             'level': AdsInsights.Level.ad,
                             'limit': 1000,
                             'time_increment': 1,
                             'time_range': my_date}
                my_params_deleted = {'action_breakdowns': my_action_breakdowns,
                                     'breakdowns': my_breakdowns,
                                     'level': AdsInsights.Level.ad,
                                     'limit': 1000,
                                     'time_increment': 1,
                                     'time_range': my_date,
                                     'filtering': [{'field': 'ad.effective_status',
                                                    'operator': 'IN',
                                                    'value': ['DELETED',
                                                              'ARCHIVED']}]}
                warnings.simplefilter('ignore')
                insights_cursor = AdAccount(act_id).get_insights(fields=my_fields, params=my_params, async=True)
                deleted_cursor = AdAccount(act_id).get_insights(fields=my_fields, params=my_params_deleted, async=True)
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
                        try:
                            actions = data['actions']
                            my_actions = ['video_view']
                            action_filter = [d for d in actions if d['action_type'] in my_actions]
                            video_view_3sec = pd.io.json.json_normalize(action_filter)['value'].values
                        except:
                            video_view_3sec = [0]
                        try:
                            video_view_10sec = data['video_10_sec_watched_actions']
                            video_view_10sec = pd.io.json.json_normalize(video_view_10sec)['value'].values
                        except:
                            video_view_10sec = [0]
                        try:
                            video_view_30sec = data['video_30_sec_watched_actions']
                            video_view_30sec = pd.io.json.json_normalize(video_view_30sec)['value'].values
                        except:
                            video_view_30sec = [0]
                        try:
                            video_p25_watched_actions = data['video_p25_watched_actions']
                            video_p25_watched_actions = pd.io.json.json_normalize(video_p25_watched_actions)['value'].values
                        except:
                            video_p25_watched_actions = [0]
                        try:
                            video_p50_watched_actions = data['video_p50_watched_actions']
                            video_p50_watched_actions = pd.io.json.json_normalize(video_p50_watched_actions)['value'].values
                        except:
                            video_p50_watched_actions = [0]
                        try:
                            video_p75_watched_actions = data['video_p75_watched_actions']
                            video_p75_watched_actions = pd.io.json.json_normalize(video_p75_watched_actions)['value'].values
                        except:
                            video_p75_watched_actions = [0]
                        try:
                            video_p100_watched_actions = data['video_p100_watched_actions']
                            video_p100_watched_actions = pd.io.json.json_normalize(video_p100_watched_actions)['value'].values
                        except:
                            video_p100_watched_actions = [0]
                        try:
                            actions = data['actions']
                            my_actions = ['comment']
                            action_filter = [d for d in actions if d['action_type'] in my_actions]
                            comment = pd.io.json.json_normalize(action_filter)['value'].values
                        except:
                            comment = [0]
                        try:
                            actions = data['actions']
                            my_actions = ['like']
                            action_filter = [d for d in actions if d['action_type'] in my_actions]
                            like = pd.io.json.json_normalize(action_filter)['value'].values
                        except:
                            like = [0]
                        try:
                            actions = data['actions']
                            my_actions = ['post']
                            action_filter = [d for d in actions if d['action_type'] in my_actions]
                            post = pd.io.json.json_normalize(action_filter)['value'].values
                        except:
                            post = [0]
                        try:
                            actions = data['actions']
                            my_actions = ['post_reaction']
                            action_filter = [d for d in actions if d['action_type'] in my_actions]
                            post_reaction = pd.io.json.json_normalize(action_filter)['value'].values
                        except:
                            post_reaction = [0]
                        reach = data['reach']
                        breakdown1 = data[list_x]
                        breakdown2 = data[list_y]
                        account_id = data['account_id']
                        account_name = data['account_name']
                        campaign_id = data['campaign_id']
                        campaign_name = data['campaign_name']
                        adset_id = data['adset_id']
                        adset_name = data['adset_name']
                        ad_id = data['ad_id']
                        ad_name = data['ad_name']
                        output_insight.append([date_start, date_stop, impressions, inline_link_clicks, spend,
                                       video_view_3sec, video_view_10sec, video_view_30sec,
                                       video_p25_watched_actions, video_p50_watched_actions, video_p75_watched_actions, video_p100_watched_actions,
                                       comment, like, post, post_reaction, reach,
                                       breakdown1, breakdown2,
                                       account_id, account_name, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name])
                    df = pd.DataFrame(output_insight, columns=my_col)
                    df['video_view_3sec'] = df['video_view_3sec'].str.get(0)
                    df['video_view_10sec'] = df['video_view_10sec'].str.get(0)
                    df['video_view_30sec'] = df['video_view_30sec'].str.get(0)
                    df['video_p25_watched_actions'] = df['video_p25_watched_actions'].str.get(0)
                    df['video_p50_watched_actions'] = df['video_p50_watched_actions'].str.get(0)
                    df['video_p75_watched_actions'] = df['video_p75_watched_actions'].str.get(0)
                    df['video_p100_watched_actions'] = df['video_p100_watched_actions'].str.get(0)
                    df['comment'] = df['comment'].str.get(0)
                    df['like'] = df['like'].str.get(0)
                    df['post'] = df['post'].str.get(0)
                    df['post_reaction'] = df['post_reaction'].str.get(0)
                    df['account_id'] = 'act_' + df['account_id']
                    df['campaign_id'] = 'cg:' + df['campaign_id']
                    df['adset_id'] = 'c:' + df['adset_id']
                    df['ad_id'] = 'a:' + df['ad_id']
                    df.to_csv(my_file_pathname, encoding='utf-8', index=False)                
                    logger.info('GENERATED ' + my_file_name)
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