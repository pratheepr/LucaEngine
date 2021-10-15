import pandas as pd
import numpy as np
from datetime import datetime
from LucaDB import DBAccess as db

pd.options.mode.chained_assignment = None


# testing

def select_fltr_df(inp_data_frame, tag_name, time_frame):
    df_snapshot = inp_data_frame[inp_data_frame['OPC_TAG'] == tag_name]
    df_refr_data = df_snapshot[
        df_snapshot['READREQ_TIMESTAMP'] >= datetime.now() - pd.Timedelta(seconds=int(time_frame))]
    df_refr_data['TAG_VALUE'] = pd.to_numeric(df_refr_data['TAG_VALUE'], errors='coerce')

    return df_refr_data


def get_alert_input(inp_row, inp_data_frame):
    out_alert_tag_name = inp_row['TAG_NAME']
    out_alert_time_frame = inp_row['CHECK_DURATION_IN_SECS']
    out_alert_lg_operator = inp_row['TAG_CONDITION']
    out_alert_inp_threshold = inp_row['THRESHOLD_VALUE']
    out_alert_pcntg_abv_thold = inp_row['PCNTG_ABOVE_THRESHOLD']

    out_df_alert_inp_frame = select_fltr_df(inp_data_frame=inp_data_frame, tag_name=out_alert_tag_name,
                                            time_frame=out_alert_time_frame)

    return out_alert_tag_name, out_alert_time_frame, out_alert_lg_operator, out_alert_inp_threshold, out_alert_pcntg_abv_thold, out_df_alert_inp_frame


def process_rule_logical_oper(ConnObj, inp_data_frame, tag_name, time_frame, lg_operator, inp_threshold,
                              inp_pcntg_ab_thold):
    df_snapshot = inp_data_frame[inp_data_frame['OPC_TAG'] == tag_name]
    df_refr_data = df_snapshot[
        df_snapshot['READREQ_TIMESTAMP'] >= datetime.now() - pd.Timedelta(seconds=int(time_frame))]
    df_refr_data['TAG_VALUE'] = pd.to_numeric(df_refr_data['TAG_VALUE'], errors='coerce')

    no_of_rows = df_refr_data.shape[0]
    tmp_Crt_Alert_Ind = 0

    if no_of_rows == 0:
        print('No Data found for Alarm')
    else:
        df_fltr_threshold = df_refr_data

        if lg_operator == '>':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_VALUE'] > inp_threshold])  # .shape(0)
        elif lg_operator == '<':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_VALUE'] < inp_threshold])  # .shape(0)
        elif lg_operator == '>=':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_VALUE'] >= inp_threshold])  # .shape(0)
        elif lg_operator == '<=':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_VALUE'] <= inp_threshold])  # .shape(0)
        elif lg_operator == '==':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_VALUE'] == inp_threshold])  # .shape(0)
        elif lg_operator == '=NULL':
            df_fltr_threshold = (df_refr_data[df_refr_data['TAG_STATUS'] == 'Error'])

        nof_fltr_threshold = df_fltr_threshold.shape[0]

        Pcntg_above_threshold = round((nof_fltr_threshold / no_of_rows) * 100, 3)

        print('No. of dpts in last %2d hours : %2d' % (time_frame, no_of_rows))
        print('No. of values above threshold: %2d' % nof_fltr_threshold)
        print('% of values above threshold:', Pcntg_above_threshold)

        if Pcntg_above_threshold >= inp_pcntg_ab_thold:
            tmp_Crt_Alert_Ind = 1
        else:
            tmp_Crt_Alert_Ind = 0

        return tmp_Crt_Alert_Ind, Pcntg_above_threshold


def Create_Alert(ConnObj, Pcntg_above_threshold, row):
    alerts_alerting_rules_sid = row['SID']
    alerts_alert_comments = row['ALARM_NAME'] + ' :' + row['TAG_NAME'] + ' ' + row['TAG_CONDITION'] + ' ' + str(
        row['THRESHOLD_VALUE']) + ' for duration: ' + str(row['CHECK_DURATION_IN_SECS'])
    alerts_alert_condition = row['TAG_NAME'] + ' ' + row['TAG_CONDITION'] + ' ' + str(row['THRESHOLD_VALUE'])
    alerts_actual_value = Pcntg_above_threshold

    print('**** ALERT CREATED ****')
    print(row['PCNTG_ABOVE_THRESHOLD'])

    db.Alerts_Insert(ConnObj=ConnObj,
                     Alerting_Rules_sid=alerts_alerting_rules_sid,
                     Alert_Conditiion=alerts_alert_condition,
                     Actual_Value=alerts_actual_value,
                     Alert_Mail_Sent='Y',
                     Alert_Comments=alerts_alert_comments
                     )


def process_rule_flatline(ConnObj, inp_data_frame, tag_name, time_frame):
    # df_find_slope = inp_data_frame[['LOAD_TIMESTAMP','TAG_VALUE']]

    x = inp_data_frame['READREQ_TIMESTAMP']
    y = inp_data_frame['TAG_VALUE'].astype(float)

    x_seq = np.arange(x.size)

    fit = np.polyfit(x_seq, y, 1)
    fit_fn = np.poly1d(fit)
    slope = round(fit[0], 5)

    print('Slope = ', fit[0], ", ", "Intercept = ", fit[1])
    print(fit_fn)
    print(abs(slope))
    print('Last y value')
    print(y.axes)

    rounded_slope = abs(slope)
    flatline_value = round(fit[1], 2)

    return rounded_slope, flatline_value


if __name__ == "__main__":

    # pd_data = pd.read_csv('opcread_metrics.csv', parse_dates=['datetime'], infer_datetime_format=True)

    # DB_Location ='/Users/pratheepravysandirane/PycharmProjects/LucaEngine/LucaDB/cnrl_alerts.db'
    DB_Location = '../LucaDB/cnrl_alerts.db'

    DbConnObj = db.CrtConnObject(DB_Location)

    db_records = db.OpcTransLog_Select(ConnObj=DbConnObj, No_Of_Days=2)

    df_OpcTransLog = pd.DataFrame(db_records,
                                  columns=['SID', 'OPC_TAG', 'TAG_VALUE', 'TAG_STATUS', 'READREQ_TIMESTAMP'])
    df_OpcTransLog['TAG_VALUE'] = pd.to_numeric(df_OpcTransLog['TAG_VALUE'], errors='coerce')

    db_alerting_rules_rcd = db.AlertingRules_Select(ConnObj=DbConnObj)
    df_alerting_rules = pd.DataFrame(db_alerting_rules_rcd,
                                     columns=['SID', 'BUSS_AREA', 'ALARM_NAME', 'ALARM_DESC', 'TAG_NAME',
                                              'TAG_CONDITION', 'THRESHOLD_VALUE',
                                              'PCNTG_ABOVE_THRESHOLD', 'CHECK_DURATION_IN_SECS', 'MULTI_COND',
                                              'LOGIC_FLOW_ORDER', 'LOGICAL_OPERATOR', 'ALERT_ACTIVE',
                                              'SUPPRESS_AFTR_ALERT_IN_SECS', 'ALERT_RECEPIENTS']
                                     )

    df_alerting_rules.sort_values(by=['ALARM_NAME', 'SID'], inplace=True)
    # print(df_alerting_rules.columns)

    multi_cond_ind = 'N'
    # curr_alarm_name = ''
    prev_alarm_name = ''
    multi_cond_result = ''
    df_multi_cond_eval = pd.DataFrame
    df_multicond_eval = pd.DataFrame(columns=['condition', 'func_output'])
    multi_cond_idx = 1

    for index, row in df_alerting_rules.iterrows():
        # print(row['ALARM_NAME'], row['TAG_NAME'])
        print(row)

        if row['MULTI_COND'] == 'Y' or multi_cond_ind == 'Y':
            print('MULTI COND')

            curr_alarm_name = row['ALARM_NAME']

            if len(prev_alarm_name) == 0:
                prev_alarm_name = row['ALARM_NAME']

            alert_tag_name, alert_time_frame, alert_lg_operator, \
            alert_inp_threshold, alert_pcntg_abv_thold, df_alert_inp_frame = get_alert_input(inp_row=row,
                                                                                             inp_data_frame=df_OpcTransLog)
            Crt_Alert_Ind = 0
            Pcntg_above_threshold = 0

            if row['TAG_CONDITION'] in ['<', '>', '=', '>=', '<=', '=NULL']:

                if df_alert_inp_frame.shape[0] > 0:
                    Crt_Alert_Ind, Pcntg_above_threshold = process_rule_logical_oper(ConnObj=DbConnObj,
                                                                                     inp_data_frame=df_alert_inp_frame,
                                                                                     tag_name=alert_tag_name,
                                                                                     time_frame=alert_time_frame,
                                                                                     lg_operator=alert_lg_operator,
                                                                                     inp_threshold=alert_inp_threshold,
                                                                                     inp_pcntg_ab_thold=alert_pcntg_abv_thold
                                                                                     )



            elif row['TAG_CONDITION'] == 'FLATLINE':
                print('FLATLINE CHECK')
                print(row['ALARM_NAME'])

                line_slope = 0
                flatline_value = 0

                if df_alert_inp_frame.shape[0] > 0:
                    line_slope, flatline_value = process_rule_flatline(ConnObj=DbConnObj,
                                                                       inp_data_frame=df_alert_inp_frame,
                                                                       tag_name=alert_tag_name,
                                                                       time_frame=alert_time_frame
                                                                       )
                if line_slope < 0.1:
                    row['THRESHOLD_VALUE'] = flatline_value
                    Crt_Alert_Ind = 1
                    # Create_Alert(ConnObj=DbConnObj, Pcntg_above_threshold=flatline_value, row=row)

            df_multicond_eval.at[multi_cond_idx, 'condition'] = row['LOGICAL_OPERATOR']

            if Crt_Alert_Ind == 1:
                df_multicond_eval.at[multi_cond_idx, 'func_output'] = 1
            else:
                df_multicond_eval.at[multi_cond_idx, 'func_output'] = 0

            multi_cond_idx = multi_cond_idx + 1

            if len(row['LOGICAL_OPERATOR']) == 0:
                print('****EVALUATE MULTI CONDITION OPERATION ****')
                final_eval = 0
                prev_alarm_name = ''

                for idx, rw in df_multicond_eval.iterrows():
                    print(idx)
                    print(rw)
                    if idx == 1:
                        prev_oper = rw['condition']
                        # prev_func_output = rw['func_output']
                        final_eval = rw['func_output']
                    else:
                        if prev_oper == 'AND':
                            final_eval = final_eval * rw['func_output']
                        else:
                            final_eval = final_eval + rw['func_output']
                        prev_oper = rw['condition']

                if final_eval >= 1:
                    Create_Alert(ConnObj=DbConnObj, Pcntg_above_threshold=Pcntg_above_threshold, row=row)

                multi_cond_idx = 1
                # Create_Alert(ConnObj=DbConnObj, Pcntg_above_threshold=Pcntg_above_threshold, row=row)
        else:

            alert_tag_name, alert_time_frame, alert_lg_operator, \
            alert_inp_threshold, alert_pcntg_abv_thold, df_alert_inp_frame = get_alert_input(inp_row=row,
                                                                                             inp_data_frame=df_OpcTransLog)

            if row['TAG_CONDITION'] in ['<', '>', '=', '>=', '<=', '=NULL']:
                print('Single Cond:')
                print(row['ALARM_NAME'])
                print(row['CHECK_DURATION_IN_SECS'])

                if df_alert_inp_frame.shape[0] > 0:
                    Crt_Alert_Ind, Pcntg_above_threshold = process_rule_logical_oper(ConnObj=DbConnObj,
                                                                                     inp_data_frame=df_alert_inp_frame,
                                                                                     tag_name=alert_tag_name,
                                                                                     time_frame=alert_time_frame,
                                                                                     lg_operator=alert_lg_operator,
                                                                                     inp_threshold=alert_inp_threshold,
                                                                                     inp_pcntg_ab_thold=alert_pcntg_abv_thold)

                    if Crt_Alert_Ind == 1:
                        Create_Alert(ConnObj=DbConnObj, Pcntg_above_threshold=Pcntg_above_threshold, row=row)

            elif row['TAG_CONDITION'] == 'FLATLINE':
                print('FLATLINE CHECK')
                print(row['ALARM_NAME'])

                if df_alert_inp_frame.shape[0] > 0:
                    line_slope, flatline_value = process_rule_flatline(ConnObj=DbConnObj,
                                                                       inp_data_frame=df_alert_inp_frame,
                                                                       tag_name=alert_tag_name,
                                                                       time_frame=alert_time_frame
                                                                       )

                    if line_slope < 0.1:
                        row['THRESHOLD_VALUE'] = flatline_value
                        Create_Alert(ConnObj=DbConnObj, Pcntg_above_threshold=flatline_value, row=row)
