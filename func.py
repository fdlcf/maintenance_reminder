import sys
import psycopg2
import pandas as pd
import os
from datetime import datetime
import openpyxl


def zendesk(path: str):
    all_files = os.listdir(path)
    report = list(filter(lambda f: f.endswith('.csv'), all_files))
    filepath = os.path.join(path, str(*report))
    try:
        df = pd.read_csv(filepath)
        df2 = df[['Гос номер ТС', 'Статус']]
    except:
        df2 = pd.DataFrame({'Гос номер ТС':[], 'Статус':[]})
    return df2

def connect_to_psql(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def get_data_from_db(conn, sql_vehicles = str, sql_events = str, sql_last_pm = str, sql_check_pm_list = str, sql_pm_period = str, sql_zero_pm = str):
    print("sql data execution are in proccess...")
    try:
        vehicles = pd.read_sql(sql_vehicles, conn)
        vehicles['created_at'] = vehicles['created_at'].dt.tz_localize(None)
    except:
        print("vehicles execution error")

    # Events_list
    try:
        events = pd.read_sql(sql_events, conn)
        events['event_date'] = pd.to_datetime(events['event_date'], errors='coerce')
        events['event_date'] = events['event_date'].dt.tz_localize(None)
        events['authorization_date'] = events['authorization_date'].dt.tz_localize(None)
        events['validation_date'] = events['validation_date'].dt.tz_localize(None)
    except:
        print("events execution error")
    try:
        last_pm = pd.read_sql(sql_last_pm, conn)
        last_pm['event_date'] = pd.to_datetime(last_pm['event_date'], errors='coerce')
    except:
        print("last_pm execution error")
    try:
        lost_pm_data = pd.read_sql(sql_check_pm_list, conn)
        print("WARNING: These vehicles doesn't have PM mileage data:")
        print(lost_pm_data)
    except:
        print("lost_pm_data execution error")

    try:
        pm_period = pd.read_sql(sql_pm_period, conn)

    except:
        print("pm_period execution error")


    try:
        zero_pm = pd.read_sql(sql_zero_pm, conn)
    except:
        print("zero pm data execution error")

    return vehicles, events, last_pm, pm_period, zero_pm

def handle_the_report(vehicles, pm_periodical, events, last_pm, pm, zen, zero_pm):

    vehicles = vehicles.merge(pm_periodical.rename({'name_id': 'name_id'}, axis=1), left_on='name_id',
                              right_on='name_id', how='left')

    last_pm_op = last_pm[['vehicle_id', 'op_name']]
    last_pm_dt = last_pm[['vehicle_id', 'event_date']]
    vehicles = vehicles.merge(last_pm_op.rename({'vehicle_id': 'vehicle_id'}, axis=1), left_on='vehicle_id',
                              right_on='vehicle_id', how='left')

    vehicles = vehicles.merge(last_pm_dt.rename({'vehicle_id': 'vehicle_id'}, axis=1), left_on='vehicle_id',
                              right_on='vehicle_id', how='left')
    for i in pm.itertuples():
        g = i[2]
        f = i[1]
        df = events.query('job_code == @f')
        df2 = df[['brand_name', 'model_name', 'state_number', 'vin', 'vehicle_id']]
        df2 = df2.T
        df3 = pd.DataFrame(df['vehicle_id'])
        df3[g] = g

        vehicles = vehicles.merge(df3.rename({'vehicle_id': 'vehicle_id'}, axis=1), left_on='vehicle_id',
                                  right_on='vehicle_id', how='left')

    vehicles = vehicles.drop_duplicates(keep='first')

    pattern = "\w+\w+\w+\w+\w+\w+\w+\w+\w+\w+\w+\w+[0-9][0-9][0-9][0-9][0-9]"
    filter = vehicles['vin'].str.contains(pattern)
    report = vehicles[filter]
    try:
        report = report.merge(zen.rename({'Гос номер ТС': 'state_number'}, axis=1), left_on='state_number',
                              right_on='state_number', how='left')
        report.rename(columns={'Статус': 'Статус из Зендеск'}, inplace=True)
    except:
        report.rename(columns={'Статус': 'Статус из Зендеск'}, inplace=True)

    report = report.merge(zero_pm, how='left', left_on='name_id', right_on='name_id')
    report = report.rename(columns={'mileage': 'zero_pm_mileage', 'month':'zero_pm_month'})

    report['Комментарий'] = ""
    report['Mileage_telematics'] = ""
    report['Должны были сделать ТО по пробегу'] = ""
    report['Должны были сделать ТО по году'] = ""
    report['Сделано ТО'] = ""
    report['Проверка по пограничным пробегам'] = ""
    report['Проверка по количеству ТО'] = ""

    report = report[
        ['name_id', 'brand_name', 'model_name', 'complectation_name', 'engine_capacity', 'transmission_type',
         'year_of_production', 'vin', 'Комментарий', 'contract_status', 'state_number', 'registration_of_the_vehicle',
         'created_at', 'last_maintenance_date', 'last_registered_mileage', 'last_registered_mileage_date', 'telematics',
         'pm_period', 'Mileage_telematics', 'Статус из Зендеск', 'Должны были сделать ТО по пробегу',
         'Должны были сделать ТО по году', 'Сделано ТО', 'Проверка по пограничным пробегам',
         'Проверка по количеству ТО', 'op_name', 'event_date', 'ТО-5000', 'ТО-10000', 'ТО-15000', 'ТО-20000',
         'ТО-25000', 'ТО-30000', 'ТО-35000', 'ТО-40000', 'ТО-45000', 'ТО-50000', 'ТО-55000', 'ТО-60000', 'ТО-65000',
         'ТО-70000', 'ТО-75000', 'ТО-80000', 'ТО-85000', 'ТО-90000', 'ТО-95000', 'ТО-100000', 'ТО-105000', 'ТО-110000',
         'ТО-115000', 'ТО-120000', 'ТО-125000', 'ТО-130000', 'ТО-135000', 'ТО-140000', 'ТО-145000', 'ТО-150000',
         'ТО-155000', 'ТО-160000', 'ТО-165000', 'ТО-170000', 'ТО-175000', 'ТО-180000', 'ТО-185000', 'ТО-190000',
         'ТО-195000', 'ТО-200000', 'ТО-182000', 'zero_pm_mileage', 'zero_pm_month']]

    return report

def get_pm(sm_dt):
    pm = pd.DataFrame(sm_dt)
    pm.astype({'job_code': str})
    return pm

def save_report(report, savedir):
    namechunk = "PM_report_"
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y")
    file = namechunk + dt_string + '.xlsx'
    filename = os.path.join(savedir,file)
    #print(filename)
    report.to_excel(filename, index=False)
    print("Report successfully saved here: " + str(savedir))
    return filename

def add_formulas_to_report(reportpath):
    dt = datetime.now().strftime('%d%m%Y')

    dic = {
        'U': '=IFERROR(ROUNDDOWN(S{0}/R{0},0),"-")',
        'V': '=IF(BR{0}="",ROUNDDOWN((TODAY()-M{0})/365,0),IF((TODAY()-M{0})-(30*BR{0})>1,ROUNDDOWN((TODAY()-M{0})/365,0)+1,ROUNDDOWN((TODAY()-M{0})/365,0)))',
        'W': '=COUNTA(AB{0}:BP{0})',
        'X': '=IF(S{0}="-","нет данных по пробегу с телематики",IF(BQ{0}="",IF(S{0}/R{0}<0.9,"рано для 1го ТО",IF(OR(MOD(S{0}/R{0},1)>0.933,MOD(S{0}/R{0},1)<0.05),"надо делать ТО","OK")),IF(AND(S{0}/BQ{0}>0.7,S{0}/BQ{0}<1),"Пора планировать ТО 0",IF(S{0}/R{0}<0.9,"рано для 1го ТО",IF(OR(MOD(S{0}/R{0},1)>0.933,MOD(S{0}/R{0},1)<0.05),"надо делать ТО","OK")))))',
        'Y': '=IF(U{0}="-", "нет данных по пробегу телематики",IF(OR(U{0}>W{0},V{0}>W{0}),"Пропущено ТО", "ТО сделаны"))',
    }

    wb = openpyxl.load_workbook(reportpath)
    ws = wb.active

    for item in dic.items():
        row = 1
        for cell in ws[item[0]]:
            if int(cell.coordinate.split(item[0])[1]) > 1:
                cell.value = item[1].format(row)
            else:
                pass
            row += 1
    ws.title = dt
    wb.save(reportpath)
    print('formulas has been added')
    return wb

def create_folder(directiry):
    folder_name = datetime.now().strftime('%d%m%Y')
    folder_path = os.path.join(directiry, folder_name)
    os.mkdir(folder_path)
    zendesk_folder = os.path.join(folder_path, 'zendesk') # due to zendesk end of operation going to be empty
    os.mkdir(zendesk_folder)
    print('new folder has been created')
    return folder_path, zendesk_folder