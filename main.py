import sys
import psycopg2
import pandas as pd
import time
import os

import dict
import func
import pathes
import sql
import users

#create folder
folder, zendesk_folder = func.create_folder(pathes.common_dir)

# connect to sql
conn = func.connect_to_psql(users.db_params)

# grab the data from sql
vehicles, events, last_pm, pm_periodical, zero_pm = func.get_data_from_db(conn, sql.sql_vehicles, sql.sql_events, sql.sql_last_pm, sql.sql_check_pm_list, sql.sql_pm_period, sql.sql_zero_pm)

# read zendesk file
zen = func.zendesk(zendesk_folder)

# creating PM table
pm = func.get_pm(dict.sm_dt)

# creating report
report = func.handle_the_report(vehicles, pm_periodical, events, last_pm, pm, zen, zero_pm)

#save the report
report_path = func.save_report(report, folder)

#adding_formulas
func.add_formulas_to_report(report_path)

