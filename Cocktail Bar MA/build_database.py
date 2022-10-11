import requests
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

#Initialising Database
conn = sqlite3.connect('bar_data1.db')
c = conn.cursor()

# Reading csv files
bar_data = pd.read_csv("data/bar_data.csv")
ny_data = pd.read_csv("data/ny.csv")
bp_data = pd.read_csv("data/budapest.csv")
#ln_data = pd.read_csv("data/london_transactions.csv")

# seperating each bar dataset into different dataframes
bar_ny = bar_data[bar_data['bar']== 'new york'].reset_index().drop(['index'],1)
bar_bp = bar_data[bar_data['bar']== 'budapest'].reset_index().drop(['index'],1)
bar_ln = bar_data[bar_data['bar']== 'london'].reset_index().drop(['index'],1)

#Opening data_tables.sql to create tables
fd = open('data_tables.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
for command in sqlCommands:
    try:
        c.execute(command)
    except:
        print("Command skipped")

# getting data from the API and adding to existing dataframes
def get_data(data11):
    dict1 = {'cate' : [],'glass' : []}

    for i in list(data11.iloc[:,2].unique()):
        print(i)
        dict1['cate'].append(requests.get("https://www.thecocktaildb.com/api/json/v1/1/search.php?s="+i).json()['drinks'][0]['strCategory'])
        dict1['glass'].append(requests.get("https://www.thecocktaildb.com/api/json/v1/1/search.php?s="+i).json()['drinks'][0]['strGlass'])

    
    for i in range(len(list(data11.iloc[:,2]))):
        print(i)
        index = list(data11.iloc[:,2].unique()).index(list(data11.iloc[:,2])[i])
        cat = dict1['cate'][index]
        gl = dict1['glass'][index]
        data11.at[i,'category'] = cat
        data11.at[i,'glasstype'] = gl

    for i in range(len(data11)):
        data11.at[i,'date'] = data11['time'][i].split(' ')[0]
        data11.at[i,'hrs'] = int(data11['time'][i].split(' ')[1].split(':')[0])

    data11 = data11[['time','drink','amount','category','glasstype','date','hrs']]
    return data11


ny_data = get_data(ny_data)
bp_data = get_data(bp_data)
#ln_data = get_data(ln_data)

#Renaming the glass names
ny_data['glasstype'] = ny_data['glasstype'].str.lower().replace('margarita/coupette glass','margarita glass')
bp_data['glasstype'] = bp_data['glasstype'].str.lower().replace('margarita/coupette glass','margarita glass')
#ln_data['glasstype'] = ln_data['glasstype'].str.lower().replace('margarita/coupette glass','margarita glass')

# changing datatype of columns
bar_bp['stock'] = bar_bp['stock'].astype(str)
bar_ln['stock'] = bar_ln['stock'].astype(str)
bar_ny['stock'] = bar_ny['stock'].astype(str)

# adding data to the database
query=''' insert or replace into bar_ny (glass_type,stock,bar) values (?,?,?) '''
c.executemany(query, bar_ny.to_records(index=False))
conn.commit()

query=''' insert or replace into bar_bp (glass_type,stock,bar) values (?,?,?) '''
c.executemany(query, bar_bp.to_records(index=False))
conn.commit()

query=''' insert or replace into bar_ln (glass_type,stock,bar) values (?,?,?) '''
c.executemany(query, bar_ln.to_records(index=False))
conn.commit()


query=''' insert or replace into ny_data (timest,drink,amount,category,glasstype,date1,hrs) values (?,?,?,?,?,?,?) '''
c.executemany(query, ny_data.to_records(index=False))
conn.commit()

query=''' insert or replace into bp_data (timest,drink,amount,category,glasstype,date1,hrs) values (?,?,?,?,?,?,?) '''
c.executemany(query, bp_data.to_records(index=False))
conn.commit()

# query=''' insert or replace into ln_data (timest,drink,amount,category,glasstype,date1,hrs) values (?,?,?,?,?,?,?) '''
# c.executemany(query, ln_data.to_records(index=False))
# conn.commit()


# get final results, by checking the current requirments
def get_fina_result(data,bar):
    date_list = list(data['date'].unique())
    glass_list = list(data['glasstype'].unique())
    hrs_list = list(data['hrs'].unique())
    
    datesss = []
    gltype = []
    time_hrs = []
    actual_glass_required = []
    glass_present = []
    
    for j in range(len(bar)):
        print(j)
        for i in date_list:
            for k in hrs_list:
                dff = data[(data['date'] == i) & (data['glasstype'] == bar.iloc[j,0]) & ((data['hrs'] == k))]
                #print(len(dff),bar_ny.iloc[j,1])
                if len(dff) > int(bar.iloc[j,1]):
                    datesss.append(i)
                    gltype.append(bar.iloc[j,0])
                    time_hrs.append(k)
                    actual_glass_required.append(len(dff))
                    glass_present.append(int(bar.iloc[j,1]))
                    #print(i,bar_ny.iloc[j,0],k,len(dff),int(bar_ny.iloc[j,1]))
    df = pd.DataFrame({'Date':datesss,'Hours':time_hrs,'Glass Type':gltype,'Actual Glass required':actual_glass_required,'Actual present in Bar':glass_present})
    return df


ny_results = get_fina_result(ny_data,bar_ny)
bp_results = get_fina_result(bp_data,bar_bp)
#ln_results = get_fina_result(ln_data,bar_ln)

#running the poc_tables.
fd = open('poc_tables.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
for command in sqlCommands:
    try:
        c.execute(command)
    except:
        print("Command skipped")

# changeing datatype of columns
ny_results['Actual present in Bar'] = ny_results['Actual present in Bar'].astype(str)
ny_results['Actual Glass required'] = ny_results['Actual Glass required'].astype(str)
bp_results['Actual present in Bar'] = bp_results['Actual present in Bar'].astype(str)
bp_results['Actual Glass required'] = bp_results['Actual Glass required'].astype(str)

# ln_results['Actual present in Bar'] = ln_results['Actual present in Bar'].astype(str)
# ln_results['Actual Glass required'] = ln_results['Actual Glass required'].astype(str)


query=''' insert or replace into ny_results (Date1,Hours1,Glass_Type,Actual_Glass_Required,Actual_Present_in_Bar) values (?,?,?,?,?) '''
c.executemany(query, ny_results.to_records(index=False))
conn.commit()

query=''' insert or replace into bp_results (Date1,Hours1,Glass_Type,Actual_Glass_Required,Actual_Present_in_Bar) values (?,?,?,?,?) '''
c.executemany(query, bp_results.to_records(index=False))
conn.commit()

# query=''' insert or replace into ln_results (Date1,Hours1,Glass_Type,Actual_Glass_Required,Actual_Present_in_Bar) values (?,?,?,?,?) '''
# c.executemany(query, ln_results.to_records(index=False))
# conn.commit()

