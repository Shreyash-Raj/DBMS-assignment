#!/usr/bin/env python
# coding: utf-8


# In[6]:

################ Input username and password for Mysql ###############
User = str(input("Enter username for Mysql: "))   
Password = str(input("Enter password for Mysql: "))
#####################################################################

########################################IMPORTS#########################################

import sqlite3
import pandas as pd
import time 
import mysql.connector as mariadb
import pymongo
from pymongo import MongoClient
import csv
from sqlalchemy import create_engine
from pathlib import Path
import pymysql
import matplotlib.pyplot as plt


########################################################################################
# In[6]:


################################# SQLITE3 ##############################################

#Creating the required databases and tables

# In[41]:

#Creating an sqlite3 database named "db_170682.db"
Path('db_170682.db').touch()
con = sqlite3.connect('db_170682.db')
c = con.cursor()


# In[43]:

# Suffixes of csv files

a_csv = ['100', '1000', '10000']
b_csv = {'100':[['3', '1'], ['5', '3'], ['10', '2']], '1000':[['5','3'], ['10', '4'], ['50', '1']], '10000':[['5','2'], ['50','1'], ['500','4']]}

# In[44]:

#Creating the required tables

for suff_a in a_csv:
    query = 'CREATE TABLE A_' + suff_a + ' (A1 int, A2 text)'
    c.execute(query)
    data = pd.read_csv('A-'+suff_a+'.csv')
    data.to_sql('A_'+suff_a, con, if_exists='replace', index = False)
    
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        query = 'CREATE TABLE B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' (B1 int, B2 int, B3 text)'
        c.execute(query)
        data = pd.read_csv('B-' + suff_a + '-' + suff_b[0] + '-' + suff_b[1] + '.csv')
        data.to_sql('B_'+suff_a + '_' + suff_b[0] + '_' + suff_b[1], con, if_exists='replace', index=False)


###### Query Processing #########

# In[84]:

sqlite_q1_time = {}  # Dictionary to hold time for 7 iterations for all 9 databases for first query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM A_' + suff_a + ' WHERE A1<=50'
            start = time.time()
            res = c.execute(query).fetchall()
            with open('res1', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        sqlite_q1_time[i] = tme
        i=i+1

print("sqlite query 1 is complete....")     
        
sqlite_q2_time = {}  # Dictionary to hold time for 7 iterations for all 9 databases for second query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' ORDER BY B3'
            start = time.time()
            res = c.execute(query).fetchall()
            with open('res2', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        sqlite_q2_time[i] = tme
        i=i+1

print("sqlite query 2 is complete....") 

sqlite_q3_time = {}  # Dictionary to hold time for 7 iterations for all 9 databases for third query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT AVG(COUNT) FROM (SELECT COUNT(*) as COUNT FROM B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' GROUP BY B2)'
            start = time.time()
            res = c.execute(query).fetchall()
            with open('res3', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        sqlite_q3_time[i] = tme
        i=i+1
        
print("sqlite query 3 is complete....") 

sqlite_q4_time = {}   # Dictionary to hold time for 7 iterations for all 9 databases for fourth query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT b.B1, b.B2, b.B3, a.A2 FROM A_' + suff_a + ' AS a INNER JOIN B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' AS b ON a.A1 = b.B2'
            start = time.time()
            res = c.execute(query).fetchall()
            with open('res4', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        sqlite_q4_time[i] = tme
        i=i+1     
        
print("sqlite query 4 is complete....") 

############################################################################################

# In[55]:

################################### MONGODB ################################################ 

######## Creating the required databases and tables ######


# Creating a MongoDb database named "db_170682"

client = MongoClient('localhost')
database = client['db_170682']

# In[183]:

# Creating the required tables

for suff_a in a_csv:
    df = pd.read_csv('A-'+ suff_a + '.csv') 
    data = df.to_dict('records')
    collection = database['A_' + suff_a]
    collection.insert_many(data)
    
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        df = pd.read_csv('B-'+ suff_a + '-' + suff_b[0] + '-' + suff_b[1] + '.csv') 
        data = df.to_dict('records')
        collection = database['B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1]]
        collection.insert_many(data)
    


# In[ ]:

########## Processing the Queries ############

mongo_q1_time = {}  # Dictionary to hold time for 7 iterations for all 9 databases for first query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        query = {"A1":{"$lte":50}}
        while itr:
            collection = database['A_' + suff_a]
            start = time.time()
            res = pd.DataFrame(collection.find(query))
            res.pop("_id")
            res.to_csv('res5', ",", index=False)
            tme.append(time.time()-start)
            itr = itr-1
        mongo_q1_time[i] = tme
        i=i+1

print("MongoDb query 1 is complete....")

mongo_q2_time = {}    # Dictionary to hold time for 7 iterations for all 9 databases for second query
i=0            
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        query = "B3"
        while itr:
            collection = database['B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1]]
            start = time.time()
            res = pd.DataFrame(collection.find().sort(query))
            res.pop("_id")
            res.to_csv('res6', ",", index=False)
            tme.append(time.time()-start)
            itr = itr-1
        mongo_q2_time[i] = tme
        i=i+1

print("MongoDb query 2 is complete....")

#In[]

mongo_q3_time = {}   # Dictionary to hold time for 7 iterations for all 9 databases for third query
i=0    
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:   
        itr = 7
        tme = []
        query = [{"$group":{"_id":"$B2", "count":{"$sum":1}}}, {"$group":{"_id":"null", "average":{"$avg":"$count"}}}]
        while itr:
            collection = database['B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1]]
            start = time.time()
            res = pd.DataFrame(collection.aggregate(query))
            res.pop("_id")
            res.to_csv('res7', ",", index=False)
            tme.append(time.time()-start)
            itr = itr-1
        mongo_q3_time[i]=tme
        i=i+1

print("MongoDb query 3 is complete....")     

#In[]
mongo_q4_time = {}   # Dictionary to hold time for 7 iterations for all 9 databases for fourth query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
            itr = 7
            tme = []
            query = [{"$lookup":{"from":"A_100", "localField":"B2", "foreignField":"A1", "as":"A"}}, {"$project":{"B1":"$B1", "B2":"$B2", "B3":"$B3", "A2":"$A.A2"}}]
            while itr:
                collection = database['B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1]]
                start = time.time()
                res = pd.DataFrame(collection.aggregate(query))
                res.pop("_id")
                res.to_csv('res8', ",", index=False)
                tme.append(time.time()-start)
                itr = itr-1
            mongo_q4_time[i]=tme
            i=i+1

print("MongoDb query 4 is complete....")

##################################################################################################################################################

############################# MariaDB ###########################################

#################### Creating databases and tables ################


# In[22]:

#Creating the MariaDb database

Host = "localhost"
db_name = "db_170682"

mydb = mariadb.connect(
  host = Host,
  user = User,
  password = Password
)
c = mydb.cursor()

c.execute("CREATE DATABASE " + db_name) 

conn_params_dic = {
    "host"      : Host,
    "database"  : db_name,
    "user"      : User,
    "password"  : Password
}

connect_alchemy = "mysql+pymysql://%s:%s@%s/%s" % (
    conn_params_dic['user'],
    conn_params_dic['password'],
    conn_params_dic['host'],
    conn_params_dic['database']
)

c = create_engine(connect_alchemy)

# In[25]:

# Creating the required tables

for suff_a in a_csv:
    query = 'CREATE TABLE A_' + suff_a + ' (A1 INTEGER, A2 VARCHAR(30), PRIMARY KEY (A1))'
    c.execute(query)
    data = pd.read_csv('A-'+suff_a+'.csv')
    data.to_sql('A_'+suff_a, con=c, if_exists='replace', index = False)
        
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        query = 'CREATE TABLE B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' (B1 INTEGER, B2 INTEGER, B3 VARCHAR(100), PRIMARY KEY (B1))'
        c.execute(query)
        data = pd.read_csv('B-' + suff_a + '-' + suff_b[0] + '-' + suff_b[1] + '.csv')
        data.to_sql('B_'+suff_a + '_' + suff_b[0] + '_' + suff_b[1], con=c, if_exists='replace', index=False)

############################################################################

#################### Query Processing #####################################

# In[ ]:


maria_q1_time = {}     # Dictionary to hold time for 7 iterations for all 9 databases for first query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM A_' + suff_a + ' WHERE A1<=50'
            start = time.time()
            res = c.execute(query)
            with open('res9', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        maria_q1_time[i] = tme
        i=i+1

print("MariaDb without index query 1 is complete....")

#In[]

maria_q2_time = {}    # Dictionary to hold time for 7 iterations for all 9 databases for second query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' ORDER BY B3'
            start = time.time()
            res = c.execute(query)
            with open('res10', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        maria_q2_time[i] = tme
        i=i+1

print("MariaDb without index query 2 is complete....")

#In[]

maria_q3_time = {}   # Dictionary to hold time for 7 iterations for all 9 databases for third query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT AVG(COUNT) FROM (SELECT COUNT(*) as COUNT FROM B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' GROUP BY B2) as t'
            start = time.time()
            res = c.execute(query)
            with open('res11', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        maria_q3_time[i] = tme
        i=i+1

print("MariaDb without index query 3 is complete....")

maria_q4_time = {}    # Dictionary to hold time for 7 iterations for all 9 databases for fourth query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT b.B1, b.B2, b.B3, a.A2 FROM A_' + suff_a + ' AS a INNER JOIN B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' AS b ON a.A1 = b.B2'
            start = time.time()
            res = c.execute(query)
            with open('res12', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        maria_q4_time[i] = tme
        i=i+1
      
print("MariaDb without index query 4 is complete....")

#In[]

#################################### Mariadb with indexing #######################################################

################### Creating indexes #################

for suff_a in a_csv:
    query = 'CREATE INDEX I_' + suff_a + ' ON A_' + suff_a + ' (A2(10))'
    c.execute(query)

        
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        query = 'CREATE INDEX I_2_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' ON B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + '(B2)'
        c.execute(query)
        query = 'CREATE INDEX I_3_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' ON B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' (B3(100))'
        c.execute(query)

######################################################

##################################### Query Processing ###########################################

# In[29]:


mariaIdx_q1_time = {}     # Dictionary to hold time for 7 iterations for all 9 databases for first query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM A_' + suff_a + ' WHERE A1<=50'
            start = time.time()
            res = c.execute(query)
            with open('res13', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        mariaIdx_q1_time[i] = tme
        i=i+1

print("MariaDb with index query 1 is complete....")

#In[]

mariaIdx_q2_time = {}      # Dictionary to hold time for 7 iterations for all 9 databases for second query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT * FROM B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' ORDER BY B3'
            start = time.time()
            res = c.execute(query)
            with open('res14', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        mariaIdx_q2_time[i] = tme
        i=i+1

print("MariaDb with index query 2 is complete....")

#In[]

mariaIdx_q3_time = {}      # Dictionary to hold time for 7 iterations for all 9 databases for third query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT AVG(COUNT) FROM (SELECT COUNT(*) as COUNT FROM B_'+ suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' GROUP BY B2) as t'
            start = time.time()
            res = c.execute(query)
            with open('res15', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        mariaIdx_q3_time[i] = tme
        i=i+1

print("MariaDb with index query 3 is complete....")

#In[]

mariaIdx_q4_time = {}      # Dictionary to hold time for 7 iterations for all 9 databases for fourth query
i=0
for suff_a in a_csv:
    for suff_b in b_csv[suff_a]:
        itr = 7
        tme = []
        while itr:
            query = 'SELECT b.B1, b.B2, b.B3, a.A2 FROM A_' + suff_a + ' AS a INNER JOIN B_' + suff_a + '_' + suff_b[0] + '_' + suff_b[1] + ' AS b ON a.A1 = b.B2'
            start = time.time()
            res = c.execute(query)
            with open('res16', 'w') as f:
                write = csv.writer(f)
                write.writerows(res)
            tme.append(time.time()-start)
            itr = itr-1
        mariaIdx_q4_time[i] = tme
        i=i+1

print("MariaDb with index query 4 is complete....")

####################################################################################################



############################################ RESULTS ##########################################################



###################################### Printing the table ###############################################


#In[]

############################## lists to produce the table ##############################
t = []  # TO append all the dictionaries containing execution time
t.append(sqlite_q1_time)
t.append(mongo_q1_time)
t.append(maria_q1_time)
t.append(mariaIdx_q1_time)
t.append(sqlite_q2_time)
t.append(mongo_q2_time)
t.append(maria_q2_time)
t.append(mariaIdx_q2_time)
t.append(sqlite_q3_time)
t.append(mongo_q3_time)
t.append(maria_q3_time)
t.append(mariaIdx_q3_time)
t.append(sqlite_q4_time)
t.append(mongo_q4_time)
t.append(maria_q4_time)
t.append(mariaIdx_q4_time)

db = ['sqlite_', 'mongo_', 'maria_', 'mariaIdx_']
qt = ['q1_time', 'q2_time', 'q3_time', 'q4_time']

#In[]


############# Prodcuing the table ################
#In[]
data = []

for dic in t:
    temp=[]
    for lst in dic.values():
        lst.sort()
        tmp = lst[1:-1]
        mean = sum(tmp)/len(tmp)
        temp.append(mean)
    data.append(temp)

cols=[]
index= []

for a in a_csv:
    for b in b_csv[a]:
        cols.append('(A_'+a+',B_'+a+'_'+b[0]+'_'+b[1]+')')


for q in qt:
    for d in db:
        index.append(d+q)
######################################################################################
#In[]

# df is the dataframe containing the required execution time for all queries and for all databases
df=pd.DataFrame(data) 
df.columns = cols
df_g = df.transpose()  # df_g is for producing graph
df.index = index

print(df)
df.to_csv('Execution_Time.csv')  # saving the Execution time table

###################################################################################################
#In[]

################################### Plotting the graph  #################################


for i in range(4):
    temp = df_g.iloc[:,4*i: 4*i+4]
    temp.columns = ['sqlite3', 'mongodb', 'maria', 'mariaIndex']
    temp.plot(
        kind='bar',
        stacked=False,
        title='Execution time for Query ' + str(i+1) + ' (in sec)')
    plt.tight_layout()
    plt.savefig('Query'+str(i+1)+'.jpeg')
    
#########################################################################################

# %%
