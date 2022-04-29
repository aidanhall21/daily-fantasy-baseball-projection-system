#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
pd.set_option('display.max_columns', None)


# In[2]:


#get box score for hitters and pitchers for specified date

Date='2020-09-10'


# In[3]:


get_ipython().run_line_magic('env', "Date='2020-09-10'")

get_ipython().system('python BattersBox.py --date $Date')

get_ipython().system('python StartingPitcherRoundup.py --date $Date')


# In[4]:


dataset_names = ['BattersBox','SPRoundup']

for i in dataset_names:
    dirName = i
    try:
        # Create Directory
        os.mkdir(dirName)
        print("Directory " , dirName ,  " Created ") 
    except FileExistsError:
        print("Directory " , dirName ,  " already exists")


# In[5]:


for i in dataset_names:
    file_name = i
    try:
        file = f'{file_name}{Date}.xls'
        path = f'./{file_name}/{file_name}{Date}.xls'
        os.rename(file, path)
        print(file_name, 'Moved')
    except:
        print(file_name , 'File Already Moved')


# In[6]:


for i in dataset_names:
    if i == 'BattersBox':
        batters = pd.read_excel(f'./{i}/{i}{Date}.xls',skiprows=[0])
    if i == 'SPRoundup':
        pitchers = pd.read_excel(f'./{i}/{i}{Date}.xls',skiprows=[0])


# In[7]:


batters


# In[8]:


pitchers

