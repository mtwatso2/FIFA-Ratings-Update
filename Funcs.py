# -*- coding: utf-8 -*-
"""
@author: MWatson717
"""

import pandas as pd

def load_data(file):
    '''This function loads the data and does some preliminary cleaning'''
    f = pd.read_csv(file) #load the data
    f = f.drop('Rk', axis = 1) #Rank is not needed, so we can drop this
    f = f[f.Pos != 'GK'] #remove goalies from the data
    
    return f


def missing_columns(file):
    '''This function checks for columns with missing data'''
    
    nan = file.columns[file.isnull().any()].tolist() #check missing values
    
    if 'Pos' in nan:
        file = file[file['Pos'].notna()]      
    
    if 'Age' in nan:            #first dealing with missing player age/birth year
        file = file[file['Age'].notna()]
    
    x = file.isnull().sum()
    x = x[x>0]
    
    if len(x) > 0:
        f = file[[c for c in file if file[c].isnull().sum() < min(x)+1]]
        dif = len(file.columns) - len(f.columns)
        print("\n{} column(s) were dropped".format(dif))
             
        return f
    else:
        return file

def missing_rows(file):
    '''This function checks for rows with missing values'''
    null_data = file[file.isnull().any(axis=1)]
    if len(null_data) > 0:
        print("\nThese rows have missing values:")
        print(null_data)
        
        f = file.dropna()
            
        dif = len(file) - len(f)
        print("\n{} row(s) were removed".format(dif))
            
        return f
    else:
        return file

def dup_players(file):
    '''This function checks to see if there are duplicate rows for the same player(s)'''
    if len(file) > len(file.Player.value_counts()):  #Checking to see if some players are in the data more than once
        print("There are currently", len(file), "rows in the data.")
        print("There are", len(file.Player.value_counts()), "unique player names.\n")
        print("There are possibly duplicate rows for the same player who switched teams mid season:")
        
        vc = file.Player.value_counts()
        vc = vc[vc > 1]
        for i, v in vc.iteritems():
            print(i, v)  
            
        cols = file.columns.to_list() #List of all columns
        keep = ['Player', 'Age', 'Born']  #These variables will be used to aggregate rows

        agg_cols = [x for x in cols if x not in keep] #columns on which aggregation will occur
        agg_func = dict()
        first = ['Nation', 'Pos', 'Squad', 'Comp']
        for i in agg_cols:
            if i in first:
                agg_func[i] = 'first'
            else:
                agg_func[i] =  'sum'

        f = file.groupby(keep).aggregate(agg_func).reset_index()
        print((len(file) - len(f)), "rows were removed\n")
        
        vc = f.Player.value_counts()
        vc = vc[vc > 1]
        if len(vc) > 1:
            print("These rows are for players who have the same name: ")
            for i, v in vc.iteritems():
                print(i, v) 
    
    return f

def fix_vars(file):
    file['Player'] = file['Player'].str.split('\\').str[0] #split the name on '\', weird formatting from website data is from
    
    file['Comp'] = file['Comp'].apply(lambda x: x[x.find(' ') + 1:]) #fixing formating of League value
    
    positions = ['FWMF', 'FWDF', 'MFFW', 'MFDF', 'DFMF', 'DFFW']
    new = ['FW', 'FW', 'MF', 'MF', 'DF', 'DF']
    
    f = file.replace(positions, new)
    
    return f
    
def clean_data(file):
    '''This function calls previous functions to load and preprocess the various data files'''
    data = load_data(file)
    data = missing_columns(data)
    data = missing_rows(data)
    data = dup_players(data)
    data= fix_vars(data)
    
    return data

def clean_all(d1, d2, d3, d4, d5, d6):
    '''This function loads and cleans all of the 6 datasets'''
    d1 = clean_data(d1)
    d2 = clean_data(d2)
    d3 = clean_data(d3)
    d4 = clean_data(d4)
    d5 = clean_data(d5)
    d6 = clean_data(d6)
    
    return d1, d2, d3, d4, d5, d6


def merge(df1, df2):
    '''This function merges two datasets at a time by finding which columns that have that are equal'''
    lst1 = list(df1.columns)
    lst2 = list(df2.columns)
    lst1_set = set(lst1)
    same = list(lst1_set.intersection(lst2))
    
    data = pd.merge(left = df1, right = df2, how = 'inner', left_on = same, right_on = same)
    
    return data


def merge_all(d1, d2, d3, d4, d5, d6):
    ''' This function merges all of the data sets into one'''
    data = merge(d1, d2)
    data = merge(data, d3)
    data = merge(data, d4)
    data = merge(data, d5)
    data = merge(data, d6)
    
    data = data.dropna()
    
    return data

def season_data(d1, d2, d3, d4, d5, d6):
    '''This is the main function that contains all of the functions created above to load/preprocess the data'''
    d1, d2, d3, d4, d5, d6 = clean_all(d1, d2, d3, d4, d5, d6)
    data = merge_all(d1, d2, d3, d4, d5, d6)
    return data

def clean_fut(data):
    '''This function load and cleans the data for FIFA Ultimate Team Player Ratings'''
    fut = pd.read_csv(data)
    fut = fut[fut['revision'].isin(['Normal', 'Non-Rare', 'Rare'])] #Only want the base/original player ratings, which is 'Normal' or 'Non-rare'/'Rare'
    fut['year'] = pd.DatetimeIndex(fut['date_of_birth']).year #Creating a new variable of just Birth year to be used for merging with full dataset
    
    fut_pen = fut[['player_extended_name', 'overall', 'year']] #Two different name variables present in data, will try merging on both
    fut_pen = fut_pen.rename(columns={'player_extended_name':'Player', 'year':'Born'})
    
    fut_n = fut[['player_name', 'overall', 'year']]
    fut_n = fut_n.rename(columns={'player_name':'Player', 'year':'Born'})
    
    return fut_pen, fut_n

def add_fut(d1, d2, d3):
    '''This function adds the FIFA Ultimate Team ratings to the full data set'''
    data = pd.merge(left = d1, right = d2, how = 'inner', left_on = ['Player', 'Born'], right_on = ['Player', 'Born'])
    data2 = pd.merge(left = d1, right = d3, how = 'inner', left_on = ['Player', 'Born'], right_on = ['Player', 'Born'])
    
    data3 = pd.concat([data, data2]).drop_duplicates(subset = ['Player', 'Born']).reset_index(drop=True)
    
    return data3

def fut_data(d1, d2):
    '''This function returns the final dataset'''
    fut1, fut2 = clean_fut(d2)
    data = add_fut(d1, fut1, fut2)
    
    return data