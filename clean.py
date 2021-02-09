# -*- coding: utf-8 -*-
"""
@author: MWatson717
"""

#This script loads all of the data from the 17-18, 18-19 and 19-20 seasons for the 'Big Five' Leagues (England, France, Spain, Italy and Germany)
#And then adds the FIFA ratings for each player (Fifa 19 for 2017-18 season, Fifa 20 for 2018-19 season, etc) and lastly combines everything

import Funcs as f
import pandas as pd

data1718 = f.season_data("standard17-18.csv", "shooting17-18.csv", "passing17-18.csv",
                         "defense17-18.csv", "possession17-18.csv", "miscellaneous17-18.csv") #2275, 81

data1718['Game'] = 'FIFA 19'

data1819 = f.season_data("standard18-19.csv", "shooting18-19.csv", "passing18-19.csv",
                         "defense18-19.csv", "possession18-19.csv", "miscellaneous18-19.csv") #2304, 88

data1819['Game'] = 'FIFA 20'

data1920 = f.season_data("standard19-20.csv", "shooting19-20.csv", "passing19-20.csv",
                         "defense19-20.csv", "possession19-20.csv", "miscellaneous19-20.csv") #2382, 88

data1920['Game'] = 'FIFA 21'


data1 = f.fut_data(data1718, "fut_bin19_players.csv") #1772, 82

data2 = f.fut_data(data1819, "fut_bin20_players.csv") #1693, 89

data3 = f.fut_data(data1920, "fut_bin21_players.csv") #1775, 89

data = pd.concat([data1, data2, data3], axis = 0).dropna(axis=1)

data.to_csv('fut_data_17_to_20.csv', index = False)