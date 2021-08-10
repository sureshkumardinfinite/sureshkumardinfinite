# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 17:25:35 2021

@author: Hp
"""
from datetime import datetime, timedelta
print(str(str(datetime.today().strftime('%Y%m%d'))+'_'+str(datetime.today().strftime('%H%M%S'))+'.txt').replace('.txt','.csv'))
print(str(datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')))
print(str(datetime.today().strftime('%d-%b-%Y')))
