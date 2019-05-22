### Survey data only
import json
import random
import sys
from threading import Thread
import time
import os
import requests as rq
from bs4 import BeautifulSoup as bs

#%%
class QuickStatsDataGetter:

    def __init__(self, statisticcat_desc, sector_desc, agg_level_desc, source_desc):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0'}
        self.apikey = '080E301A-686A-3EC7-A498-6052AE2B7EC6'

        self.statisticcat_desc = statisticcat_desc
        self.sector_desc = sector_desc
        self.agg_level_desc = agg_level_desc
        self.source_desc = source_desc

    def get_counts(self):
        url = 'http://quickstats.nass.usda.gov/api/get_counts/?key=%s&commodity_desc=CORN&year__GE=2012&state_alpha=VA' % self.apikey
        resp = rq.get(url, headers=self.headers).json()
        print(self.sector_desc)
        return resp['count']

    def get_params(self, param, other):
        url = 'http://quickstats.nass.usda.gov/api/get_param_values/?key=%s&sector_desc=%s&param=%s&%s' % (self.apikey, self.sector_desc, param, other)

        resp = rq.get(url, headers=self.headers).json()

        return list(resp.values())[0]

    def get_items(self, group_desc, state):
        url = 'http://quickstats.nass.usda.gov/api/api_GET/?key=%s&statisticcat_desc=%s&sector_desc=%s&agg_level_desc=%s&group_desc=%s&state_fips_code=%s&source_desc=%s' \
              % (self.apikey, self.statisticcat_desc, self.sector_desc, self.agg_level_desc, group_desc, state, self.source_desc)
        resp = rq.get(url, headers=self.headers)
        return resp


statisticcat_desc = 'PRODUCTION'
sector_desc = 'CROPS'
agg_level_desc = 'COUNTY'
source_desc = 'SURVEY'
QSDG = QuickStatsDataGetter(statisticcat_desc, sector_desc, agg_level_desc, source_desc)

group_desc_ls = QSDG.get_params('group_desc', '')[1:]
group_desc_ls = [(i, el) for i, el in enumerate(group_desc_ls)]
state_ls = QSDG.get_params('state_fips_code', '')[:-2][1:]

path = 'G:/PRODUCTION'
u = 1

if __name__ == '__main__':
    for group_desc in group_desc_ls:
        for state in state_ls:
            if not os.path.isfile(path + '/%s_%s.json' % (group_desc[0], state)):
                print('%s/200' % u)
                res = QSDG.get_items(group_desc[1], state).json()
                with open(path + '/%s_%s.json' % (group_desc[0], state), 'w') as fp:
                    json.dump(res, fp)
                fp.close()
            u += 1
