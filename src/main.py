import pandas as pd
import src.data_parser as dp
import os.path as path

pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 500)
pd.set_option('display.precision', 4)
pd.set_option('display.float_format', '{:,.2f}'.format)

event_markets = dp.load_meta_data()

for event_market in event_markets:
    event_id, market_id = event_market

    filename = '../data/{0}.{1}.csv'.format(event_id, market_id)
    if path.isfile(filename):
        print('Skipping current file {0}'.format(filename))
        continue

    results = dp.load_data(event_id=event_id, market_id=market_id)
    processed_results = dp.process(results)
    processed_results.to_csv(filename)
    print('Written market {0}'.format(market_id))
