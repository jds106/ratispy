import pandas as pd
import numpy as np
import os

pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 500)
pd.set_option('display.precision', 4)
pd.set_option('display.float_format', '{:,.2f}'.format)

dir = '../data'

for file in os.listdir(dir):
    if not file.endswith('.csv'):
        print('Ignoring file {0}'.format(file))
        continue

    filename = '{0}/{1}'.format(dir, file)
    df = pd.read_csv(filename, index_col=0)

    s1 = df #df[(df.Secs == df.iloc[0].Secs)].copy()

    mid = pd.DataFrame(columns=['Mid'], data=(s1.L1P + s1.B1P) / 2)
    mid['Secs'] = s1.Secs
    mid['Runner'] = s1.Runner

    mid['ImpliedMid'] = 1 / mid.Mid

    mid['WeightedLay'] = (s1.L1P * s1.L1S + s1.L2P * s1.L2S + s1.L3P * s1.L3S) / (s1.L1S + s1.L2S + s1.L3S)
    mid['WeightedBack'] = (s1.B1P * s1.B1S + s1.B2P * s1.B2S + s1.B3P * s1.B3S) / (s1.B1S + s1.B2S + s1.B3S)
    mid['ImpliedWeightedLay'] = 1 / mid.WeightedLay
    mid['ImpliedWeightedBack'] = 1 / mid.WeightedBack

    mid['L1P'] = s1.L1P
    mid['B1P'] = s1.B1P
    mid['TrueMid'] = (mid.WeightedLay + mid.WeightedBack) / 2

    # If Lay - WeightLay > WeightedBack - Back then more volume on
    mid['Prediction'] = np.where(mid.TrueMid == mid.Mid , 'Unknown', np.where(mid.TrueMid < mid.Mid, 'Down', 'Up'))

    print(mid[(mid.Runner == 9434506)][['Runner','Secs', 'Mid', 'TrueMid','L1P','B1P','Prediction']])
    break