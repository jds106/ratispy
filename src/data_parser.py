import pandas as pd
import json
import urllib.request
from dateutil.parser import parse
import datetime


def prices_to_data_frame(pxs: list) -> pd.DataFrame:
    rows = []
    for px in pxs:
        assert (isinstance(px, dict))
        price = px.get('price')
        size = px.get('size')
        rows.append({'Price': price, 'Size': size})
    return pd.DataFrame(data=rows, columns=['Price', 'Size']).set_index(keys=['Price'])


def exec_diffs(pre_df: pd.DataFrame, post_df: pd.DataFrame) -> pd.DataFrame:
    # The post DataFrame should be a superset of all prices of the pre DataFrame as they are cumulative
    merged = post_df.join(pre_df, how='left', lsuffix='_Post', rsuffix='_Pre')
    merged['Delta'] = merged.Size_Post - merged.Size_Pre
    # Ignore executions of less than £1 as these are just noise
    return merged[(merged.Delta > 1)]


def load_data(event_id: str, market_id: str) -> pd.DataFrame:
    url_market = "http://Jamess-iMac.home:8700/events/{0}/{1}/market".format(event_id, market_id)
    url_prices = "http://Jamess-iMac.home:8700/events/{0}/{1}/prices".format(event_id, market_id)

    with urllib.request.urlopen(url_market) as market_response:
        with urllib.request.urlopen(url_prices) as price_response:
            market = json.loads(market_response.read().decode("utf-8"))
            snapshots = json.loads(price_response.read().decode("utf-8"))

            assert (isinstance(market, dict))
            assert (isinstance(snapshots, list))

            start_time = parse(market.get('description').get('marketTime'), ignoretz=True)
            rows = []
            prev_snapshot_volume = {}

            for snapshot in snapshots:
                assert (isinstance(snapshot, dict))

                runners = snapshot.get('runners')
                assert (isinstance(runners, list))

                time = datetime.datetime.utcfromtimestamp(snapshot.get('ticks') / 1000)
                secs = (start_time - time).total_seconds()
                if secs > 600 or secs < 0:
                    continue

                for runner in runners:
                    assert (isinstance(runner, dict))
                    selection_id = runner.get('selectionId')

                    ex = runner.get('ex')
                    assert (isinstance(ex, dict))
                    available_to_lay = ex.get('availableToLay')
                    available_to_back = ex.get('availableToBack')
                    traded_volume = prices_to_data_frame(ex.get('tradedVolume'))    # Store the traded volume in a DF

                    assert (isinstance(available_to_lay, list))
                    assert (isinstance(available_to_back, list))

                    min_exec_px = None
                    min_exec_sz = None
                    max_exec_px = None
                    max_exec_sz = None

                    prev_traded_volume = prev_snapshot_volume.get(selection_id)
                    if prev_traded_volume is not None and not prev_traded_volume.empty:
                        diffs = exec_diffs(prev_traded_volume, traded_volume)
                        if not diffs.empty:
                            min_exec_px = diffs.index.values[0]
                            min_exec_sz = diffs.iloc[0].Delta
                            max_exec_px = diffs.index.values[-1]
                            max_exec_sz = diffs.iloc[-1].Delta

                    prev_snapshot_volume[selection_id] = traded_volume

                    row = {
                        'Market': snapshot.get('marketId'),
                        'Start': start_time,
                        'TotalMatched': snapshot.get('totalMatched'),
                        'Runner': selection_id,
                        'Time': time,
                        'Secs': secs,

                        'B3P': available_to_lay[2].get('price') if len(available_to_lay) > 2 else None,
                        'B3S': available_to_lay[2].get('size') if len(available_to_lay) > 2 else None,

                        'B2P': available_to_lay[1].get('price') if len(available_to_lay) > 1 else None,
                        'B2S': available_to_lay[1].get('size') if len(available_to_lay) > 1 else None,

                        'B1P': available_to_lay[0].get('price') if len(available_to_lay) > 0 else None,
                        'B1S': available_to_lay[0].get('size') if len(available_to_lay) > 0 else None,

                        'L1P': available_to_back[0].get('price') if len(available_to_back) > 0 else None,
                        'L1S': available_to_back[0].get('size') if len(available_to_back) > 0 else None,

                        'L2P': available_to_back[1].get('price') if len(available_to_back) > 1 else None,
                        'L2S': available_to_back[1].get('size') if len(available_to_back) > 1 else None,

                        'L3P': available_to_back[2].get('price') if len(available_to_back) > 2 else None,
                        'L3S': available_to_back[2].get('size') if len(available_to_back) > 2 else None,

                        'MinExecPx': min_exec_px,
                        'MinExecSz': min_exec_sz,
                        'MaxExecPx': max_exec_px,
                        'MaxExecSz': max_exec_sz,
                    }

                    rows.append(row)

            return pd.DataFrame(data=rows, columns=[
                'Market', 'Runner', 'Secs',
                'L3S', 'L3P', 'L2S', 'L2P', 'L1S', 'L1P',
                'B1P', 'B1S', 'B2P', 'B2S', 'B3P', 'B3S',
                'MinExecPx', 'MinExecSz', 'MaxExecPx', 'MaxExecSz'
            ])


def process(df: pd.DataFrame) -> pd.DataFrame:
    df['BackPerc'] = 1 / df.B1P
    df['LayPerc'] = 1 / df.L1P

    gb = df.groupby(['Secs']).agg({
        'BackPerc': {  # Work on this column
            'BackPercSum': 'sum'  # Apply this operation to the results
        },
        'LayPerc': {
            'LayPercSum': 'sum'
        }})

    # Drop the BackPerc and LayPerc column names
    gb.columns = gb.columns.droplevel(0)

    return df.join(gb, how='left', on='Secs', lsuffix='L', rsuffix='R')


def load_meta_data() -> list:
    url = "http://Jamess-iMac.home:8700/events"

    meta_data = []
    with urllib.request.urlopen(url) as response:
        events_by_date = json.loads(response.read().decode("utf-8"))
        assert(isinstance(events_by_date, dict))

        for date in events_by_date.keys():
            events_for_date = events_by_date[date]
            assert(isinstance(events_for_date, dict))

            event_ids = events_for_date.keys()
            for event_id in event_ids:
                markets = events_for_date[event_id]
                assert(isinstance(markets, list))
                for market in markets:
                    assert(isinstance(market, dict))
                    market_id = market.get('marketId')
                    meta_data.append((event_id, market_id))

    return meta_data
