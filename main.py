import pandas as pd
import pickle
import os
import argparse
from itertools import compress
from copy import deepcopy
from class_definition import order_class, LOB_class

# Debug Parameter
parser = argparse.ArgumentParser()
parser.add_argument('-a', '--allowable', type=int, help='Batch size')
args = parser.parse_args()

allowable = 20 if args.allowable is None else args.allowable

# read data and program initialization
dir_connector = '\\' if 'nt' in os.name else '/'

for folder in ['Input', 'Output', 'Debug']:
    if not os.path.isdir(folder):
        os.mkdir(folder)

lsdir = os.listdir('Input')
csv_file = list(compress(lsdir, ['csv' in files for files in lsdir]))
csv_file = ['Input' + dir_connector + csv for csv in csv_file]

for csv in csv_file:
    data = pd.read_csv(csv)
    # WARNING: removing completely duplicated rows
    data = data[~data.duplicated()]

    data.timestamp = pd.to_datetime(data.timestamp, format='%Y-%m-%dD%H:%M:%S.%f')

    # Create list of 5 minute snapshot points
    trade_date = data.date[0]
    print('===============================================')
    print('Working on trade date %s' % trade_date)
    print('===============================================')
    contracts = pd.unique(data.series)
    five_min_list_day = [pd.to_datetime(trade_date + 'T07:25:10',
                                        format='%Y-%m-%dT%H:%M:%S') +
                         pd.Timedelta(minutes=m) for m in range(0, 755, 5)]
    five_min_list_night = [pd.to_datetime(trade_date + 'T20:15:10',
                                          format='%Y-%m-%dT%H:%M:%S') +
                           pd.Timedelta(minutes=m) for m in range(0, 540, 5)]

    last_trading = False  # TBD: Check last trading day
    five_min_list = five_min_list_day + (five_min_list_night if not last_trading else [])

    # Initialize main result table
    five_min_depth = pd.DataFrame(columns=['date', 'contract', 'time',
                                           'bb', 'bo',
                                           'b5', 'b4', 'b3', 'b2', 'b1',
                                           'o1', 'o2', 'o3', 'o4', 'o5'])

    midpoint = pd.DataFrame(columns=['date', 'contract',
                                     'seq_no', 'time', 'trade_price',
                                     'pre_mid', 'post_mid', 'initiated'])

    for contract in contracts:  # Potential breakpoint for parallel
        date_n_contract = [trade_date, contract]
        print('Working on contract: ' + contract + '...')
        # Initialization
        sample = data[data.series == contract]

        # To generate 5 Minute depth
        LOB = LOB_class()
        this_list = deepcopy(five_min_list)
        five_min = this_list.pop(0)

        # To generate trade pre and post midpoint
        post_trade_time_list = []
        post_trade_time = None

        # Create a holder for marketable limit order
        holder = []
        mktable = None

        # Locate the error record:
        try:
            for line, row in sample.iterrows():

                mktable_adj_remove = (row.change_reason == 8 and
                                      row.ob_command == 1 and
                                      False if mktable is None else
                                      row.order_number == mktable[1].oid)

                if mktable_adj_remove:
                    if mktable[3] == 0:
                        LOB.add_order(mktable[1], mktable[2])
                    elif mktable[3] == 2:
                        LOB.change_order(mktable[1], mktable[2])

                    if len(holder):
                        mktable = holder.pop(0)
                    else:
                        mktable = None

                if mktable is not None and row.timestamp != mktable[0]:
                    while mktable is not None:
                        if mktable[3] == 0:
                            LOB.add_order(mktable[1], mktable[2])
                        elif mktable[3] == 2:
                            LOB.change_order(mktable[1], mktable[2])

                        if len(holder):
                            mktable = holder.pop(0)
                        else:
                            mktable = None

                if row.change_reason == 3 and row.ob_command != 0:
                    BBO = LOB.bbo()
                    post_trade_time_list.append([row.timestamp + pd.Timedelta(minutes=5),
                                                 row.sequence_number])
                    if post_trade_time is None:
                        post_trade_time = post_trade_time_list.pop(0)
                    ind = len(midpoint) + 1
                    midpoint.loc[ind] = date_n_contract + [row.sequence_number,
                                                           row.timestamp,
                                                           row.price,
                                                           sum(BBO) / 2.,
                                                           0.,
                                                           2. * row.bid_or_ask - 3.]

                while five_min is not None and row.timestamp > five_min:
                    BBO = LOB.bbo()
                    best_five = LOB.best_five()
                    ind = len(five_min_depth.index) + 1
                    five_min_depth.loc[ind] = date_n_contract + [five_min] + BBO + best_five
                    if len(this_list):
                        five_min = this_list.pop(0)
                    else:
                        five_min = None

                while post_trade_time is not None and row.timestamp > post_trade_time[0]:
                    BBO = LOB.bbo()
                    midpoint.post_mid[midpoint.seq_no == post_trade_time[1]] = sum(BBO) / 2.
                    if len(post_trade_time_list):
                        post_trade_time = post_trade_time_list.pop(0)
                    else:
                        post_trade_time = None

                marketable = (row.ob_command == 0 or row.ob_command == 2) and \
                        ((LOB.bbo()[1] > 0 and row.price >= LOB.bbo()[1] and
                          row.bid_or_ask == 1) or
                         (row.price <= LOB.bbo()[0] and row.bid_or_ask == 2))

                if marketable:
                    holder.append([row.timestamp,
                                   order_class(row.order_number,
                                               row.price,
                                               row.mp_quantity),
                                   row.bid_or_ask,
                                   row.ob_command])
                    if mktable is None:
                        mktable = holder.pop(0)

                else:
                    if row.ob_command == 0:
                        LOB.add_order(order_class(row.order_number,
                                                  row.price,
                                                  row.mp_quantity),
                                      row.bid_or_ask)

                    elif row.ob_command == 1:
                        LOB.delete_order(row.order_number, row.bid_or_ask)

                    elif row.ob_command == 2:  # Change an existing order
                        if row.change_reason == 3:  # execute mktable limit remaining
                            LOB.traded_order(row.order_number,
                                             row.bid_or_ask,
                                             row.quantity_difference)
                        else:
                            LOB.change_order(order_class(row.order_number,
                                                         row.price,
                                                         row.mp_quantity),
                                             row.bid_or_ask)

                with open('Debug/%s_%s.pkl' % (contract, row.sequence_number), 'wb') as output:
                    pickle.dump(LOB, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(this_list, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(five_min, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(post_trade_time_list, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(post_trade_time, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(holder, output, pickle.HIGHEST_PROTOCOL)
                    pickle.dump(mktable, output, pickle.HIGHEST_PROTOCOL)

                if row.sequence_number > allowable:
                    os.remove('Debug/%s_%s.pkl' % (contract, row.sequence_number - allowable))

        except ValueError:
            error_no = row.sequence_number
            upb = max((error_no - allowable), 0)
            print('Error found at seq_no: %d at file line %d' % (error_no, line))
            print('Now going back to file line %d' % (upb))

            with open('Debug/%s_%s.pkl' % (contract, upb), 'rb') as myinput:
                LOB = pickle.load(myinput)
                this_list = pickle.load(myinput)
                five_min = pickle.load(myinput)
                post_trade_time_list = pickle.load(myinput)
                post_trade_time = pickle.load(myinput)
                holder = pickle.load(myinput)
                mktable = pickle.load(myinput)

            print(LOB)

            row_iter = sample.iterrows()
            row_ind = 0
            while row_ind < upb:
                _, row = next(row_iter)
                row_ind = row.sequence_number

            print('Jumped to error location: %d' % row.sequence_number)

            while input('process next row? ') == 'y':
                _, row = next(row_iter)
                print(row.sequence_number)

                mktable_adj_remove = (row.change_reason == 8 and
                                      row.ob_command == 1 and
                                      False if mktable is None else
                                      row.order_number == mktable[1].oid)

                if mktable_adj_remove:
                    if mktable[3] == 0:
                        LOB.add_order(mktable[1], mktable[2])
                    elif mktable[3] == 2:
                        LOB.change_order(mktable[1], mktable[2])

                    if len(holder):
                        mktable = holder.pop(0)
                    else:
                        mktable = None

                if mktable is not None and row.timestamp != mktable[0]:
                    while mktable is not None:
                        if mktable[3] == 0:
                            LOB.add_order(mktable[1], mktable[2])
                        elif mktable[3] == 2:
                            LOB.change_order(mktable[1], mktable[2])

                        if len(holder):
                            mktable = holder.pop(0)
                        else:
                            mktable = None

                if row.change_reason == 3 and row.ob_command != 0:
                    BBO = LOB.bbo()
                    post_trade_time_list.append([row.timestamp + pd.Timedelta(minutes=5),
                                                 row.sequence_number])
                    if post_trade_time is None:
                        post_trade_time = post_trade_time_list.pop(0)
                    ind = len(midpoint) + 1
                    midpoint.loc[ind] = date_n_contract + [row.sequence_number,
                                                           row.timestamp,
                                                           row.price,
                                                           sum(BBO) / 2.,
                                                           0.,
                                                           2. * row.bid_or_ask - 3.]

                while five_min is not None and row.timestamp > five_min:
                    BBO = LOB.bbo()
                    best_five = LOB.best_five()
                    ind = len(five_min_depth.index) + 1
                    five_min_depth.loc[ind] = date_n_contract + [five_min] + BBO + best_five
                    if len(this_list):
                        five_min = this_list.pop(0)
                    else:
                        five_min = None

                while post_trade_time is not None and row.timestamp > post_trade_time[0]:
                    BBO = LOB.bbo()
                    midpoint.post_mid[midpoint.seq_no == post_trade_time[1]] = sum(BBO) / 2.
                    if len(post_trade_time_list):
                        post_trade_time = post_trade_time_list.pop(0)
                    else:
                        post_trade_time = None

                marketable = (row.ob_command == 0 or row.ob_command == 2) and \
                        ((LOB.bbo()[1] > 0 and row.price >= LOB.bbo()[1] and
                          row.bid_or_ask == 1) or
                         (row.price <= LOB.bbo()[0] and row.bid_or_ask == 2))

                if marketable:
                    holder.append([row.timestamp,
                                   order_class(row.order_number,
                                               row.price,
                                               row.mp_quantity),
                                   row.bid_or_ask,
                                   row.ob_command])
                    if mktable is None:
                        mktable = holder.pop(0)

                else:
                    if row.ob_command == 0:
                        LOB.add_order(order_class(row.order_number,
                                                  row.price,
                                                  row.mp_quantity),
                                      row.bid_or_ask)

                    elif row.ob_command == 1:
                        LOB.delete_order(row.order_number, row.bid_or_ask)

                    elif row.ob_command == 2:  # Change an existing order
                        if row.change_reason == 3:  # execute mktable limit remaining
                            LOB.traded_order(row.order_number,
                                             row.bid_or_ask,
                                             row.quantity_difference)
                        else:
                            LOB.change_order(order_class(row.order_number,
                                                         row.price,
                                                         row.mp_quantity),
                                             row.bid_or_ask)

                print(LOB)
                # print(five_min_list)
                # print(post_trade_time)
                # print(post_trade_time_list)

        # Calculate trade midpoint after the last record
        while five_min is not None:
            BBO = LOB.bbo()
            best_five = LOB.best_five()
            ind = len(five_min_depth.index) + 1
            five_min_depth.loc[ind] = date_n_contract + [five_min] + BBO + best_five
            if len(this_list):
                five_min = this_list.pop(0)
            else:
                five_min = None

        # Calculate post_trade_midpoint after the last record
        while post_trade_time is not None:
            BBO = LOB.bbo()
            midpoint.post_mid[midpoint.seq_no == post_trade_time[1]] = sum(BBO) / 2.
            if len(post_trade_time_list):
                post_trade_time = post_trade_time_list.pop(0)
            else:
                post_trade_time = None

    # Calculate spreads in HJM 2011
    midpoint['espread'] = midpoint.initiated * (midpoint.trade_price -
                                                midpoint.pre_mid) / midpoint.pre_mid
    midpoint['rspread'] = midpoint.initiated * (midpoint.trade_price -
                                                midpoint.post_mid) / midpoint.pre_mid
    midpoint['adv_selection'] = midpoint.initiated * (midpoint.post_mid -
                                                      midpoint.pre_mid) / midpoint.pre_mid

    file_name = 'Output' + dir_connector + trade_date + '_'
    five_min_depth.to_csv(file_name + 'best_five.csv', index=False)
    midpoint.to_csv(file_name + 'trade_midpoint.csv', index=False)
