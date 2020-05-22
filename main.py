import pandas as pd
import os
from itertools import compress
from copy import deepcopy
from class_definition import order_class, LOB_class

# read data and program initialization
dir_connector = '\\' if 'nt' in os.name else '/'

if not os.path.isdir('Output'):
    os.mkdir('Output')

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

        for _, row in sample.iterrows():
            ######################################################
            # The Logic of this for loop:
            # 1. Special process rule for change reason 8 marketable adjust
            # 2. Check if there are remained marketable limit yet to be recorded.
            #    this is due to the fact marketable come in first before trade
            #    is triggerred
            # 3. Record midpoints and BBO, depth first
            # 4. Test if the current order is a marketable limit
            # 5. (if yes) hold the record until all trade is complete
            # 5. (if no) update the LOB
            ######################################################

            # Special processing rule for change reason 8 marketable adjust
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

            # Added other marketable order (change) into LOB
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

            ######################################################
            # Record information
            ######################################################
            # Calculate pre_trade midpoint
            # NB: change reason 3 combine with ob command 0 is the remaining of
            # a large marketable limit order, it is not a trade itself
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
                # Q_jt = 2 * bidask_jt - 3:
                # Q_jt from HJM 2011 takes 1 for buyer initiated
                # trades, where offer (2) is taken, and takes -1 for
                # seller initiated trades, where bid (1) is taken.

            # Calculate trade midpoint
            while five_min is not None and row.timestamp > five_min:
                BBO = LOB.bbo()
                best_five = LOB.best_five()
                ind = len(five_min_depth.index) + 1
                five_min_depth.loc[ind] = date_n_contract + [five_min] + BBO + best_five
                if len(this_list):
                    five_min = this_list.pop(0)
                else:
                    five_min = None

            # Calculate post trade midpoint
            while post_trade_time is not None and row.timestamp > post_trade_time[0]:
                BBO = LOB.bbo()
                midpoint.post_mid[midpoint.seq_no == post_trade_time[1]] = sum(BBO) / 2.
                if len(post_trade_time_list):
                    post_trade_time = post_trade_time_list.pop(0)
                else:
                    post_trade_time = None

            ######################################################
            # Update LOB
            ######################################################
            # SGX marketable order appear before the counter-party order
            # We will put order (change) into LOB after execution of
            # counter-party order
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

            # Normal update process
            else:
                # Add a new order or remain of marketable order
                if row.ob_command == 0:
                    LOB.add_order(order_class(row.order_number,
                                              row.price,
                                              row.mp_quantity),
                                  row.bid_or_ask)

                # Delete order
                elif row.ob_command == 1:
                    LOB.delete_order(row.order_number, row.bid_or_ask)

                # Change existing order
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
