import pandas as pd
from copy import deepcopy
from class_definition import order_class, LOB_class

# read data
data = pd.read_csv('METAF_Futures_2019.01.02.csv')  # TBD: read use argument
data.timestamp = pd.to_datetime(data.timestamp, format='%Y-%m-%dD%H:%M:%S.%f')

# Create list of 5 minute snapshot points
trade_date = data.date[0]
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
    holder = None

    ######################################################
    # For Debug purpose
    # row_iter = sample.iterrows()
    # while input('process next row? ') == 'y':
    #     _, row = next(row_iter)
    ######################################################
    for i, row in sample.iterrows():
        ######################################################
        # The Logic of this for loop:
        # 1. check if there are remained marketable limit yet to be recorded.
        #    this is due to the fact marketable come in first and then trade 
        #    it triggers
        # 2. Record midpoints and BBO, depth first
        # 3. Test if the current order is a marketable limit
        # 4. (if yes) hold the record until all trade is complete
        # 4. (if no) update the LOB
        ######################################################
        # Check if there are marketable limit yet to be recorded in LOB
        if holder is not None and row.timestamp != holder[0]:
            # print(row.sequence_number)
            if holder[3] == 0:
                LOB.add_order(holder[1], holder[2])
            elif holder[3] == 2:
                LOB.change_order(holder[1], holder[2])
            holder = None

        ######################################################
        # Record information
        ######################################################
        # Calculate pre_trade midpoint
        # NB: change reason 3 combine with ob command 0 is the remaining of
        # a large marketable limit order, it is not a trade itself
        if row.change_reason == 3 and row.ob_command != 0:
            BBO = LOB.bbo()
            # print(contract, row.sequence_number, BBO)
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

        # Calculate post_trade_midpoint
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
        marketable = (row.ob_command == 0 or row.ob_command == 2) and \
                ((LOB.bbo()[1] > 0 and row.price >= LOB.bbo()[1] and
                  row.bid_or_ask == 1) or
                 (row.price <= LOB.bbo()[0] and row.bid_or_ask == 2))

        # If marketable, hold until all immediate transaction is complete
        if marketable:
            # print(contract, row.sequence_number)
            holder = [row.timestamp,
                      order_class(row.order_number,
                                  row.price,
                                  row.mp_quantity),
                      row.bid_or_ask,
                      row.ob_command]

        # Normal update process
        else:
            # Create a new order or remain of mktble order
            if row.ob_command == 0:
                    LOB.add_order(order_class(row.order_number,
                                              row.price,
                                              row.mp_quantity),
                                  row.bid_or_ask)

            # Delete an order regardless of reason
            elif row.ob_command == 1:
                LOB.delete_order(row.order_number, row.bid_or_ask)

            # Change an existing order
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

        ######################################################
        # Debug printing
        ######################################################

        # print(LOB)
        # print(five_min)
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

file_name = trade_date + '_'
five_min_depth.to_csv(file_name + 'best_five.csv', index=False)
midpoint.to_csv(file_name + 'trade_midpoint.csv', index=False)
