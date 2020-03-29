from copy import deepcopy


class order_class:
    def __init__(self, oid, price, quantity):
        self.oid = oid
        self.price = price
        self.quantity = quantity

    def __repr__(self):
        return('...%s: %d shares @ %d' % (self.oid[-4:],
                                          self.quantity,
                                          self.price))

    def update(self, price=None, delta_quantity=None):
        if price is not None:
            self.price = price
        if delta_quantity is not None:
            self.quantity = self.quantity + delta_quantity


class LOB_class:
    def __init__(self):
        self._bid_book = []
        self._offer_book = []

    def __repr__(self):
        bid = [''.join(['Price: %d, ' % tick[0].price] +
                       ['%s, ' % order.oid[-4:] for order in tick])
               for tick in self._bid_book]
        offer_book_copy = deepcopy(self._offer_book)
        offer_book_copy.reverse()
        offer = [''.join(['Price: %d' % tick[0].price] +
                         ['%s, ' % order.oid[-4:] for order in tick])
                 for tick in offer_book_copy]
        printer = '\n'.join(offer + ['\n'] + bid)
        return(printer)

    def __str__(self):
        bid = [''.join(['Price: %d, ' % tick[0].price] +
                       ['[...%s: %d], ' % (order.oid[-4:], order.quantity)
                        for order in tick])
               for tick in self._bid_book]
        offer_book_copy = deepcopy(self._offer_book)
        offer_book_copy.reverse()
        offer = [''.join(['Price: %d' % tick[0].price] +
                         ['[...%s: %d], ' % (order.oid[-4:], order.quantity)
                          for order in tick])
                 for tick in offer_book_copy]
        printer = '\n'.join(offer + ['\n'] + bid)
        return(printer)

    def _pop(self, oid, bidask):
        book = self._offer_book if (bidask - 1) else self._bid_book
        ind_tick = [oid in [order.oid for order in tick]
                    for tick in book].index(True)
        ind_priority = [order.oid for order in book[ind_tick]].index(oid)
        poped_order = book[ind_tick].pop(ind_priority)
        return(poped_order)

    def _delete(self, oid, bidask):
        book = self._offer_book if (bidask - 1) else self._bid_book
        ind_tick = [oid in [order.oid for order in tick]
                    for tick in book].index(True)
        ind_priority = [order.oid for order in book[ind_tick]].index(oid)
        del book[ind_tick][ind_priority]
        if len(book[ind_tick]) == 0:
            del book[ind_tick]

    def _insert(self, new_order, bidask):
        price = new_order.price
        book = self._offer_book if (bidask - 1) else self._bid_book
        ticks = [tick[0].price for tick in book]
        if price in ticks:
            ind = ticks.index(price)
            book[ind].append(new_order)
        else:
            ind = 0
            if bidask - 1:
                while ind < len(ticks) and price > ticks[ind]:
                    ind = ind + 1
            else:
                while ind < len(ticks) and price < ticks[ind]:
                    ind = ind + 1
            book.insert(ind, [new_order])

    def add_order(self, new_order, bidask):
        self._insert(new_order, bidask)

    def change_order(self, order, bidask):
        self._delete(order.oid, bidask)
        self._insert(order, bidask)

    def delete_order(self, oid, bidask):
        self._delete(oid, bidask)

    def traded_order(self, oid, bidask, trade_share):
        book = self._offer_book if (bidask - 1) else self._bid_book
        ind_tick = [oid in [order.oid for order in tick]
                    for tick in book].index(True)
        ind_priority = [order.oid for order in book[ind_tick]].index(oid)
        book[ind_tick][ind_priority].update(delta_quantity=trade_share)

    def best_five(self):
        bb_depth = [sum([bid.quantity for bid in bids])
                    for bids in self._bid_book[:5]]
        if len(self._bid_book) < 5:
            bb_depth = bb_depth + [0] * (5 - len(self._bid_book))

        ba_depth = [sum([offer.quantity for offer in offers])
                    for offers in self._offer_book[:5]]
        if len(self._offer_book) < 5:
            ba_depth = ba_depth + [0] * (5 - len(self._offer_book))

        bb_depth.reverse()  # Make bids in increasing order

        return (bb_depth + ba_depth)

    def bbo(self):
        return ([0 if len(self._bid_book) == 0
                 else self._bid_book[0][0].price,
                 0 if len(self._offer_book) == 0
                 else self._offer_book[0][0].price])
