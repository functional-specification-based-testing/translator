import json
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from typing import List, Tuple, Dict, DefaultDict


@dataclass
class Trade:
    """Trade DTO"""
    price: float
    qty: int
    buy_id: str
    sell_id: str


@dataclass
class OrderRq:
    """Order Request DTO"""
    order_request_type: str
    old_id: str
    order_type: str
    id: str
    broker: str
    shareholder: str
    price: float
    qty: int
    side: str
    min_qty: int
    fak: bool
    disclodes_qty: int


class Translator:
    """Haskell to MMTP Translator"""
    security_id: str
    cisin: str
    group: str
    date: str
    time: str
    reference_price: float
    lower_bound_percentage: float
    upper_bound_percentage: float
    src_shareholder_id: str
    trade_cnt: int
    order_cnt: int
    orders: Dict[str, object]
    remaining_qty: DefaultDict[str, int]
    previous_remaining_qty: DefaultDict[str, int]
    sequence_nums: Dict[str, int]
    eliminated: Dict[str, bool]

    def __init__(
            self,
            security_id: str = "SPY",
            cisin: str = "US78462F1030",
            group: str = "N1",
            date: str = "20191028",
            time: str = "083000",
            reference_price: float = 10.0,
            lower_bound_percentage: float = 0.9,
            upper_bound_percentage: float = 0.9,
            src_shareholder_id: str = "1000",
    ):
        self.security_id = security_id
        self.cisin = cisin
        self.group = group
        self.date = date
        self.time = time
        self.reference_price = reference_price
        self.lower_bound_percentage = lower_bound_percentage
        self.upper_bound_percentage = upper_bound_percentage
        self.src_shareholder_id = src_shareholder_id
        self.trade_cnt = 0
        self.order_cnt = 0
        self.orders = {}
        self.remaining_qty = defaultdict(int)
        self.previous_remaining_qty = defaultdict(int)
        self.sequence_nums = {}
        self.eliminated = {}

    @staticmethod
    def translate_price_to_mmtp(price: float) -> str:
        """translate price to IFt-QMt9 MMTP format"""
        return "2" + "%07d" % int(price) + "00"  # FIXME

    def translate_admin_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        if rq[0] == "SetReferencePriceRq":
            return self.translate_reference_price_cmd(rq)
        elif rq[0] == "SetStaticPriceBandUpperLimitRq":
            return self.translate_upper_price_band_cmd(rq)
        elif rq[0] == "SetStaticPriceBandLowerLimitRq":
            return self.translate_lower_price_band_cmd(rq)
        elif rq[0] == "SetOwnershipRq":
            return self.translate_ownership_cmd(rq)
        elif rq[0] == "SetCreditRq":
            return self.translate_credit_cmd(rq)
        elif rq[0] == "SetTickSizeRq":
            return self.translate_tick_size_cmd(rq)
        elif rq[0] == "SetLotSizeRq":
            return self.translate_lot_size_cmd(rq)
        elif rq[0] == "SetOwnershipUpperLimitRq":
            return self.translate_ownership_upper_limit_cmd(rq)
        elif rq[0] == "SetTotalSharesRq":
            return self.translate_total_shares_cmd(rq)
        else:
            raise ValueError(rq[0])

    def translate_trade(self, trade: Trade) -> List[str]:
        self.update_order_book_view_by_trade(trade)
        return [
            self.translate_execution_notice(trade, self.orders[trade.buy_id]),
            self.translate_execution_notice(trade, self.orders[trade.sell_id]),
        ]

    def update_order_book_view_by_order(self, rq: OrderRq, eliminated: bool) -> None:
        assert rq.order_request_type in {"NewOrderRq", "ReplaceOrderRq"}, "Invalid order request type"
        if rq.order_request_type == "NewOrderRq":
            assert rq.id not in self.orders, "order %s already in order book view" % rq.id
            assert rq.id not in self.sequence_nums, "order %s already in order book view" % rq.id

        self.orders[rq.id] = rq
        self.order_cnt += 1
        self.sequence_nums[rq.id] = self.order_cnt
        self.remaining_qty[rq.id] = rq.qty if not eliminated else 0
        self.eliminated[rq.id] = eliminated

    def update_order_book_view_by_cancel_order(self, rq: OrderRq) -> None:
        assert rq.order_request_type == "CancelOrderRq", "Invalid order request type"
        assert rq.old_id in self.orders, "old order %s not in order book view" % rq.id
        assert rq.old_id in self.sequence_nums, "old order %s not in order book view" % rq.id

        self.orders[rq.id] = rq
        self.sequence_nums[rq.id] = self.sequence_nums[rq.old_id]
        self.remaining_qty[rq.id] = self.remaining_qty[rq.old_id]

    def update_order_book_view_by_trade(self, trade: Trade) -> None:
        assert trade.qty <= self.remaining_qty[trade.buy_id] and trade.qty <= self.remaining_qty[
            trade.sell_id], "not enough qty in order book view"

        self.trade_cnt += 1
        self.remaining_qty[trade.buy_id] -= trade.qty
        self.remaining_qty[trade.sell_id] -= trade.qty

    def regenerate_order_book(self, orderbook: [OrderRq]):
        self.previous_remaining_qty = self.remaining_qty
        self.remaining_qty = defaultdict(int)
        for queued_order in orderbook:
            self.remaining_qty[queued_order.id] = queued_order.qty

    def translate_incoming_order_cmd(self, rq: OrderRq, rs: List[object], trades: List[Trade], orderbook: [OrderRq]) -> \
            Tuple[str, List[str]]:
        order = self.translate_order(rq)
        # print(asdict(rq))
        translated_trades = []
        if rs[1] in {"Accepted", "Eliminated"}:
            self.update_order_book_view_by_order(rq, rs[1] == "Eliminated")

            traded_qty_on_entry = 0
            assert not (rs[1] == "Eliminated" and len(trades) > 0), "Eliminated orders should not generate trades"
            for trade in trades:
                traded_qty_on_entry += trade.qty
                translated_trades += self.translate_trade(trade)

            self.regenerate_order_book(orderbook)

            result = self.translate_confirmation_msg(rq, traded_qty_on_entry)
        else:
            result = self.translate_rejection_msg(rq)
            assert not trades, "trades on rejected order %s" % rq.id
        return order, [result] + translated_trades

    def translate_cancel_order_cmd(self, rq: OrderRq, rs: List[object], orderbook: [OrderRq]) -> Tuple[str, str]:
        if rq.old_id in self.orders:
            new_rq = deepcopy(self.orders[rq.old_id])
            new_rq.order_request_type = rq.order_request_type
            new_rq.old_id = rq.old_id
            new_rq.id = rq.id
            new_rq.side = rq.side
            rq = new_rq

        order = self.translate_cancel_order(rq)
        # print(asdict(rq))
        if rs[1] in {"Accepted", "Eliminated"}:
            self.update_order_book_view_by_cancel_order(rq)

            self.regenerate_order_book(orderbook)

            result = self.translate_confirmation_msg(rq)
        else:
            result = self.translate_rejection_msg(rq)
        return order, result

    def _read_state(self, haskell_res: List[List[object]], rs_idx: int, request_count: int):
        orderbook_count_msg = haskell_res[rs_idx]
        rs_idx += 1
        assert orderbook_count_msg[0] == "Orders", "line " + str(
            rs_idx + request_count + 2) + " OrderBooks length should be declared after OrderRq but " + \
                                                   orderbook_count_msg[0]
        orderbook_count = orderbook_count_msg[1]
        orderbook = haskell_res[rs_idx: rs_idx + orderbook_count]
        orderbook = list(map(lambda order: OrderRq(None, None, *order[1:]), orderbook))
        rs_idx += orderbook_count

        credits_count_msg = haskell_res[rs_idx]
        rs_idx += 1
        assert credits_count_msg[0] == "Credits", "line " + str(
            rs_idx + request_count + 2) + " Credits count should be declared after OrderRq but " + credits_count_msg[0]
        credits_count = credits_count_msg[1]
        rs_idx += credits_count

        ownerships_count_msg = haskell_res[rs_idx]
        rs_idx += 1
        assert ownerships_count_msg[0] == "Ownerships", "line " + str(
            rs_idx + request_count + 2) + " Ownerships count should be declared after OrderRq but " + \
                                                        ownerships_count_msg[0]
        ownerships_count = ownerships_count_msg[1]
        rs_idx += ownerships_count

        reference_price_msg = haskell_res[rs_idx]
        assert reference_price_msg[0] == "ReferencePrice", "line " + str(
            rs_idx + request_count + 2) + " ReferencePrice should be declared after OrderRq but " + reference_price_msg[
                                                               0]
        rs_idx += 1

        static_price_band_lower_limit_msg = haskell_res[rs_idx]
        assert static_price_band_lower_limit_msg[0] == "StaticPriceBandLowerLimit", "line " + str(
            rs_idx + request_count + 2) + " StaticPriceBandLowerLimit should be declared after OrderRq but " + \
                                                                                    reference_price_msg[0]
        rs_idx += 1

        static_price_band_upper_limit_msg = haskell_res[rs_idx]
        assert static_price_band_upper_limit_msg[0] == "StaticPriceBandUpperLimit", "line " + str(
            rs_idx + request_count + 2) + " StaticPriceBandUpperLimit should be declared after OrderRq but " + \
                                                                                    reference_price_msg[0]
        rs_idx += 1

        total_shares_msg = haskell_res[rs_idx]
        assert total_shares_msg[0] == "TotalShares", "line " + str(
            rs_idx + request_count + 2) + " TotalShares should be declared after OrderRq but " + reference_price_msg[0]
        rs_idx += 1

        ownership_upper_limit_msg = haskell_res[rs_idx]
        assert ownership_upper_limit_msg[0] == "OwnershipUpperLimit", "line " + str(
            rs_idx + request_count + 2) + " OwnershipUpperLimit should be declared after OrderRq but " + \
                                                                      reference_price_msg[0]
        rs_idx += 1

        tick_size_msg = haskell_res[rs_idx]
        assert tick_size_msg[0] == "TickSize", "line " + str(
            rs_idx + request_count + 2) + " TickSize should be declared after OrderRq but " + reference_price_msg[0]
        rs_idx += 1

        lot_size_msg = haskell_res[rs_idx]
        assert lot_size_msg[0] == "LotSize", "line " + str(
            rs_idx + request_count + 2) + " LotSize should be declared after OrderRq but " + reference_price_msg[0]
        rs_idx += 1

        return rs_idx, orderbook

    def _read_trades(self, haskell_res: List[List[object]], rs_idx: int, request_count: int):
        trades_count_msg = haskell_res[rs_idx]
        rs_idx += 1
        assert trades_count_msg[0] == "Trades", "line " + str(
            rs_idx + request_count + 2) + " Trades count should be declared after OrderRq but " + trades_count_msg[0]
        trades_count = trades_count_msg[1]
        trades = haskell_res[rs_idx: rs_idx + trades_count]
        trades = list(map(lambda trade: Trade(*trade[1:]), trades))
        rs_idx += trades_count

        return rs_idx, trades

    def translate(self, request_count: int, haskell: List[List[object]]) -> Tuple[List[str], List[str]]:
        haskell_req = haskell[:request_count]
        haskell_res = haskell[request_count:]
        translated_feed = []
        translated_result = []
        rs_idx = 0

        translated_feed.append(json.dumps({"command": "Change System State", "targetState": "TRADING_SESSION"}))
        translated_feed.append(json.dumps({"timestamp": "08:30:00.000000000"}))
        for i in range(request_count):
            rq = haskell_req[i]
            rs = haskell_res[rs_idx]
            rs_idx += 1
            # print(rq)
            # print(rs)
            assert rq[0].endswith("Rq"), "line " + str(i + 2) + " request should be ended with 'Rq' " + rq[0]
            assert rs[0].endswith("Rs"), "line " + str(
                rs_idx + request_count + 2) + " response should be ended with 'Rs' " + rq[0]
            assert rq[0][0:-2] == rs[0][0:-2], "line " + str(
                rs_idx + request_count + 2) + " response should match request: %s, %s" % (rq[0], rs[0])

            if rq[0].startswith("Set"):
                assert rs[1] == "Accepted", "unsuccessful admin command " + rq[0]

                feed, result = self.translate_admin_cmd(rq)
                translated_feed += feed
                translated_result += result

            else:
                if rq[0] in {"NewOrderRq", "ReplaceOrderRq"}:
                    order_rq = OrderRq(*rq)
                    rs_idx, trades = self._read_trades(haskell_res, rs_idx, request_count)
                    rs_idx, orderbook = self._read_state(haskell_res, rs_idx, request_count)
                    feed, results = self.translate_incoming_order_cmd(order_rq, rs, trades, orderbook)
                    translated_feed.append(feed)
                    translated_result += results
                elif rq[0] == "CancelOrderRq":
                    order_rq = OrderRq(*rq, None, None, None)
                    rs_idx, orderbook = self._read_state(haskell_res, rs_idx, request_count)
                    feed, result = self.translate_cancel_order_cmd(order_rq, rs, orderbook)
                    translated_feed.append(feed)
                    translated_result.append(result)
                else:
                    raise RuntimeError("Invalid request type '%s'" % rq[0])

        # translated_feed.append(json.dumps({"command": "End Session"}))
        translated_feed.append(json.dumps({"command": "Shutdown"}))

        return translated_feed, translated_result

    def translate_reference_price_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Change Static Price Band" Admin Command to set reference price"""
        self.reference_price = float(rq[1])
        return self.get_price_band_cmds(), [""]

    def translate_lower_price_band_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Change Static Price Band" Admin Command to set lower price band"""
        self.lower_bound_percentage = float(rq[1])
        return self.get_price_band_cmds(), [""]

    def translate_upper_price_band_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Change Static Price Band" Admin Command to set upper price band"""
        self.upper_bound_percentage = float(rq[1])
        return self.get_price_band_cmds(), [""]

    def get_price_band_cmds(self) -> [str]:
        return [
            json.dumps({
                "command": "Change Security State",
                "items": [self.security_id],
                "groupName": None,
                "targetState": "SURVEILLANCE"
            }),
            json.dumps({
                "command": "Change Security Static Price Band",
                "items": [self.security_id],
                "priceBandPercentage":
                    {"upperBound": self.upper_bound_percentage * 100,
                     "lowerBound": self.lower_bound_percentage * 100},
                "referencePrice": self.reference_price,
                "groupCode": None
            }),
            json.dumps({
                "command": "Change Security State",
                "items": [self.security_id],
                "groupName": None,
                "targetState": "RESERVED"
            }),
            json.dumps({
                "command": "Change Security State",
                "items": [self.security_id],
                "groupName": None,
                "targetState": "OPENED"
            }),
        ]

    def translate_ownership_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set Ownership" Admin Command to set ownership of shareholders"""
        return ["SET PO shareholder=%s shares=%s" % tuple(rq[1:])], [""]

    def translate_credit_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set Credit" Admin Command to set credit of brokers"""
        return ["SET CM broker=%s credit=%s" % tuple(rq[1:])], [""]

    def translate_tick_size_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set Tick" Admin Command to set security price tick size"""
        return ["SET SECURITY tick=%s" % tuple(rq[1:])], [""]

    def translate_lot_size_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set Lot" Admin Command to set security quantity lot size"""
        return ["SET SECURITY lot=%s" % tuple(rq[1:])], [""]

    def translate_ownership_upper_limit_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set OwnershipUpperLimit" Admin Command to set security max allowed percentage of ownership"""
        return ["SET SECURITY ownershipUpperLimit=%s" % tuple(rq[1:])], [""]

    def translate_total_shares_cmd(self, rq: List[object]) -> Tuple[List[str], List[str]]:
        """translate "Set TotalShares" Admin Command to set security total number of shares"""
        return ["SET SECURITY totalShares=%s" % tuple(rq[1:])], [""]

    def translate_order(self, rq: OrderRq) -> str:
        """translate SLE-0001 & SLE-0002"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            {"NewOrderRq": "0001", "ReplaceOrderRq": "0002"}.get(rq.order_request_type, "0000"),
            self.date,  # original order date
            "%06d" % self.sequence_nums.get(rq.old_id, 0),  # original HON
            str(self.security_id).ljust(12),
            {"BUY": "A", "SELL": "V", "CROSS": "2"}.get(rq.side, " "),  # side
            "%012d" % rq.qty,  # qty
            {"Limit": "L", "Iceberg": "L"}.get(rq.order_type, " "),  # type
            self.translate_price_to_mmtp(rq.price),  # price
            {True: "E", False: "J"}.get(rq.fak, " "),  # validity type
            "%08d" % 0,  # validity date
            "%012d" % rq.min_qty,  # min qty
            "%012d" % (rq.disclodes_qty if rq.order_type == "Iceberg" else 0),  # disclosed qty
            str(rq.broker).ljust(8),  # broker
            "A",  # technical origin
            "0",  # confirmation flag
            "0",  # preopening flag
            " %09d" % 0,  # trigger price
            "%6s" % "",
            "%012d" % self.remaining_qty.get(rq.old_id, 0),  # expected remaining qty
            " 189980021",
            str(rq.shareholder).ljust(16),  # shareholder
            "1       ",
            self.date,
            self.time,
            "189-IR98001",
            "%26s" % "",
        ])

    def translate_cancel_order(self, rq: OrderRq) -> str:
        """translate SLE-0003"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            {"CancelOrderRq": "0003"}.get(rq.order_request_type, "0000"),
            self.date,  # original order date
            "%06d" % self.sequence_nums.get(rq.old_id, 0),  # original HON
            str(rq.broker if rq.broker else "").ljust(8),  # broker
            str(self.security_id).ljust(12),
            {"BUY": "A", "SELL": "V", "CROSS": "2"}.get(rq.side, " "),  # side
        ])

    def translate_rejection_msg(self, rq: OrderRq) -> str:
        """translate SLE-0144"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            "0144",
            {"NewOrderRq": "0001", "ReplaceOrderRq": "0002", "CancelOrderRq": "0003"}.get(rq.order_request_type,
                                                                                          "0000"),
            "%06d" % 0,
            "".ljust(71),
        ])

    def translate_confirmation_msg(self, rq: OrderRq, traded_qty_on_entry: int = 0) -> str:
        """translate SLE-0172"""
        if rq.order_request_type == "CancelOrderRq":
            order_status = "A"
        elif self.eliminated[rq.id]:
            order_status = "E"
        elif self.remaining_qty[rq.id] == 0:
            order_status = "X"
        else:
            order_status = " "

        return "".join([
            ("%d=" % rq.id).ljust(16),
            "0172",
            self.date,
            "%06d" % self.sequence_nums[rq.id],  # HON
            order_status,  # status
            str(self.security_id).ljust(12),
            {
                "NewOrderRq": "%012d" % rq.qty,
                "ReplaceOrderRq": "%012d" % rq.qty,
                "CancelOrderRq": "%012d" % self.previous_remaining_qty[rq.old_id],
            }.get(rq.order_request_type, "%012d" % 0),  # qty
            # "%012d" % rq.qty,  # qty
            {"BUY": "A", "SELL": "V", "CROSS": "2"}.get(rq.side, " "),  # side
            self.translate_price_to_mmtp(rq.price),  # price
            str(rq.broker).ljust(8),  # broker
            "%06d" % 0,
            self.time,
            {"Limit": "L", "Iceberg": "L"}.get(rq.order_type, " "),  # type
            "%012d" % traded_qty_on_entry,  # matched qty at entry
            {
                "NewOrderRq": "0001",
                "ReplaceOrderRq": "0002",
                "CancelOrderRq": "0003",
            }.get(rq.order_request_type, "0000"),  # original function code
            {
                "NewOrderRq": "%08d" % 0,
                "ReplaceOrderRq": self.date,
                "CancelOrderRq": "%08d" % 0,
            }.get(rq.order_request_type, "%08d" % 0),  # original order date
            {
                "NewOrderRq": "%06d" % 0,
                "ReplaceOrderRq": "%06d" % self.sequence_nums.get(rq.old_id, 0),
                "CancelOrderRq": "%06d" % 0,
            }.get(rq.order_request_type, "%06d" % 0),  # original HON
            {True: "E", False: "J"}.get(rq.fak, " "),  # validity type
            {
                "NewOrderRq": "%08d" % 0,
                "ReplaceOrderRq": "%08d" % 0,
                "CancelOrderRq": self.date,
            }.get(rq.order_request_type, "%08d" % 0),  # validity date
            "%012d" % rq.min_qty,  # min qty
            "%012d" % (rq.disclodes_qty if rq.order_type == "Iceberg" else 0),  # disclosed qty
            "A",  # technical origin
            "0",  # confirmation flag
            "%012d" % {
                "NewOrderRq": 0,
                "ReplaceOrderRq": self.previous_remaining_qty.get(rq.old_id, 0),
                "CancelOrderRq": 0,
            }.get(rq.order_request_type, 0),  # original remaining qty
            " %09d" % 0,  # trigger price
            "%06d" % 0,
            " 189980021",
            str(rq.shareholder).ljust(16),  # shareholder
            "1       ",
            self.date,
            self.time,
            "189-IR98001",
            "                          ",
            ("%s%s" % (self.date, self.time)).ljust(20, "0"),
        ])

    def translate_execution_notice(self, trade: Trade, rq: OrderRq) -> str:
        """translate SLE-0105"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            "0105",
            self.date,
            "%06d" % self.sequence_nums[rq.id],  # HON
            str(self.security_id).ljust(12),
            str(self.group).ljust(2),
            {"BUY": "A", "SELL": "V", "CROSS": "2"}.get(rq.side, " "),  # side
            "%012d" % trade.qty,
            self.translate_price_to_mmtp(trade.price),  # price
            ["0", "1"][self.remaining_qty[rq.id] > 0],  # remaining qty flag
            "%012d" % self.remaining_qty[rq.id],  # remaining qty
            "%8s" % "",  # counterpart broker
            "1",  # counterpart origin
            "A",  # technical origin
            "%06d" % 0,
            self.date,
            self.time,
            "%07d" % self.trade_cnt,  # trade number
            self.date,
            {"Limit": "L", "Iceberg": "L"}.get(rq.order_type, " "),  # type
            {True: "E", False: "J"}.get(rq.fak, " "),  # validity type
            "A",  # Code of the instrument category
            "%9s" % "",
            "189980021",
            str(rq.shareholder).ljust(16),  # shareholder
            "1       ",
            self.date,
            self.time,
            "189-IR98001",
            ("%s%s" % (self.date, self.time)).ljust(20, "0"),
            "%6s" % "",
            "00",
        ])
