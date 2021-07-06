import json
from typing import List, Tuple, Dict
from dataclasses import dataclass, asdict


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
    lower_bound_percentage: float
    upper_bound_percentage: float
    src_shareholder_id: str
    trade_cnt: int
    order_cnt: int
    orders: Dict[str, object]
    remaining_qty: Dict[str, int]
    sequence_nums: Dict[str, int]

    def __init__(
        self,
        security_id: str = "SPY",
        cisin: str = "SPY",
        group: str = "N1",
        date: str = "20191028",
        time: str = "083000",
        lower_bound_percentage: float = 0.4,
        upper_bound_percentage: float = 0.4,
        src_shareholder_id: str = "1000",
    ):
        self.security_id = security_id
        self.cisin = cisin
        self.group = group
        self.date = date
        self.time = time
        self.lower_bound_percentage = lower_bound_percentage
        self.upper_bound_percentage = upper_bound_percentage
        self.src_shareholder_id = src_shareholder_id
        self.trade_cnt = 0
        self.order_cnt = 0
        self.orders = {}
        self.remaining_qty = {}
        self.sequence_nums = {}

    @staticmethod
    def translate_price_to_mmtp(price: float):
        """translate price to IFt-QMt9 MMTP format"""
        return "2" + "%07d" % int(price) + "00"  # FIXME

    def translate_admin_cmd(self, rq: List[object]) -> Tuple[str, str]:
        if rq[0] == "SetReferencePriceRq":
            return self.translate_reference_price_cmd(rq)
        elif rq[0] == "SetOwnershipRq":
            return self.translate_ownership_cmd(rq)
        elif rq[0] == "SetCreditRq":
            return self.translate_credit_cmd(rq)
        else:
            raise ValueError(rq[0])

    def translate_trade(self, trade: Trade) -> List[str]:
        self.update_order_book_view_by_trade(trade)
        return [
            self.translate_execution_notice(trade, self.orders[trade.buy_id]),
            self.translate_execution_notice(trade, self.orders[trade.sell_id]),
        ]

    def update_order_book_view_by_order(self, rq: OrderRq) -> None:
        if rq.order_request_type == "NewOrderRq":
            assert rq.id not in self.orders, "order %s already in order book view" % rq.id
            assert rq.id not in self.sequence_nums, "order %s already in order book view" % rq.id
            assert rq.id not in self.remaining_qty, "order %s already in order book view" % rq.id
        
        self.orders[rq.id] = rq
        self.order_cnt += 1
        self.sequence_nums[rq.id] = self.order_cnt
        self.remaining_qty[rq.id] = rq.qty

    def update_order_book_view_by_trade(self, trade: Trade) -> None:
        assert trade.buy_id in self.remaining_qty, "trade buy order (%s) not in order book view" % trade.buy_id
        assert trade.sell_id in self.remaining_qty, "trade sell order (%s) not in order book view" % trade.sell_id
        assert trade.qty <= self.remaining_qty[trade.buy_id] and trade.qty <= self.remaining_qty[trade.sell_id], "not enough qty in order book view"

        self.trade_cnt += 1
        self.remaining_qty[trade.buy_id] -= trade.qty
        self.remaining_qty[trade.sell_id] -= trade.qty

    def translate_new_order_cmd(self, rq: OrderRq, rs: List[object], trades: List[Trade]) -> Tuple[str, List[str]]:
        order = self.translate_order(rq)
        print(asdict(rq))
        translated_trades = []
        if rs[1]:
            self.update_order_book_view_by_order(rq)
            for trade in trades:
                translated_trades += self.translate_trade(trade)
            result = self.translate_confirmation_msg(rq)
        else:
            result = self.translate_rejection_msg(rq)
            assert not trades, "trades on rejected order %s" % rq.id
        return order, [result] + translated_trades

    def translate(self, request_count: int, haskell: List[List[object]]) -> Tuple[List[str], List[str]]:
        haskell_req = haskell[:request_count]
        haskell_res = haskell[request_count:]
        translated_feed = []
        translated_result = []
        rs_idx = 0

        translated_feed.append(json.dumps({"command": "Start Session"}))

        for i in range(request_count):
            rq = haskell_req[i]
            rs = haskell_res[rs_idx]
            rs_idx += 1
            print(rq)
            print(rs)
            assert rq[0].endswith("Rq"), "line " + str(i+2) + " request should be ended with 'Rq' " + rq[0]
            assert rs[0].endswith("Rs"), "line " + str(rs_idx+request_count+2) + " response should be ended with 'Rs' " + rq[0]
            assert rq[0][0:-2] == rs[0][0:-2], "line " + str(rs_idx+request_count+2) + " response should match request: %s, %s" % (rq[0], rs[0])
            
            if rq[0].startswith("Set"):
                assert rs[1], "unsuccessful admin command " + rq[0]
            
                feed, result = self.translate_admin_cmd(rq)
                translated_feed.append(feed)
                translated_result.append(result)
            
            else:
                if rq[0] == "NewOrderRq":
                    order_rq = OrderRq(*rq)
                    trades_count_msg = haskell_res[rs_idx]
                    rs_idx += 1
                    assert trades_count_msg[0] == "Trades", "line " + str(rs_idx+request_count+2) + " Trades count should be declared after OrderRq but " + trades_count_msg[0]
                    trades_count = trades_count_msg[1]
                    trades = haskell_res[rs_idx: rs_idx+trades_count]
                    trades = list(map(lambda trade: Trade(*trade[1:]), trades))
                    rs_idx += trades_count
                    
                    feed, results = self.translate_new_order_cmd(order_rq, rs, trades)
                    translated_feed.append(feed)
                    translated_result += results
        
        translated_feed.append(json.dumps({"command": "End Session"}))
        translated_feed.append(json.dumps({"command": "Shutdown"}))

        return translated_feed, translated_result

    def translate_reference_price_cmd(self, rq: List[object]) -> Tuple[str, str]:
        """translate "Change Static Price Band" Admin Command to set reference price & price bands"""
        return json.dumps({
            "command": "Change Static Price Band",
            "staticPriceBandData": {
                "lowerBoundPercentage": self.lower_bound_percentage,
                "upperBoundPercentage": self.upper_bound_percentage,
                "referencePrice": float(rq[1]),
                "securityId": self.security_id,
            },
        }), ""

    def translate_ownership_cmd(self, rq: List[object]) -> Tuple[str, str]:
        """translate "Transfer Share" Admin Command to set ownership of shareholders

        supports at most one SetOwnershipRq for each shareholder
        """
        return json.dumps({
            "command": "Transfer Share",
            "transferCommand": {
                "sourceId": self.src_shareholder_id,
                "destinationId": str(rq[1]),
                "cisin": self.cisin,
                "quantity": rq[2],
                # "isBlockedStatusIgnored": False,
            },
        }), ""

    def translate_credit_cmd(self, rq: List[object]) -> Tuple[str, str]:
        """translate "Set Credit" Admin Command to set credit of brokers"""
        return "POST CM broker=%s credit=%s" % tuple(rq[1:]), ""

    def translate_order(self, rq: OrderRq) -> str:
        """translate SLE-0001 & SLE-0002"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            {"NewOrderRq": "0001", "ReplaceOrderRq": "0002"}.get(rq.order_request_type, "0000"),
            self.date,  # original order date
            "%06d" % 0,  # HON FIXME for replace
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
            "%012d" % 0,  # expected remaining qty FIXME for replace
            " 189980021",
            str(rq.shareholder).ljust(16),  # shareholder
            "1       ",
            self.date,
            self.time,
            "189-IR98001",
            "%26s" % "",
        ])

    def translate_rejection_msg(self, rq: OrderRq) -> str:
        """translate SLE-0144"""
        return "".join([
            ("%d=" % rq.id).ljust(16),
            "0144",
            {"NewOrderRq": "0001", "ReplaceOrderRq": "0002"}.get(rq.order_request_type, "0000"),
            "%06d" % 0,
            "".ljust(71),
        ])

    def translate_confirmation_msg(self, rq: OrderRq) -> str:
        """translate SLE-0172"""
        if rq.order_request_type == "CancelOrderRq":
            order_status = "A"
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
            "%012d" % rq.qty,  # qty
            {"BUY": "A", "SELL": "V", "CROSS": "2"}.get(rq.side, " "),  # side
            self.translate_price_to_mmtp(rq.price),  # price
            str(rq.broker).ljust(8),  # broker
            "%06d" % 0,
            self.time,
            {"Limit": "L", "Iceberg": "L"}.get(rq.order_type, " "),  # type
            "%012d" % (rq.qty - self.remaining_qty[rq.id]),  # matched qty at entry
            {"NewOrderRq": "0001", "ReplaceOrderRq": "0002"}.get(rq.order_request_type, "0000"),  # original function code
            {
                "NewOrderRq": "%8s" % "",  # "%08d" % 0,
                "ReplaceOrderRq": self.date
            }.get(rq.order_request_type, "0000"),  # original order date
            "%06d" % 0,  # original HON FIXME for replace
            {True: "E", False: "J"}.get(rq.fak, " "),  # validity type
            "%08d" % 0,  # validity date
            "%012d" % rq.min_qty,  # min qty
            "%012d" % (rq.disclodes_qty if rq.order_type == "Iceberg" else 0),  # disclosed qty
            "A",  # technical origin
            "0",  # confirmation flag
            "%012d" % 0,  # self.remaining_qty[rq.id],  # remaining qty
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
