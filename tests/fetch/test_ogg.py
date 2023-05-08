from cross_arbitrage.fetch.fetch_orderbook import normalize_orderbook_5


def test_normalize_orderbook():
    ask = [
        ["7403.89", "0.002"],
        ["7403.90", "3.906"],
        ["7404.00", "1.428"],
        ["7404.85", "5.239"],
        ["7405.43", "2.562"],
    ]
    expect = [
        ["7.40389", "0.002"],
        ["7.4039", "3.906"],
        ["7.404", "1.428"],
        ["7.40485", "5.239"],
        ["7.40543", "2.562"],
    ]

    res = normalize_orderbook_5(ask, 1000)
    assert res == expect
