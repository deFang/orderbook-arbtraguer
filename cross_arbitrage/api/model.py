
import peewee


_db_proxy = peewee.DatabaseProxy()


def init_db(db: peewee.Database):
    _db_proxy.initialize(db)
    db.create_tables([ExchangeBalance])


class ExchangeBalance(peewee.Model):
    timestamp = peewee.BigIntegerField()
    exchange = peewee.CharField(max_length=255)

    total_cash_usd = peewee.DecimalField(max_digits=32, decimal_places=12)
    total_cash_usdt = peewee.DecimalField(
        max_digits=32, decimal_places=12, null=True)
    total_margin_usd = peewee.DecimalField(max_digits=32, decimal_places=12)
    total_margin_usdt = peewee.DecimalField(
        max_digits=32, decimal_places=12, null=True)
    pos_used_usd = peewee.DecimalField(max_digits=32, decimal_places=12)
    pnl_usd = peewee.DecimalField(max_digits=32, decimal_places=12)

    class Meta:
        table_name = 'exchange_balance'
        database = _db_proxy


ExchangeBalance.add_index(ExchangeBalance.exchange, ExchangeBalance.timestamp)
