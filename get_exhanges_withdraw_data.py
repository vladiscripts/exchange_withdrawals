import asyncio
import os
import ccxt.pro as ccxtpro
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, ForeignKey, Index, tuple_, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@localhost/exchanges_withdraw", pool_pre_ping=True)
Base = declarative_base()
Session = sessionmaker(bind=engine)


class Exchange(Base):
    __tablename__ = "exchanges"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)  # e.id


class Currency(Base):
    __tablename__ = "currencies"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(16), nullable=False, index=True, unique=True)


class Network(Base):
    __tablename__ = "networks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)  # "APT" / "ERC20" / ...


class WithdrawData(Base):
    __tablename__ = "data"
    id = Column(Integer, primary_key=True, autoincrement=True)

    exchange_id = Column(Integer, ForeignKey(Exchange.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
    currency_id = Column(Integer, ForeignKey(Currency.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
    network_id = Column(Integer, ForeignKey(Network.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

    deposit = Column(Boolean, nullable=True)
    withdraw = Column(Boolean, nullable=True)

    fee = Column(Float, nullable=True)
    min_withdraw = Column(Float, nullable=True)
    max_withdraw = Column(Float, nullable=True)
    min_deposit = Column(Float, nullable=True)
    max_deposit = Column(Float, nullable=True)

    precision = Column(Float, nullable=True)
    active = Column(Boolean, nullable=True)


t = WithdrawData
Index(None, t.exchange_id, t.currency_id, t.network_id, unique=True)

sql_to_view_create = '''
CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW exchanges_withdraw.data_view AS
SELECT
    e.code AS exch_code,
    c.code AS token_code,
    n.name AS net,
    d.deposit AS deposit,
    d.withdraw AS withdraw,
    d.fee AS fee,
    d.min_withdraw AS min_withdraw,
    d.min_deposit AS min_deposit,
    d.active AS active
FROM exchanges_withdraw.data d
JOIN exchanges_withdraw.exchanges e ON d.exchange_id = e.id
JOIN exchanges_withdraw.currencies c ON d.currency_id = c.id
JOIN exchanges_withdraw.networks n ON d.network_id = n.id;
'''


def get_or_create_exchange(session, exchange_code: str) -> int:
    t = Exchange
    result = session.execute(select(t.id).where(t.code == exchange_code)).scalar()
    if result is not None:
        return result
    obj = t(code=exchange_code)
    session.add(obj)
    session.flush()
    return obj.id


def get_or_create_currency(session, currency_code: str) -> int:
    t = Currency
    result = session.execute(select(t.id).where(t.code == currency_code)).scalar()
    if result is not None:
        return result
    obj = t(code=currency_code)
    session.add(obj)
    session.flush()
    return obj.id


def get_or_create_network(session, network_name: str) -> int:
    t = Network
    result = session.execute(select(t.id).where(t.name == network_name)).scalar()
    if result is not None:
        return result
    obj = t(name=network_name)
    session.add(obj)
    session.flush()
    return obj.id


async def load_markets_for_exchange(e):
    try:
        await e.load_markets()
        if not e.currencies or not e.currencies['USDT']['networks']:
            print(f"[ERROR] {e.id}: exchange.currencies is empty after load_markets(); skipping")
            return None
        else:
            print(f"[INFO] {e.id}: ok")
            return e
    except Exception as exc:
        print(f"[ERROR] Failed connection to {e.id}: {exc}")
        return None
    finally:
        try:
            await e.close()
        except Exception as exc:
            print(f"[ERROR] Error closing exchange {e.id}: {exc}")


async def get_exchanges(exchanges_list):
    tasks = [load_markets_for_exchange(e) for e in exchanges_list]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    valid_exchanges = []
    for result in results:
        if result is None:
            continue
        if isinstance(result, Exception):
            print(f"[ERROR] Unexpected exception in task: {result}")
            continue
        valid_exchanges.append(result)

    return valid_exchanges


def save_to_database(exchanges: list):
    Base.metadata.create_all(engine)  # create DB tables

    for e in exchanges:
        with Session() as session:
            try:
                db_ex_id = get_or_create_exchange(session, e.id)

                new_data = {}
                seen_triplets = set()
                t = WithdrawData

                for code, currency in e.currencies.items():
                    if not isinstance(currency, dict):
                        continue

                    networks = currency.get("networks")
                    if not isinstance(networks, dict):
                        continue

                    for net_name, net_data in networks.items():
                        if not isinstance(net_data, dict):
                            continue

                        cur_id = get_or_create_currency(session, code)
                        net_id = get_or_create_network(session, net_name)

                        triplet = (db_ex_id, cur_id, net_id)
                        seen_triplets.add(triplet)

                        new_data[triplet] = {
                            t.deposit: net_data.get("deposit"),
                            t.withdraw: net_data.get("withdraw"),
                            t.fee: net_data.get("fee"),
                            t.min_withdraw: net_data.get("limits", {}).get("withdraw", {}).get("min"),
                            t.max_withdraw: net_data.get("limits", {}).get("withdraw", {}).get("max"),
                            t.min_deposit: net_data.get("limits", {}).get("deposit", {}).get("min"),
                            t.max_deposit: net_data.get("limits", {}).get("deposit", {}).get("max"),
                            t.precision: net_data.get("precision"),
                            t.active: net_data.get("active")}

                # Получаем существующие данные
                existing_query = session.query(t).filter(t.exchange_id == db_ex_id)
                existing_map = {(r.currency_id, r.network_id): r for r in existing_query}

                to_insert = []
                to_update = []

                for (ex_id, cur_id, net_id), new_row in new_data.items():
                    key = (cur_id, net_id)
                    old_row = existing_map.get(key)

                    if old_row is None:
                        to_insert.append({
                            t.exchange_id: ex_id,
                            t.currency_id: cur_id,
                            t.network_id: net_id,
                            **new_row})
                    else:
                        updated_fields = {}
                        for field, new_val in new_row.items():
                            old_val = getattr(old_row, field.name)
                            if old_val != new_val:
                                updated_fields[field] = new_val

                        if updated_fields:
                            updated_fields[t.id] = old_row.id
                            to_update.append(updated_fields)

                # Удаление старых записей для этой биржи, которых нет в seen_triplets
                q = session.query(t).filter(t.exchange_id == db_ex_id)
                if seen_triplets:
                    q = session.query(t).filter(t.exchange_id == db_ex_id)
                    q = q.filter(~tuple_(t.exchange_id, t.currency_id, t.network_id).in_(list(seen_triplets)))
                    q.delete(synchronize_session=False)

                if to_insert:
                    to_insert = [{c.name: v for c, v in r.items()} for r in to_insert]
                    session.bulk_insert_mappings(t, to_insert)

                if to_update:
                    to_update = [{c.name: v for c, v in r.items()} for r in to_update]
                    session.bulk_update_mappings(t, to_update)

                session.commit()
                print(f"[INFO] {e.id}: ok")

            except Exception as exc:
                session.rollback()
                print(f"[ERROR] {exc}")
                raise


if __name__ == "__main__":
    # List of exchanges. Some requires API keys for get currencies data.
    exchanges = [
        ccxtpro.binance({"apiKey": os.getenv("BINANCE_KEY"), "secret": os.getenv("INANCE_SECRET")}),
        ccxtpro.bingx({'apiKey': os.getenv('BINGX_KEY'), 'secret': os.getenv('BINGX_SECRET')}),  # type: ignore
        ccxtpro.bitget(),
        # ccxtpro.bitmart(),  # api url blocked by ISP
        ccxtpro.bybit({'apiKey': os.getenv('BYBIT_KEY'), 'secret': os.getenv('BYBIT_SECRET')}),  # type: ignore
        # ccxtpro.gate({'apiKey': os.getenv('GATE_KEY'), 'secret': os.getenv('GATE_SECRET')}),  # type: ignore  # api url blocked by ISP
        ccxtpro.kucoin(),
        ccxtpro.mexc({'apiKey': os.getenv('MEXC_KEY'), 'secret': os.getenv('MEXC_SECRET')}),  # type: ignore
        ccxtpro.okx({'apiKey': os.getenv('OKX_KEY'), 'secret': os.getenv('OKX_SECRET'), 'password': os.getenv('OKX_PASSWORD')}),  # type: ignore
        # ccxtpro.htx({'apiKey': os.getenv('HTX_KEY'), 'secret': os.getenv('HTX_SECRET')}),  # type: ignore  # api url blocked by ISP
    ]
    print(f"[INFO] Download from exchanges...")
    exchanges_loaded = asyncio.run(get_exchanges(exchanges))
    print(f"[INFO] Saving to database...")
    save_to_database(exchanges)
