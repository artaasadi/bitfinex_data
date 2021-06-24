from bfxapi import BfxRest
import asyncio
import time
import json


bfx = BfxRest(None, None)


async def get_one_pair_snapshot(sym) -> dict:
    pair_snapshot: dict = {'symbol': sym}
    get_books_time = time.time() * 1000.0
    pair_snapshot.update({'timestamp': get_books_time})
    books_list = await bfx.get_public_books(symbol=sym, precision='P0', length=25)
    pair_snapshot.update({'books_list': books_list})
    return pair_snapshot


async def get_pair_list():
    list_of_pairs = []
    try:
        with open('bitfinex-pairs.json', 'r') as file:
            list_of_pairs = json.load(file)
            print('loaded successfully')
    except Exception as e:
        print('could not load')
        get_pairs_from_bitfinex = await bfx.fetch('conf/pub:list:pair:exchange')
        for i in get_pairs_from_bitfinex[0]:
            i = 't' + i.replace(':', '')
            if i not in ['t1INCHUSD', 't1INCHUST', 'tAAVEUSD', 'tAAVEUST', 'tALBTUSD', 'tALBTUST', 'tAVAXUSD',
                         'tAVAXUST', 'tB21XUSD', 'tB21XUST', 'tBANDUSD', 'tBANDUST', 'tBCHABCUSD', 'tBCHNUSD',
                         'tBESTUSD', 'tBOSONUSD', 'tBOSONUST', 'tBTCCNHT', 'tBTSEUSD', 'tCHEXUSD', 'tCNHCNHT',
                         'tCOMPUSD', 'tCOMPUST', 'tDAPPUSD', 'tDAPPUST', 'tDOGEUSD', 'tDOGEUST', 'tDUSKBTC', 'tDUSKUSD',
                         'tEGLDUSD', 'tEGLDUST', 'tEOSDTUSD', 'tEOSDTUST', 'tETH2XETH', 'tETH2XUSD', 'tETH2XUST',
                         'tEXRDBTC', 'tEXRDUSD', 'tFORTHUSD', 'tFORTHUST', 'tLINKUSD', 'tLINKUST', 'tLUNAUSD',
                         'tLUNAUST', 'tNEARUSD', 'tNEARUST', 'tRINGXUSD', 'tSUKUUSD', 'tSUKUUST', 'tSUSHIUSD',
                         'tSUSHIUST', 'tTESTBTCTESTUSD', 'tTESTBTCTESTUSDT', 'tUSTCNHT', 'tXAUTBTC', 'tXAUTUSD',
                         'tXAUTUST']:
                list_of_pairs.append(i)
        with open('./bitfinex-pairs.json', 'w') as output:
            json.dump(list_of_pairs, output, indent=2)
    return list_of_pairs


async def get_snapshots():
    list_of_pairs = await get_pair_list()
    list_of_snapshots = []

    data = asyncio.get_event_loop()
    loop_list = []
    for pair in list_of_pairs:
        try:
            loop_list.append(data.create_task(get_one_pair_snapshot(pair)))
        except Exception as e:
            print('get_snapshots1')
            print(e)

    await asyncio.wait(loop_list)
    for task in loop_list:
        try:
            list_of_snapshots.append(task.result())
        except Exception as e:
            print('get_snapshots2')
            print(e)

    return list_of_snapshots


async def get_one_pair_trades(sym, start, end, limit=100) -> dict:
    pair_trades: dict = {'symbol': sym, 'start_time': start, 'end_time': end}
    trades_list = await bfx.get_public_trades(symbol=sym,start=start,end=end,limit=limit)
    pair_trades.update({'trades_list': trades_list})
    return pair_trades


async def get_trades(start, end=time.time()*1000.0):
    list_of_pairs = await get_pair_list()
    list_of_pairs_trades = []

    data = asyncio.get_event_loop()
    loop_list = []
    for pair in list_of_pairs:
        try:
            loop_list.append(data.create_task(get_one_pair_trades(sym=pair, start=start, end=end, limit=100)))
        except Exception as e:
            print('get_trades1')
            print(e)

    await asyncio.wait(loop_list)
    for task in loop_list:
        try:
            list_of_pairs_trades.append(task.result())
        except Exception as e:
            print('get_trades2')
            print(e)

    return list_of_pairs_trades


if __name__ == '__main__':
    select = input('input t if you want to see the trades and input s if you want to see the snapshots: ')
    start_time = time.time() * 1000.0
    print('start: ', str(start_time))
    if select is 't':
        try:
            loop = asyncio.get_event_loop()
            pairs_snapshots = loop.run_until_complete(get_snapshots())
            for snapshot in pairs_snapshots:
                print(snapshot)
            with open('./bitfinex-snapshots-{ts}.json'.format(ts=str(start_time)), 'w') as output:
                json.dump(pairs_snapshots, output, indent=2)
        except Exception as e:
            print(e)
        finally:
            loop.close()
    elif select is 's':
        try:
            loop2 = asyncio.get_event_loop()
            pairs_trades = loop2.run_until_complete(get_trades(start_time - 1000*60*60))
            for trades in pairs_trades:
                print(trades)
            with open('./bitfinex-trades-{ts}.json'.format(ts=str(start_time)), 'w') as output:
                json.dump(pairs_trades, output, indent=2)
        except Exception as e:
            print(e)
        finally:
            loop2.close()

    end_time = time.time() * 1000.0
    print('end: ', end_time)
    print('whole time:', end_time - start_time)
