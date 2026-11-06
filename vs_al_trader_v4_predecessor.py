#!/usr/bin/env python3
# VS AL TRADER - COMPLETE BINANCE DATA POOL
# Veriyi tüm aktif USDT perpetual pariteler için parça parça toplar,
# tek havuzda birleştirir ve veri eksikse sinyal üretmez.
# Mevcut VS AL TRADER strateji/puanlama kurallarına yeni filtre eklemez.

import asyncio, aiohttp, json, math
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone

BASE='https://fapi.binance.com'
CHUNK=20
CONCURRENCY=8
LIMIT=121
TIMEOUT=20


def now(): return datetime.now(timezone.utc).isoformat()

def finite(v):
    try: return math.isfinite(float(v))
    except: return False

async def get_json(s, path, params=None, retries=3):
    last=None
    for n in range(retries):
        try:
            async with s.get(BASE+path, params=params) as r:
                t=await r.text()
                if r.status==200: return json.loads(t)
                last=f'HTTP {r.status}: {t[:300]}'
        except Exception as e: last=repr(e)
        await asyncio.sleep(0.7*(n+1))
    raise RuntimeError(last)

async def exchange_info(s):
    return await get_json(s,'/fapi/v1/exchangeInfo')

async def server_time(s):
    x=await get_json(s,'/fapi/v1/time')
    return int(x['serverTime'])

async def tickers(s):
    return await get_json(s,'/fapi/v1/ticker/24hr')

async def klines(s,symbol,interval):
    x=await get_json(s,'/fapi/v1/klines',{'symbol':symbol,'interval':interval,'limit':LIMIT})
    return [{'open_time':int(a[0]),'open':float(a[1]),'high':float(a[2]),'low':float(a[3]),
             'close':float(a[4]),'volume':float(a[5]),'close_time':int(a[6]),
             'quote_volume':float(a[7]),'trades':int(a[8]),
             'taker_buy_base':float(a[9]),'taker_buy_quote':float(a[10])} for a in x]

async def liquidity(s,symbol):
    ls, mp = await asyncio.gather(
        get_json(s,'/futures/data/globalLongShortAccountRatio',
                 {'symbol':symbol,'period':'30m','limit':1}),
        get_json(s,'/fapi/v1/premiumIndex',{'symbol':symbol})
    )
    a=ls[-1] if ls else None
    return {
      'longShort': ({'longAccount':float(a['longAccount']),
                     'shortAccount':float(a['shortAccount']),
                     'longShortRatio':float(a['longShortRatio']),
                     'timestamp':int(a['timestamp'])} if a else None),
      'markFunding': ({'markPrice':float(mp['markPrice']),
                       'indexPrice':float(mp['indexPrice']),
                       'funding':float(mp['lastFundingRate']),
                       'nextFundingTime':int(mp['nextFundingTime']),
                       'time':int(mp['time'])} if mp else None)
    }

async def one(s,meta,ticker):
    sym=meta['symbol']; errors=[]; warnings=[]
    async def k(tf):
        try: return await klines(s,sym,tf)
        except Exception as e: errors.append(f'{tf}: {e}'); return []
    try: liq=await liquidity(s,sym)
    except Exception as e:
        # Liquidity/positioning is optional context, not a mandatory candle-data gate.
        liq=None; warnings.append(f'liquidity unavailable: {e}')
    a,b,c=await asyncio.gather(k('4h'),k('30m'),k('15m'))
    return {'symbol':sym,'meta':meta,'ticker24h':ticker,
            'klines':{'4h':a,'30m':b,'15m':c},'liquidity':liq,
            'errors':errors,'warnings':warnings,'collected_at':now()}

async def bounded(s,items,universe,tmap):
    sem=asyncio.Semaphore(CONCURRENCY)
    async def run(sym):
        async with sem:
            try: return sym,await one(s,universe[sym],tmap.get(sym)),None
            except Exception as e: return sym,None,repr(e)
    return await asyncio.gather(*(run(x) for x in items))


def valid_klines(rows, server_ms=None):
    if len(rows)<61: return False
    check = rows[:-1] if server_ms is not None and rows[-1]['close_time'] >= server_ms else rows
    if len(check)<60: return False
    return all(all(finite(r[k]) for k in ('open','high','low','close','volume')) and r['high']>=r['low']>0 and r['close']>0 for r in check[-60:])

def ema(values, period):
    if len(values)<period: return values[-1] if values else 0.0
    alpha=2/(period+1); value=sum(values[:period])/period
    for x in values[period:]: value=x*alpha+value*(1-alpha)
    return value

def rsi(values, period=14):
    # Wilder RSI: smooth gains/losses over the full available series.
    if len(values) <= period: return 50.0
    changes=[values[i]-values[i-1] for i in range(1,len(values))]
    gains=[max(x,0.0) for x in changes]
    losses=[max(-x,0.0) for x in changes]
    avg_gain=sum(gains[:period])/period
    avg_loss=sum(losses[:period])/period
    for g,l in zip(gains[period:],losses[period:]):
        avg_gain=(avg_gain*(period-1)+g)/period
        avg_loss=(avg_loss*(period-1)+l)/period
    if avg_loss==0: return 100.0 if avg_gain else 50.0
    return 100-100/(1+avg_gain/avg_loss)

def atr(rows, period=14):
    if len(rows)<period+1:return 0.0
    trs=[]
    for i in range(1,len(rows)):
        r,prev=rows[i],rows[i-1]
        trs.append(max(r['high']-r['low'],abs(r['high']-prev['close']),abs(r['low']-prev['close'])))
    return sum(trs[-period:])/period

def state(rows):
    closes=[r['close'] for r in rows]; vols=[r['volume'] for r in rows]
    e20,e50=ema(closes,20),ema(closes,50); price=closes[-1]
    trend='LONG' if price>e20 and e20>e50 else 'SHORT' if price<e20 and e20<e50 else 'NEUTRAL'
    # Compare the latest candle against prior candles only, not against an average containing itself.
    prior_vols=vols[-21:-1]
    av=sum(prior_vols)/len(prior_vols) if prior_vols else 0
    return {'trend':trend,'price':price,'ema20':e20,'rsi':rsi(closes),'atr':atr(rows),
            'vol_ratio':vols[-1]/av if av else 0,
            'range_high':max(r['high'] for r in rows[-20:]),'range_low':min(r['low'] for r in rows[-20:]),
            'last_high':rows[-1]['high'],'last_low':rows[-1]['low']}

def round_to_tick(value, tick_size):
    """Round a price to the exchange tick size without float-step errors."""
    try:
        tick = Decimal(str(tick_size))
        if tick <= 0:
            return float(value)
        units = (Decimal(str(value)) / tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
        return float(units * tick)
    except Exception:
        return float(value)


def analyze_vs_al(record, diagnostics=None):
    # V4: 30M is the hard direction filter; 15M confirms and defines the trigger entry.
    # Every rejection is counted so a zero-signal run can be diagnosed.
    def reject(reason):
        if diagnostics is not None:
            diagnostics[reason] = diagnostics.get(reason, 0) + 1
        return None

    rows4, rows30, rows15 = (record.get('klines', {}).get(tf, []) for tf in ('4h', '30m', '15m'))
    if min(len(rows4), len(rows30), len(rows15)) < 61:
        return reject('yetersiz_mum_verisi')
    server_ms = record.get('exchange_server_time_ms')
    def closed(rows):
        return [r for r in rows if server_ms is None or r['close_time'] < server_ms]
    c4, c30, c15 = closed(rows4), closed(rows30), closed(rows15)
    if min(len(c4), len(c30), len(c15)) < 60:
        return reject('yeterli_kapanmis_mum_yok')

    context4, a, b, trigger = state(c4), state(c30), state(c15), state(c15)
    direction = a['trend']
    if direction not in ('LONG', 'SHORT'):
        return reject('30m_yonu_belirsiz')
    if b['trend'] != direction:
        return reject('15m_teyit_uyumsuz')
    if direction == 'LONG' and not (50 <= b['rsi'] < 75):
        return reject('long_rsi_filtresi')
    if direction == 'SHORT' and not (25 < b['rsi'] <= 50):
        return reject('short_rsi_filtresi')
    if a['vol_ratio'] < 0.65 or a['atr'] <= 0 or a['price'] <= 0:
        return reject('30m_hacim_veya_atr')
    atr_pct = a['atr'] / a['price']
    if not 0.0010 <= atr_pct <= 0.06:
        return reject('30m_volatilite_araligi')
    dist = abs(a['price'] - a['ema20']) / a['atr']
    if dist > 1.25:
        return reject('ema20den_asiri_uzak')

    # Closed 15M breakout/retest sequence; use 15M ATR for the retest tolerance.
    if len(c15) < 30 or trigger['atr'] <= 0:
        return reject('15m_yapi_verisi_yetersiz')
    base = c15[-24:-4]
    breakout_window = c15[-4:-1]
    last = c15[-1]
    if len(base) < 20 or len(breakout_window) < 3:
        return reject('kirilim_penceresi_yetersiz')
    level = max(r['high'] for r in base) if direction == 'LONG' else min(r['low'] for r in base)
    span = max(last['high'] - last['low'], 1e-12)
    close_loc = (last['close'] - last['low']) / span
    body = abs(last['close'] - last['open'])
    if direction == 'LONG':
        broke = any(r['close'] > level for r in breakout_window)
        retest = abs(last['low'] - level) <= trigger['atr'] * 0.55 and last['close'] >= level
        directional = close_loc >= 0.60 and last['close'] >= last['open']
        swing = min(r['low'] for r in c15[-8:])
    else:
        broke = any(r['close'] < level for r in breakout_window)
        retest = abs(last['high'] - level) <= trigger['atr'] * 0.55 and last['close'] <= level
        directional = close_loc <= 0.40 and last['close'] <= last['open']
        swing = max(r['high'] for r in c15[-8:])
    if not broke:
        return reject('kapanmis_kirilim_yok')
    if not retest:
        return reject('retest_dogrulanmadi')
    if not directional:
        return reject('tetik_mumu_yon_onayi_yok')
    if body > max(a['atr'] * 1.5, a['price'] * 0.025):
        return reject('tetik_mumu_asiri_buyuk')

    # CRITICAL FIX: entry and all TP/SL levels now share the actual 15M trigger close.
    entry = float(last['close'])
    risk = max(a['atr'] * 1.25, entry * 0.002)
    stop = min(entry - risk, swing - trigger['atr'] * 0.15) if direction == 'LONG' else max(entry + risk, swing + trigger['atr'] * 0.15)
    actual_risk = abs(entry - stop)
    if actual_risk <= 0 or actual_risk > a['atr'] * 3.0:
        return reject('stop_mesafesi_gecersiz')

    sign = 1 if direction == 'LONG' else -1
    tick = (record.get('meta') or {}).get('tickSize')
    if not tick or not finite(tick) or float(tick) <= 0:
        return reject('tick_size_eksik')
    entry = round_to_tick(entry, tick)
    stop = round_to_tick(stop, tick)
    tp1 = round_to_tick(entry + sign * actual_risk * 1.5, tick)
    tp2 = round_to_tick(entry + sign * actual_risk * 2.5, tick)
    tp3 = round_to_tick(entry + sign * actual_risk * 3.5, tick)
    if direction == 'LONG' and not (stop < entry < tp1 < tp2 < tp3):
        return reject('yuvarlama_sonrasi_seviyeler_gecersiz')
    if direction == 'SHORT' and not (stop > entry > tp1 > tp2 > tp3):
        return reject('yuvarlama_sonrasi_seviyeler_gecersiz')

    liquidity = record.get('liquidity') or {}
    positioning = liquidity.get('longShort')
    funding = liquidity.get('markFunding')
    score = 60
    if context4['trend'] == direction: score += 8
    if dist <= 0.45: score += 8
    elif dist <= 0.8: score += 5
    if a['vol_ratio'] >= 1.2: score += 6
    elif a['vol_ratio'] >= 0.9: score += 3
    if (close_loc >= 0.8 if direction == 'LONG' else close_loc <= 0.2): score += 4
    score = min(90, score)
    zone_pad = max(trigger['atr'] * 0.12, entry * 0.0005)
    zone = [round_to_tick(entry - zone_pad, tick), round_to_tick(entry + zone_pad, tick)]
    if zone[0] > zone[1]: zone.reverse()
    if diagnostics is not None:
        diagnostics['aday_olustu'] = diagnostics.get('aday_olustu', 0) + 1
    return {'direction': direction, 'entry': entry, 'entry_zone': zone,
            'sl': stop, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
            'confidence': score, 'confidence_type': 'heuristic_not_backtested',
            'entry_distance_atr': round(dist, 2), 'context_4h': context4['trend'],
            'positioning': positioning, 'funding': funding,
            'reason': f"4H bağlam {context4['trend']}; 30M ana yön {a['trend']} + 15M teyit {b['trend']}; 15M kırılım+retest; RSI {b['rsi']:.1f}; hacim {a['vol_ratio']:.2f}x; EMA20 uzaklığı {dist:.2f} ATR"}

def integrity(pool,symbols,server_ms=None):
    missing=[]; bad=[]; usable=[]; optional_warnings=[]
    for sym in symbols:
        r=pool.get(sym)
        if not r: missing.append(sym); continue
        errors=list(r.get('errors') or [])
        if not valid_klines(r.get('klines',{}).get('4h',[]),server_ms): errors.append('invalid/stale 4h candles')
        if not valid_klines(r.get('klines',{}).get('30m',[]),server_ms): errors.append('invalid/stale 30m candles')
        if not valid_klines(r.get('klines',{}).get('15m',[]),server_ms): errors.append('invalid/stale 15m candles')
        if errors: bad.append((sym,errors))
        else:
            usable.append(sym)
            if r.get('warnings'): optional_warnings.append((sym,r['warnings']))
    return {'expected':len(symbols),'usable_count':len(usable),'complete':not missing and not bad,
            'missing_count':len(missing),'bad_count':len(bad),'missing':missing,'bad':bad,
            'optional_warning_count':len(optional_warnings),'optional_warnings':optional_warnings,'usable':usable}


async def main():
    timeout=aiohttp.ClientTimeout(total=TIMEOUT)
    connector=aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(timeout=timeout,connector=connector) as s:
        print('Semboller alınıyor...')
        info=await exchange_info(s)
        universe={}
        for x in info['symbols']:
            if x.get('status')=='TRADING' and x.get('quoteAsset')=='USDT' and x.get('contractType')=='PERPETUAL':
                pf=next((f for f in x.get('filters',[]) if f.get('filterType')=='PRICE_FILTER'),{})
                universe[x['symbol']]={'symbol':x['symbol'],'baseAsset':x.get('baseAsset'),'tickSize':pf.get('tickSize'),
                    'pricePrecision':x.get('pricePrecision'),'quantityPrecision':x.get('quantityPrecision')}
        symbols=sorted(universe); print(f'Aktif USDT perpetual: {len(symbols)}')
        ts=await tickers(s); tmap={x['symbol']:x for x in ts if x['symbol'] in universe}; pool={}
        for i in range(0,len(symbols),CHUNK):
            chunk=symbols[i:i+CHUNK]; print(f'Veri: {i+1}-{i+len(chunk)} / {len(symbols)}')
            results=await bounded(s,chunk,universe,tmap)
            for sym,r,e in results: pool[sym]=r if r else {'symbol':sym,'meta':universe[sym],'errors':[e]}
        # FIX: get fresh exchange time AFTER bulk collection; start-time clock may be stale by minutes.
        server_ms=await server_time(s)
        for rec in pool.values(): rec['exchange_server_time_ms']=server_ms
        local_ms=int(datetime.now(timezone.utc).timestamp()*1000)
        clock_skew_ms=server_ms-local_ms  # diagnostic only; exchange time remains candle authority
        check=integrity(pool,symbols,server_ms)
        snapshot={'project':'VS AL TRADER V4','collected_at':now(),'exchange_server_time_ms':server_ms,
                  'universe':'Binance USDⓈ-M USDT PERPETUAL','symbol_count':len(symbols),'symbols':symbols,'integrity':check,'pool':pool}
        with open('vs_al_pool.json','w',encoding='utf-8') as f: json.dump(snapshot,f,ensure_ascii=False,indent=2)
        signals=[]; skipped=0; rejection_counts={}
        # Hatalı tek parite diğer sağlıklı pariteleri durdurmaz; her eleme nedeni raporlanır.
        for sym in check['usable']:
            try:
                local_reasons={}
                z=analyze_vs_al(pool[sym], local_reasons)
                for reason,count in local_reasons.items(): rejection_counts[reason]=rejection_counts.get(reason,0)+count
                if z: signals.append({'symbol':sym,**z})
            except Exception as e:
                skipped+=1; print(f'Analiz hatası {sym}: {e}')
        signals.sort(key=lambda x:x.get('confidence',0),reverse=True)
        report={'project':'VS AL TRADER V4','created_at':now(),'exchange_server_time_ms':server_ms,
                'clock_skew_ms_diagnostic_only':clock_skew_ms,'integrity':check,'signal_count':len(signals),
                'analysis_errors':skipped,'rejection_counts':rejection_counts,'signals':signals,
                'confidence_note':'Heuristik kalite puanıdır; backtest ile kalibre edilmiş kazanma olasılığı değildir.'}
        with open('vs_al_scan_report.json','w',encoding='utf-8') as f: json.dump(report,f,ensure_ascii=False,indent=2)
        print('\nSONUÇ')
        print('Parite:',len(symbols),' Kullanılabilir:',check['usable_count'],' Veri hatalı:',check['bad_count'],' Aday:',len(signals))
        print('Eleme nedenleri:',json.dumps(rejection_counts,ensure_ascii=False,sort_keys=True))

if __name__=='__main__': asyncio.run(main())
