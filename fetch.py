#!/usr/bin/env python3
"""牧之研究：每天下載證交所、櫃買中心、期交所的公開資料。

只負責下載與保存原始回應（gzip 壓縮），解析在牧之研究的建置流程裡做。
用法：
  python3 fetch.py daily            抓最近幾個交易日還沒有的資料
  python3 fetch.py backfill 250     回補最近 250 個平日（證交所、櫃買中心有日期參數的資料）
  python3 fetch.py probe            測試候選網址，結果寫到 probe/（找網址格式用）
只用 Python 標準函式庫。
"""
import datetime as dt
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(ROOT, 'raw')
TW = dt.timezone(dt.timedelta(hours=8))
UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
KEEP_DAYS = 420
GAP = 3.0  # 同一個網站兩次請求之間至少間隔幾秒
GAP_HOST = {'www.twse.com.tw': 4.0}  # 證交所比較嚴格
MAX_FAIL = 6  # 同一個網站連續失敗幾次就放棄這一輪（避免被封鎖時空等）

# 有日期參數、可以回補的資料集（{ymd}=20261120、{ymds}=2026/11/20、{roc}=115/11/20）
DATED = {
    'twse_margin': 'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={ymd}&selectType=ALL&response=json',
    'twse_sbl': 'https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?date={ymd}&response=json',
    'twse_qfii': 'https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?date={ymd}&selectType=ALLBUT0999&response=json',
}
# 只有最新一天的資料集（日期從內容判斷）
LATEST = {
    'taifex_inst_fo': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate',
    'taifex_inst_fut': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate',
    'taifex_inst_cp': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate',
    'taifex_pcr': 'https://openapi.taifex.com.tw/v1/PutCallRatio',
}
# 找網址格式用的候選
PROBE = [
    ('twse_margin', DATED['twse_margin'], None),
    ('twse_sbl', DATED['twse_sbl'], None),
    ('twse_qfii', DATED['twse_qfii'], None),
    ('tpex_margin_www', 'https://www.tpex.org.tw/www/zh-tw/margin/balance?date={ymds}&response=json', None),
    ('tpex_margin_www_post', 'https://www.tpex.org.tw/www/zh-tw/margin/balance', {'date': '{ymds}', 'response': 'json'}),
    ('tpex_margin_legacy', 'https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc}', None),
    ('tpex_sbl_www', 'https://www.tpex.org.tw/www/zh-tw/margin/sbl?date={ymds}&response=json', None),
    ('tpex_sbl_legacy', 'https://www.tpex.org.tw/web/stock/margin_trading/margin_sbl/margin_sbl_result.php?l=zh-tw&o=json&d={roc}', None),
    ('tpex_qfii_www', 'https://www.tpex.org.tw/www/zh-tw/insti/qfii?date={ymds}&response=json', None),
    ('tpex_qfii_legacy', 'https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php?l=zh-tw&o=json&d={roc}', None),
    ('tpex_openapi_swagger', 'https://www.tpex.org.tw/openapi/swagger.json', None),
    ('twse_openapi_swagger', 'https://openapi.twse.com.tw/v1/swagger.json', None),
    ('twse_openapi_margin', 'https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN', None),
    ('taifex_swagger', 'https://openapi.taifex.com.tw/swagger.json', None),
    ('taifex_swagger_v1', 'https://openapi.taifex.com.tw/v1/swagger.json', None),
    ('tpex_margin_www_post_id', 'https://www.tpex.org.tw/www/zh-tw/margin/balance', {'date': '{ymds}', 'id': '', 'response': 'json'}),
    ('tpex_quotes_www_post', 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQ', {'date': '{ymds}', 'type': 'EW', 'response': 'json'}),
    ('taifex_inst_fo', LATEST['taifex_inst_fo'], None),
    ('taifex_inst_fut', LATEST['taifex_inst_fut'], None),
    ('taifex_pcr', LATEST['taifex_pcr'], None),
    ('taifex_fut_csv', 'https://www.taifex.com.tw/cht/3/futContractsDateDown', {'queryStartDate': '{ymds_m30}', 'queryEndDate': '{ymds}', 'commodityId': 'TXF'}),
]

_last = {}
_fail = {}


def host_of(url):
    return urllib.parse.urlparse(url).netloc


def polite(url):
    host = host_of(url)
    wait = GAP_HOST.get(host, GAP) - (time.time() - _last.get(host, 0))
    if wait > 0:
        time.sleep(wait)
    _last[host] = time.time()


def blocked(url, ds):
    return _fail.get(host_of(url), 0) >= MAX_FAIL or _fail.get(ds, 0) >= MAX_FAIL


def mark(url, ds, good):
    for k in (host_of(url), ds):
        _fail[k] = 0 if good else _fail.get(k, 0) + 1


def fetch(url, form=None, timeout=40, tries=3):
    """回傳 (HTTP 狀態碼或 None, 內容 bytes, content-type)"""
    data = urllib.parse.urlencode(form).encode() if form else None
    err = b''
    for k in range(tries):
        polite(url)
        try:
            req = urllib.request.Request(url, data=data, headers={'User-Agent': UA, 'Accept': 'application/json, text/csv, */*', 'Accept-Language': 'zh-TW,zh;q=0.9'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read(), r.headers.get('Content-Type', '')
        except urllib.error.HTTPError as e:
            body = e.read() or b''
            if e.code in (400, 403, 404, 405):
                return e.code, body, e.headers.get('Content-Type', '') if e.headers else ''
            err = f'HTTP {e.code}'.encode() + b' ' + body[:200]
        except Exception as e:  # 連線逾時、DNS、SSL 等
            err = repr(e).encode()
        time.sleep(5 * (k + 1))
    return None, err, ''


def fmt(s, day):
    roc = f'{day.year - 1911}/{day:%m/%d}'
    m30 = day - dt.timedelta(days=30)
    return s.replace('{ymd}', f'{day:%Y%m%d}').replace('{ymds}', f'{day:%Y/%m/%d}').replace('{roc}', roc).replace('{ymds_m30}', f'{m30:%Y/%m/%d}')


def path(ds, date_iso):
    return os.path.join(RAW, ds, f'{date_iso}.json.gz')


def save(ds, date_iso, body):
    os.makedirs(os.path.join(RAW, ds), exist_ok=True)
    with gzip.open(path(ds, date_iso), 'wb', compresslevel=9) as f:
        f.write(body)


def twse_ok(body):
    """證交所 rwd 回應：stat 為 OK 而且有表格資料才算成功"""
    try:
        j = json.loads(body.decode('utf-8'))
    except Exception:
        return False, 'not json'
    st = str(j.get('stat', ''))
    if st.upper() != 'OK':
        return False, st[:60]
    rows = sum(len(t.get('data') or []) for t in (j.get('tables') or [])) + len(j.get('data') or [])
    return rows > 0, f'rows={rows}'


def weekdays_back(n, start=None):
    d = start or dt.datetime.now(TW).date()
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return out


def latest_date_of(body):
    """期交所 OpenAPI 回應裡的日期（Date 欄，格式 20261120 或 2026/11/20）"""
    try:
        j = json.loads(body.decode('utf-8-sig'))
    except Exception:
        return None
    if isinstance(j, list) and j and isinstance(j[0], dict):
        v = str(j[0].get('Date') or j[0].get('日期') or '')
        v = v.replace('/', '').replace('-', '')
        if len(v) == 8 and v.isdigit():
            return f'{v[:4]}-{v[4:6]}-{v[6:]}'
    return None


def run_dated(days, log):
    now = dt.datetime.now(TW)
    for day in days:
        iso = day.isoformat()
        if day == now.date() and now.hour < 15:  # 當天下午三點前資料還沒出來
            continue
        for ds, tpl in DATED.items():
            if os.path.exists(path(ds, iso)):
                continue
            url = fmt(tpl, day)
            if blocked(url, ds):
                log.append({'ds': ds, 'date': iso, 'ok': False, 'info': 'skipped: failing'})
                continue
            st, body, ct = fetch(url, tries=2)
            mark(url, ds, st == 200)
            ok, info = twse_ok(body) if st == 200 else (False, f'HTTP {st} {body[:80]!r}')
            log.append({'ds': ds, 'date': iso, 'ok': ok, 'info': info})
            if ok:
                save(ds, iso, body)


def run_latest(log):
    for ds, url in LATEST.items():
        if blocked(url, ds):
            log.append({'ds': ds, 'date': None, 'ok': False, 'info': 'skipped: failing'})
            continue
        st, body, ct = fetch(url)
        mark(url, ds, st == 200)
        d = latest_date_of(body) if st == 200 else None
        log.append({'ds': ds, 'date': d, 'ok': bool(d), 'info': f'HTTP {st} {len(body)}B'})
        if d and not os.path.exists(path(ds, d)):
            save(ds, d, body)


def prune():
    cut = (dt.datetime.now(TW).date() - dt.timedelta(days=KEEP_DAYS)).isoformat()
    for ds in os.listdir(RAW) if os.path.isdir(RAW) else []:
        for fn in os.listdir(os.path.join(RAW, ds)):
            if fn[:10] < cut:
                os.remove(os.path.join(RAW, ds, fn))


def manifest(log):
    m = {'updated': dt.datetime.now(TW).strftime('%Y-%m-%d %H:%M'), 'datasets': {}, 'lastRun': log[-80:]}
    for ds in sorted(os.listdir(RAW)) if os.path.isdir(RAW) else []:
        ds_dir = os.path.join(RAW, ds)
        if not os.path.isdir(ds_dir):
            continue
        dates = sorted(fn[:10] for fn in os.listdir(ds_dir) if fn.endswith('.json.gz'))
        m['datasets'][ds] = {'n': len(dates), 'first': dates[0] if dates else None, 'last': dates[-1] if dates else None}
    with open(os.path.join(RAW, 'manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(m, f, ensure_ascii=False, indent=1)


def probe():
    out = os.path.join(ROOT, 'probe')
    os.makedirs(out, exist_ok=True)
    day = weekdays_back(2)[1] if dt.datetime.now(TW).hour < 17 else weekdays_back(1)[0]
    summary = []
    for name, tpl, form in PROBE:
        url = fmt(tpl, day)
        f2 = {k: fmt(v, day) for k, v in form.items()} if form else None
        t0 = time.time()
        st, body, ct = fetch(url, f2, tries=1)
        txt = body[:6000]
        try:
            txt = txt.decode('utf-8')
        except UnicodeDecodeError:
            txt = body[:6000].decode('big5', 'replace')
        summary.append({'name': name, 'url': url, 'form': f2, 'status': st, 'type': ct, 'bytes': len(body), 'sec': round(time.time() - t0, 1)})
        with open(os.path.join(out, f'{name}.txt'), 'w', encoding='utf-8') as f:
            f.write(f'URL: {url}\nFORM: {f2}\nSTATUS: {st}\nTYPE: {ct}\nBYTES: {len(body)}\n\n{txt}')
    with open(os.path.join(out, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump({'day': day.isoformat(), 'at': dt.datetime.now(TW).strftime('%Y-%m-%d %H:%M'), 'results': summary}, f, ensure_ascii=False, indent=1)
    print(json.dumps(summary, ensure_ascii=False, indent=1))


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'daily'
    n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 0
    if mode == 'probe':
        probe()
        return
    log = []
    run_dated(weekdays_back(n if mode == 'backfill' and n > 0 else 6), log)
    run_latest(log)
    prune()
    manifest(log)
    ok = sum(1 for x in log if x['ok'])
    print(f'{mode}: {ok}/{len(log)} ok')
    miss = [x for x in log if not x['ok']]
    for x in miss[:40]:
        print('  miss', x)
    if len(miss) > 40:
        print(f'  ……另有 {len(miss) - 40} 筆')


if __name__ == '__main__':
    main()
