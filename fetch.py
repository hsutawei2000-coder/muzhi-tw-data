#!/usr/bin/env python3
"""牧之研究：每天下載證交所、櫃買中心、期交所的公開資料。

只負責下載與保存原始回應（gzip 壓縮），解析在牧之研究的建置流程裡做。
用法：
  python3 fetch.py daily            抓最近幾個交易日還沒有的資料
  python3 fetch.py backfill 250     回補最近 250 個平日（證交所、櫃買中心有日期參數的資料）
  python3 fetch.py probe            測試候選網址，結果寫到 probe/（找網址格式用）
除權除息結果表從 2010 年起每月一檔長期保存；證交所除權明細每輪補 EXRD_MAX 筆。
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
    'tpex_margin': 'https://www.tpex.org.tw/www/zh-tw/margin/balance?date={ymds}&response=json',
    'tpex_sbl': 'https://www.tpex.org.tw/www/zh-tw/margin/sbl?date={ymds}&response=json',
    'tpex_qfii': 'https://www.tpex.org.tw/www/zh-tw/insti/qfii?date={ymds}&response=json',
}
# 期交所歷史資料（一次下載一個月，存成 raw/<資料集>/<YYYY-MM>.csv.gz；當月每次重抓）
MONTHLY = {
    'taifex_txf': ('https://www.taifex.com.tw/cht/3/futContractsDateDown', {'queryStartDate': '{m0}', 'queryEndDate': '{m1}', 'commodityId': 'TXF'}),
    'taifex_pcr_m': ('https://www.taifex.com.tw/cht/3/pcRatioDown', {'queryStartDate': '{m0}', 'queryEndDate': '{m1}'}),
}
# 只有最新一天的資料集（日期從內容判斷）
LATEST = {
    'taifex_inst_fo': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate',
    'taifex_inst_fut': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate',
    'taifex_inst_cp': 'https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate',
    'taifex_pcr': 'https://openapi.taifex.com.tw/v1/PutCallRatio',
}
# 除權除息計算結果表：一次抓一個月，存成 raw/<資料集>/<YYYY-MM>.json.gz，從 EXR_FROM 起長期保存
# {a}{b}=20250601、20250630；{as}{bs}=2025/06/01、2025/06/30
EXR = {
    'twse_exr': 'https://www.twse.com.tw/rwd/zh/exRight/TWT49U?startDate={a}&endDate={b}&response=json',
    'tpex_exr': 'https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ?startDate={as}&endDate={bs}&response=json',
}
EXR_FROM = (2010, 1)
# 證交所的結果表只有「權值+息值」：除權、權息要另外查明細，才分得出現金股利、無償配股與現金增資
EXRD_URL = 'https://www.twse.com.tw/rwd/zh/exRight/TWT49UDetail?STK_NO={code}&T1={ymd}&response=json'
EXRD_MAX = 600  # 每一輪最多補幾筆明細（回補舊資料分幾輪做）
EXRD_SEC = 35 * 60  # 每一輪補明細最多花幾秒，避免拖到其他資料存檔的時間
# 只保留最新一份的資料（存成 raw/<資料集>/latest.json.gz，內容沒變就不重寫）
SNAP = {
    'twse_exr_pre': 'https://www.twse.com.tw/rwd/zh/exRight/TWT48U?response=json',   # 上市除權除息預告
    'tpex_exr_pre': 'https://www.tpex.org.tw/openapi/v1/tpex_exright_prepost',        # 上櫃除權除息預告
    'twse_divpol': 'https://openapi.twse.com.tw/v1/opendata/t187ap45_L',              # 上市公司股利分派情形
    'tpex_divpol': 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap39_O',           # 上櫃股利分派情形（董事會通過）
}
SNAP_DROP = ('摘錄公司章程',)  # 股利分派情形裡的章程全文很長，存檔前拿掉
KEEP_ALL = {'twse_exr', 'tpex_exr', 'twse_exrd'} | set(SNAP)  # 不受 KEEP_DAYS 刪除

# 找網址格式用的候選（第四欄：要存多少位元組，預設 6000）
PROBE = [
    ('tpex_exr_pre', SNAP['tpex_exr_pre'], None, 6000),
    ('tpex_divpol', SNAP['tpex_divpol'], None, 6000),
    ('twse_t49u_d2012', 'https://www.twse.com.tw/rwd/zh/exRight/TWT49UDetail?STK_NO=2610&T1=20120109&response=json', None, 3000),
    ('tpex_exr_2010', 'https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ?startDate=2010/07/01&endDate=2010/07/31&response=json', None, 3000),
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
    m30, m365 = day - dt.timedelta(days=30), day - dt.timedelta(days=365)
    s = s.replace('{today}', f'{dt.datetime.now(TW):%Y/%m/%d}')
    return (s.replace('{ymd}', f'{day:%Y%m%d}').replace('{ymds}', f'{day:%Y/%m/%d}').replace('{roc}', roc)
            .replace('{ymds_m30}', f'{m30:%Y/%m/%d}').replace('{ymds_m365}', f'{m365:%Y/%m/%d}'))


def path(ds, key, ext='json'):
    return os.path.join(RAW, ds, f'{key}.{ext}.gz')


def save(ds, key, body, ext='json'):
    os.makedirs(os.path.join(RAW, ds), exist_ok=True)
    with gzip.open(path(ds, key, ext), 'wb', compresslevel=9) as f:
        f.write(body)


def load_raw(ds, key, ext='json'):
    try:
        with gzip.open(path(ds, key, ext), 'rb') as f:
            return f.read()
    except Exception:
        return None


def save_same(ds, key, body, ext='json'):
    """內容和已存的一樣就不重寫；gzip 不記時間，同樣內容存出來的檔案也一樣，git 才不會每次都有變更"""
    if load_raw(ds, key, ext) == body:
        return False
    os.makedirs(os.path.join(RAW, ds), exist_ok=True)
    with open(path(ds, key, ext), 'wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=9, mtime=0) as f:
            f.write(body)
    return True


def resp_date(j):
    """回應裡的資料日期（YYYYMMDD）：最外層 date，或第一個表格的 date（民國年或西元年）"""
    cands = [j.get('date')] + [t.get('date') for t in (j.get('tables') or []) if isinstance(t, dict)][:1]
    for v in cands:
        v = str(v or '').strip().replace('-', '/')
        if len(v) == 8 and v.isdigit():
            return v
        p = v.split('/')
        if len(p) == 3 and all(x.isdigit() for x in p):
            y = int(p[0]) + (1911 if int(p[0]) < 1911 else 0)
            return f'{y:04d}{int(p[1]):02d}{int(p[2]):02d}'
    return None


def tw_ok(body, ymd):
    """證交所、櫃買中心回應：有 stat 時必須是 OK，資料日期要等於查詢日期，而且有表格資料"""
    try:
        j = json.loads(body.decode('utf-8-sig'))
    except Exception:
        return False, 'not json'
    if 'stat' in j and str(j.get('stat', '')).upper() != 'OK':
        return False, str(j.get('stat'))[:60]
    d = resp_date(j)
    if d and d != ymd:
        return False, f'date {d}'
    rows = sum(len(t.get('data') or []) for t in (j.get('tables') or []) if isinstance(t, dict)) + len(j.get('data') or [])
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
    """期交所 OpenAPI 回應裡的日期：JSON 的 Date 欄，或 CSV 第一筆資料的第一欄（20261120 或 2026/11/20）"""
    txt = body.decode('utf-8-sig', 'replace')
    v = ''
    try:
        j = json.loads(txt)
        if isinstance(j, list) and j and isinstance(j[0], dict):
            v = str(j[0].get('Date') or j[0].get('日期') or '')
    except Exception:
        lines = [x for x in txt.splitlines() if x.strip()]
        if len(lines) >= 2:
            v = lines[1].split(',')[0]
    v = v.replace('/', '').replace('-', '').strip()
    if len(v) == 8 and v.isdigit():
        return f'{v[:4]}-{v[4:6]}-{v[6:]}'
    return None


def months_back(n, today):
    """最近 n 個月（含本月）：[(YYYY-MM, 月初, 月底或今天)]"""
    out, y, m = [], today.year, today.month
    for _ in range(n):
        first = dt.date(y, m, 1)
        nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
        out.append((f'{y:04d}-{m:02d}', first, min(nxt - dt.timedelta(days=1), today)))
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return out


def csv_ok(body):
    """期交所下載的 CSV（Big5／MS950）：第一列是欄位，至少一筆資料"""
    try:
        txt = body.decode('cp950')
    except UnicodeDecodeError:
        return False, 'not cp950'
    lines = [x for x in txt.splitlines() if x.strip()]
    if len(lines) < 2 or ',' not in lines[0] or '<' in lines[0]:
        return False, (lines[0][:60] if lines else 'empty')
    return True, f'rows={len(lines) - 1}'


def csv_last_date(p):
    """已存的月檔裡最新的資料日期（date）；有的檔案由新到舊排，所以取最大值。讀不到就回傳 None"""
    try:
        with gzip.open(p, 'rb') as f:
            lines = [x for x in f.read().decode('cp950').splitlines() if x.strip()][1:]
        ds = []
        for x in lines:
            try:
                ds.append(dt.date(*map(int, x.split(',')[0].strip().split('/'))))
            except ValueError:
                pass
        return max(ds) if ds else None
    except Exception:
        return None


def run_monthly(n, log):
    today = dt.datetime.now(TW).date()
    for ds, (url, form) in MONTHLY.items():
        for key, d0, d1 in months_back(n, today):
            last = csv_last_date(path(ds, key, 'csv'))
            # 本月每次重抓；以前的月份，已存的檔案最後一天離月底超過 6 天（月中存的不完整檔）也重抓
            if key != f'{today:%Y-%m}' and last and last >= d1 - dt.timedelta(days=6):
                continue
            if blocked(url, ds):
                log.append({'ds': ds, 'date': key, 'ok': False, 'info': 'skipped: failing'})
                continue
            # 本月的結束日期可能還沒有資料（假日、收盤前），期交所會回錯誤頁：往前退幾天再試
            for back in (range(0, 5) if key == f'{today:%Y-%m}' else [0]):
                e = d1 - dt.timedelta(days=back)
                if e < d0:
                    break
                f2 = {k: v.replace('{m0}', f'{d0:%Y/%m/%d}').replace('{m1}', f'{e:%Y/%m/%d}') for k, v in form.items()}
                st, body, ct = fetch(url, f2, tries=2)
                mark(url, ds, st == 200)
                ok, info = csv_ok(body) if st == 200 else (False, f'HTTP {st}')
                if ok:
                    break
            log.append({'ds': ds, 'date': key, 'ok': ok, 'info': info})
            if ok:
                save(ds, key, body, 'csv')


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
            ok, info = tw_ok(body, f'{day:%Y%m%d}') if st == 200 else (False, f'HTTP {st} {body[:80]!r}')
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


def exr_months(today):
    """EXR_FROM 到本月：[(YYYY-MM, 月初, 月底或今天)]"""
    out, (y, m) = [], EXR_FROM
    while (y, m) <= (today.year, today.month):
        first = dt.date(y, m, 1)
        nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
        out.append((f'{y:04d}-{m:02d}', first, min(nxt - dt.timedelta(days=1), today)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def exr_ok(ds, body, d0, d1):
    """結果表回應：查詢區間要對得上；沒有除權息的月份算成功（存空表），其他錯誤算失敗。回傳 (成功, 筆數或原因)"""
    try:
        j = json.loads(body.decode('utf-8-sig'))
    except Exception:
        return False, 'not json'
    if ds == 'twse_exr':
        st = str(j.get('stat', ''))
        if st.upper() != 'OK':
            return ('沒有符合' in st), st[:40]
        want = f'{d0.year - 1911}年{d0:%m}月{d0:%d}日'
        if not str(j.get('title', '')).lstrip('0').startswith(want):   # 民國 99 年以前標題是 099年
            return False, 'title ' + str(j.get('title', ''))[:30]
        return True, len(j.get('data') or [])
    if str(j.get('date', '')) != f'{d0:%Y%m%d}~{d1:%Y%m%d}':
        return False, 'date ' + str(j.get('date', ''))[:30]
    t = (j.get('tables') or [{}])[0]
    return isinstance(t.get('data', []), list), len(t.get('data') or [])


def run_exr(log):
    today = dt.datetime.now(TW).date()
    months = exr_months(today)
    recent = {k for k, _, _ in months[-2:]}  # 本月、上個月每次重抓
    for ds, tpl in EXR.items():
        for key, d0, d1 in reversed(months):
            if key not in recent and os.path.exists(path(ds, key)):
                continue
            url = (tpl.replace('{as}', f'{d0:%Y/%m/%d}').replace('{bs}', f'{d1:%Y/%m/%d}')
                   .replace('{a}', f'{d0:%Y%m%d}').replace('{b}', f'{d1:%Y%m%d}'))
            if blocked(url, ds):
                log.append({'ds': ds, 'date': key, 'ok': False, 'info': 'skipped: failing'})
                continue
            st, body, ct = fetch(url, timeout=90, tries=2)
            mark(url, ds, st == 200)
            ok, info = exr_ok(ds, body, d0, d1) if st == 200 else (False, f'HTTP {st}')
            if key not in recent or not ok:
                log.append({'ds': ds, 'date': key, 'ok': ok, 'info': info})
            if ok:
                save_same(ds, key, body)


def run_exrd(log):
    """證交所除權、權息事件的明細：新到舊補，存成 raw/twse_exrd/<年>.json.gz（{"代號,日期": 明細列}）；
    查得到但沒有資料的記成 null，下次不再查"""
    want = []
    for fn in sorted(os.listdir(os.path.join(RAW, 'twse_exr')) if os.path.isdir(os.path.join(RAW, 'twse_exr')) else [], reverse=True):
        try:
            j = json.loads(gzip.open(os.path.join(RAW, 'twse_exr', fn)).read().decode('utf-8-sig'))
        except Exception:
            continue
        for row in reversed(j.get('data') or []):
            if len(row) > 11 and str(row[6]).strip() in ('權', '權息'):
                k = str(row[11]).replace(' ', '')
                if ',' in k and len(k.split(',')[1]) == 8:
                    want.append(k)
    books, todo = {}, []
    for k in want:
        y = k.split(',')[1][:4]
        if y not in books:
            b = load_raw('twse_exrd', y)
            books[y] = json.loads(b) if b else {'fields': None, 'rows': {}}
        if k not in books[y]['rows']:
            todo.append(k)
    done, t0 = 0, time.time()
    for k in todo[:EXRD_MAX]:
        if time.time() - t0 > EXRD_SEC:
            break
        code, ymd = k.split(',')
        url = EXRD_URL.format(code=urllib.parse.quote(code), ymd=ymd)
        if blocked(url, 'twse_exrd'):
            log.append({'ds': 'twse_exrd', 'date': ymd, 'ok': False, 'info': 'skipped: failing'})
            break
        st, body, ct = fetch(url, tries=2)
        mark(url, 'twse_exrd', st == 200)
        if st != 200:
            log.append({'ds': 'twse_exrd', 'date': ymd, 'ok': False, 'info': f'{code} HTTP {st}'})
            continue
        try:
            j = json.loads(body.decode('utf-8-sig'))
        except Exception:
            log.append({'ds': 'twse_exrd', 'date': ymd, 'ok': False, 'info': f'{code} not json'})
            continue
        bk = books[ymd[:4]]
        rows = j.get('data') or []
        if str(j.get('stat', '')).lower() == 'ok' and rows and str(rows[0][0]).strip() == code:
            bk['fields'] = bk['fields'] or j.get('fields')
            bk['rows'][k] = rows[0]
            done += 1
        elif str(j.get('stat', '')).lower() in ('ok', '') or '沒有' in str(j.get('stat', '')):
            bk['rows'][k] = None
        else:
            log.append({'ds': 'twse_exrd', 'date': ymd, 'ok': False, 'info': f"{code} {str(j.get('stat'))[:40]}"})
    for y, bk in books.items():
        save_same('twse_exrd', y, json.dumps(bk, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode())
    left = sum(1 for k in todo if k not in books[k.split(',')[1][:4]]['rows'])
    log.append({'ds': 'twse_exrd', 'date': None, 'ok': True, 'info': f'new={done} todo={len(todo)} left={left}'})


def run_snap(log):
    for ds, url in SNAP.items():
        if blocked(url, ds):
            log.append({'ds': ds, 'date': None, 'ok': False, 'info': 'skipped: failing'})
            continue
        st, body, ct = fetch(url, timeout=90)
        mark(url, ds, st == 200)
        ok, info = False, f'HTTP {st}'
        if st == 200:
            try:
                j = json.loads(body.decode('utf-8-sig'))
                if isinstance(j, list):
                    j = [{k: v for k, v in x.items() if not any(k.startswith(d) for d in SNAP_DROP)} if isinstance(x, dict) else x for x in j]
                    ok, info = len(j) > 0, f'rows={len(j)}'
                else:
                    ok = str(j.get('stat', 'OK')).upper() == 'OK'
                    info = f"rows={len(j.get('data') or [])}"
                if ok:
                    save_same(ds, 'latest', json.dumps(j, ensure_ascii=False, separators=(',', ':')).encode())
            except Exception as e:
                info = 'bad json ' + repr(e)[:40]
        log.append({'ds': ds, 'date': None, 'ok': ok, 'info': info})


def prune():
    cut = (dt.datetime.now(TW).date() - dt.timedelta(days=KEEP_DAYS)).isoformat()
    for ds in os.listdir(RAW) if os.path.isdir(RAW) else []:
        if ds in KEEP_ALL:
            continue
        for fn in os.listdir(os.path.join(RAW, ds)) if os.path.isdir(os.path.join(RAW, ds)) else []:
            if fn.endswith('.gz') and fn.split('.')[0] < cut[:len(fn.split('.')[0])]:
                os.remove(os.path.join(RAW, ds, fn))


def manifest(log):
    m = {'updated': dt.datetime.now(TW).strftime('%Y-%m-%d %H:%M'), 'datasets': {}, 'lastRun': log[-80:]}
    for ds in sorted(os.listdir(RAW)) if os.path.isdir(RAW) else []:
        ds_dir = os.path.join(RAW, ds)
        if not os.path.isdir(ds_dir):
            continue
        dates = sorted(fn.split('.')[0] for fn in os.listdir(ds_dir) if fn.endswith('.gz'))
        m['datasets'][ds] = {'n': len(dates), 'first': dates[0] if dates else None, 'last': dates[-1] if dates else None}
    with open(os.path.join(RAW, 'manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(m, f, ensure_ascii=False, indent=1)


def probe():
    out = os.path.join(ROOT, 'probe')
    os.makedirs(out, exist_ok=True)
    day = weekdays_back(2)[1] if dt.datetime.now(TW).hour < 17 else weekdays_back(1)[0]
    summary = []
    for item in PROBE:
        name, tpl, form = item[:3]
        cap = item[3] if len(item) > 3 else 6000
        url = fmt(tpl, day)
        f2 = {k: fmt(v, day) for k, v in form.items()} if form else None
        t0 = time.time()
        st, body, ct = fetch(url, f2, tries=1)
        txt = body[:cap]
        try:
            txt = txt.decode('utf-8')
        except UnicodeDecodeError:
            txt = body[:cap].decode('big5', 'replace')
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
    run_monthly(13 if mode == 'backfill' else 2, log)
    run_latest(log)
    run_snap(log)
    run_exr(log)
    run_exrd(log)
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
