# muzhi-tw-data

「牧之研究」網站用的台股公開資料，由 GitHub Actions 每個交易日自動下載，保存官方網站的原始回應（gzip 壓縮）。

| 資料夾 | 內容 | 來源 |
|---|---|---|
| `raw/twse_margin` | 上市股票融資融券餘額（MI_MARGN） | 臺灣證券交易所 |
| `raw/twse_sbl` | 上市股票融券、借券賣出餘額（TWT93U） | 臺灣證券交易所 |
| `raw/twse_qfii` | 上市股票外資及陸資持股比率（MI_QFIIS） | 臺灣證券交易所 |
| `raw/tpex_margin` | 上櫃股票融資融券餘額 | 證券櫃檯買賣中心 |
| `raw/tpex_sbl` | 上櫃股票融券、借券賣出餘額 | 證券櫃檯買賣中心 |
| `raw/tpex_qfii` | 上櫃股票僑外資及陸資持股比例 | 證券櫃檯買賣中心 |
| `raw/taifex_txf` | 臺股期貨三大法人交易與未平倉（每月一檔 CSV，MS950 編碼） | 臺灣期貨交易所 |
| `raw/taifex_pcr_m` | 臺指選擇權 Put/Call 比（每月一檔 CSV，MS950 編碼） | 臺灣期貨交易所 |
| `raw/taifex_*` | 三大法人期貨、選擇權交易與未平倉，臺指選擇權 Put/Call 比 | 臺灣期貨交易所 OpenAPI |

`raw/manifest.json` 記錄每個資料集有哪些日期與最近一次執行結果，`probe/` 是測試網址時的回應樣本。資料內容的權利屬於各交易所，一切以官方公布為準。

執行方式：排程在台北時間平日 17:40、22:20、23:50 與隔天 06:05 各跑一次；`fetch.py` 或工作流程有更新時會先測試網址，再依 `backfill.txt` 的天數回補；也可以在 Actions 頁面手動執行（daily、backfill、probe）。
