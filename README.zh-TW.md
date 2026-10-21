# Candle Audit Kit：K 線核對工具

[English README](README.md)

目前版本為 **0.1.0rc1 本地預發布版，已正式採用 MIT**。使用者已確認原程式、測試及樣例
由其指揮 Codex 開發，並同意採用 MIT。以「Candle Audit Kit contributors」署名，
不加入未確認的個人真名或郵件。第三方依賴保持原許可；本版作為首次公開預發布；驗證紀錄見 `docs/validation.md`。

## 有什麼用途

檢查 K 線是否缺漏、重複、未收棒或數量單位不符；把 CSV 與原始 JSON 回應逐項對照。
保留原始小數文字、資料版本及修訂紀錄，產生可離線查看的中文報告。
支援 OKX 現貨與線性 USDT 永續合約的成交 K 線；不下單、不使用帳戶或憑證。
「核對一致」不代表已證明是真實交易所資料。

## 安裝

需要 CPython 3.11–3.14，使用 macOS 或 Linux；不宣稱支援 Windows。
原生套件可能另有最低作業系統要求。實際驗證環境與套件版本見
`requirements-test-lock.txt`。請在解壓後的資料夾執行：

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install --index-url https://pypi.org/simple -r requirements-test-lock.txt
python -m pip install --no-build-isolation --no-deps .
python -m pip check
candle-audit --help
```

第一次安裝會從 PyPI 下載依賴，只寫入這個隔離環境，不需全域安裝。
鎖定檔也包含測試與打包工具；只需使用功能時可安裝 `python -m pip install .`。

## 跑完整離線範例

```sh
mkdir demo-workspace
cd demo-workspace
candle-audit demo --root data/demo
```

預期回傳 `SYNTHETIC_DEMO_COMPLETE`、`rows: 8`、`aggregate_rows: 2`、
`network_used: false`。結果會列出中文 HTML 報告的完整位置，直接用瀏覽器打開即可。

範例提供八根人工 K 線及假設的合約規則，會實際執行保存、查詢、四小時聚合和報告。
完整 CSV、原始 JSON、metadata、source、request 及 pages 輸入位於 `data/demo/inputs/`；
原始合成範例也放在 `examples/synthetic/`。不下載真實行情，不產生帳戶憑證。
若你改過範例輸入，重跑不會默默覆寫它。

## 資料放哪裡

預設以目前資料夾作為工作區，不必放在程式安裝位置，也不需要其他工程設定。
可用 `CANDLE_AUDIT_WORKSPACE` 指定已存在、沒有符號連結的絕對工作區路徑。
所有使用者輸入與輸出限於其中 `data/` 或 `.tmp-data-layer/`，不接受越界路徑及不安全檔案。
讀取不修改既有資料版本，但會在 `.tmp-data-layer/` 暫存核對資料，完成或失敗後清理；
因此工作區必須可寫。請建立獨立工作區。

```sh
candle-audit providers
candle-audit ingest --help
candle-audit read --help
candle-audit report --help
```

更多參數見 `docs/data_layer_m1.md`、`docs/data_layer_full.md`；限制見 `docs/limitations.md`。
公開行情下載另需明確授權與限額；本候選版驗證不用它，也不宣稱已證實真實歷史資料可取得。

## 測試與打包

回到解壓後的原始碼根目錄，啟用上述隔離環境，再執行：

```sh
python -B -m pytest -q
python -m build --no-isolation
```

測試使用合成資料並阻擋網路連線。完整隨機測試保留每項 1000 個例子的設定，可能耗時。
開發時可執行 `python -B -m pytest -q -m 'not property'`，但這不能替代完整驗收。
測試只在受控的 `.tmp-data-layer/` 建立暫存，不會為了通過而放寬正式路徑保護。
有三個舊版產物相容性案例因未提供原始歷史產物而明確跳過；另有三個真實的目前格式
搬移測試，核對搬移後資料及語義不變，不冒充缺失的歷史相容性證據。

## 授權及發布狀態

`PROVENANCE.md` 列出來源可核實事項及未知項目；`THIRD_PARTY_NOTICES.md` 與
`licenses/third-party/` 保存依賴的授權及原著作權文字。
正式自有程式許可見 `LICENSE`，確認紀錄見 `LICENSE-STATUS.md`。
使用者的自有程式確認不會改授權第三方依賴；各原文條款與歸屬繼續保留。
本地測試、獨立安裝與開源授權也都不保證任何活動申請獲批。
