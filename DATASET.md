# PMDA データセット取得（OTC / 医療用）

このリポジトリには、PMDA の以下検索ページから  
製品情報と成分情報を収集するスクリプトを追加しています。

- 一般用医薬品・要指導医薬品 (`otcSearch`)
- 医療用医薬品 (`iyakuSearch`)

## スクリプト

- `scripts/fetch_pmda_otc_dataset.py`
- `scripts/fetch_pmda_iyaku_dataset.py`

## 実行例

```bash
python3 scripts/fetch_pmda_otc_dataset.py --max-products 300 --output-dir data
python3 scripts/fetch_pmda_iyaku_dataset.py --from-date 20100101 --to-date 20260213 --output-dir data
```

## 出力ファイル

- `data/pmda_otc_products.json`
  - 製品コード、製品名、製造販売業者、薬効分類、リスク区分、剤形
  - 成分分量テキスト
  - 抽出した成分一覧（name / amount）
  - 参照元 URL（HTML / GeneralList / PDF）
- `data/pmda_otc_ingredient_index.json`
  - 成分名ごとの製品逆引きインデックス
- `data/pmda_iyaku_products.json`
  - 一般名、販売名（ゾロ含む）、製造販売業者
  - 文書有無（PDF/HTML/XML）と更新日
  - 一般名から分解した成分候補
- `data/pmda_iyaku_ingredient_index.json`
  - 医療用由来の成分名逆引きインデックス
- `data/jpic_compatible_schema.json`
  - アプリ内で使用するJPIC互換スキーマ定義

## 収集ロジック概要

### OTC (`fetch_pmda_otc_dataset.py`)

1. PMDA の候補リスト (`list_n.lib`) から製品名の先頭文字を収集
2. 先頭文字で検索（前方一致）し、ページング API で結果を巡回
3. 一意な製品コードを集約
4. 各製品の HTML 詳細ページから `成分分量` を抽出
5. 成分名と分量を正規表現ベースで抽出し JSON 化

### 医療用 (`fetch_pmda_iyaku_dataset.py`)

1. `iyakuSearch` を「販売名のみ・前方一致」で検索
2. 更新日レンジを再帰分割して、検索件数 1000 件上限を回避
3. 各レンジで `exportSearchResult/csv` を取得
4. CSV の一般名・販売名・製造販売業者を正規化して重複除去
5. 一般名から成分候補を分解し JSON 化

## アプリ側スキーマ実装

- `index.html` 内で `jpic-compatible-v1` プロファイルを実装
- 各成分に以下の項目を付与
  - 中毒量閾値（注意/中毒/重症/危機）
  - 症状タイムライン（時間帯別、赤旗所見）
  - 体内動態（Tmax, 半減期, Vd, 蛋白結合, 代謝, 排泄）
  - 治療ガイド（除染、拮抗薬、血液浄化、支持療法）
  - 分析法（推奨検査、解釈）
  - 根拠情報（出典、更新日、レベル）

## 注意

- 成分抽出は HTML 記述ゆれの影響を受けるため、すべてを完全に構造化できるわけではありません。
- 本データは教育・検討用途を想定し、臨床判断の一次情報としては PMDA 原文を必ず確認してください。
