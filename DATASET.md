# PMDA 市販薬データセット取得

このリポジトリには、PMDA の一般用医薬品・要指導医薬品検索ページから  
製品情報と成分情報を収集するスクリプトを追加しています。

## スクリプト

- `scripts/fetch_pmda_otc_dataset.py`

## 実行例

```bash
python3 scripts/fetch_pmda_otc_dataset.py --max-products 300 --output-dir data
```

## 出力ファイル

- `data/pmda_otc_products.json`
  - 製品コード、製品名、製造販売業者、薬効分類、リスク区分、剤形
  - 成分分量テキスト
  - 抽出した成分一覧（name / amount）
  - 参照元 URL（HTML / GeneralList / PDF）
- `data/pmda_otc_ingredient_index.json`
  - 成分名ごとの製品逆引きインデックス

## 収集ロジック概要

1. PMDA の候補リスト (`list_n.lib`) から製品名の先頭文字を収集
2. 先頭文字で検索（前方一致）し、ページング API で結果を巡回
3. 一意な製品コードを集約
4. 各製品の HTML 詳細ページから `成分分量` を抽出
5. 成分名と分量を正規表現ベースで抽出し JSON 化

## 注意

- 成分抽出は HTML 記述ゆれの影響を受けるため、すべてを完全に構造化できるわけではありません。
- 本データは教育・検討用途を想定し、臨床判断の一次情報としては PMDA 原文を必ず確認してください。
