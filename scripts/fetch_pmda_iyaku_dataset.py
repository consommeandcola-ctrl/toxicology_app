#!/usr/bin/env python3
"""
PMDA 医療用医薬品(iyakuSearch)の検索結果CSVを収集し、
販売名(ゾロ含む)・一般名データセットを生成する。

検索結果は 1000 件上限があるため、更新日レンジを再帰分割して取得する。
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import re
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests


BASE_URL = "https://www.pmda.go.jp"
IYAKU_SEARCH_URL = f"{BASE_URL}/PmdaSearch/iyakuSearch/"
IYAKU_EXPORT_CSV_URL = f"{BASE_URL}/PmdaSearch/iyakuSearch/exportSearchResult/csv"


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_html_form_defaults(page_html: str) -> Dict[str, str]:
    payload: Dict[str, str] = {}

    for tag in re.findall(r"<input[^>]*>", page_html, flags=re.I):
        name_match = re.search(r'name="([^"]+)"', tag, flags=re.I)
        if not name_match:
            continue
        name = name_match.group(1)
        type_match = re.search(r'type="([^"]+)"', tag, flags=re.I)
        input_type = (type_match.group(1).lower() if type_match else "text")
        value_match = re.search(r'value="([^"]*)"', tag, flags=re.I)
        value = html.unescape(value_match.group(1) if value_match else "")
        checked = bool(re.search(r"checked", tag, flags=re.I))

        if input_type in {"hidden", "text"}:
            payload[name] = value
        elif input_type == "radio" and checked:
            payload[name] = value
        elif input_type == "checkbox" and checked:
            payload[name] = value or "on"

    for select_match in re.finditer(
        r'<select[^>]+name="([^"]+)"[^>]*>([\s\S]*?)</select>',
        page_html,
        flags=re.I,
    ):
        name = select_match.group(1)
        body = select_match.group(2)
        selected = re.search(
            r'<option[^>]*selected="selected"[^>]*value="([^"]*)"',
            body,
            flags=re.I,
        )
        if not selected:
            selected = re.search(
                r'<option[^>]*value="([^"]*)"[^>]*selected="selected"',
                body,
                flags=re.I,
            )
        if not selected:
            selected = re.search(r'<option[^>]*value="([^"]*)"', body, flags=re.I)
        payload[name] = html.unescape(selected.group(1) if selected else "")

    return payload


def extract_hidden_inputs(result_html: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for tag in re.findall(r'<input[^>]*type="hidden"[^>]*>', result_html, flags=re.I):
        name_match = re.search(r'name="([^"]+)"', tag, flags=re.I)
        if not name_match:
            continue
        value_match = re.search(r'value="([^"]*)"', tag, flags=re.I)
        values[name_match.group(1)] = html.unescape(value_match.group(1) if value_match else "")
    return values


def parse_search_count(result_html: str) -> int:
    match = re.search(r'name="searchCnt"[^>]*value="([0-9]+)"', result_html)
    return int(match.group(1)) if match else 0


def parse_csv_rows(csv_text: str) -> List[Dict[str, str]]:
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if len(rows) < 4:
        return []

    header = rows[2]
    data_rows: List[Dict[str, str]] = []
    for row in rows[3:]:
        if not row:
            continue
        if all(not cell.strip() for cell in row):
            continue
        item = {}
        for idx, col_name in enumerate(header):
            item[col_name] = row[idx].strip() if idx < len(row) else ""
        data_rows.append(item)
    return data_rows


def split_generic_components(generic_name: str) -> List[str]:
    text = normalize_text(generic_name)
    if not text:
        return []

    # 注記・括弧内をまず除去し、配合剤表記を平坦化する。
    text = re.sub(r"（[^）]*）", "", text)
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace("配合剤", "")
    text = normalize_text(text)

    parts = re.split(r"[・＋+／/,，]", text)
    cleaned = []
    seen = set()
    for part in parts:
        name = normalize_text(part).strip("[]【】")
        if not name:
            continue
        if name in {"遺伝子組換え", "遺伝子組み換え"}:
            continue
        if name in seen:
            continue
        seen.add(name)
        cleaned.append(name)

    return cleaned or [normalize_text(generic_name)]


def parse_doc_field(doc_field: str) -> Dict[str, str]:
    doc = normalize_text(doc_field)
    date_match = re.search(r"PDF\((\d{4})年(\d{2})月(\d{2})日\)", doc)
    update_date = ""
    if date_match:
        update_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
    return {
        "has_pdf": "PDF(" in doc,
        "has_html": "HTML" in doc,
        "has_xml": "XML" in doc,
        "update_date": update_date,
    }


class IyakuFetcher:
    def __init__(self, list_rows: int, max_search_count: int, sleep_sec: float) -> None:
        self.list_rows = list_rows
        self.max_search_count = max_search_count
        self.sleep_sec = sleep_sec
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "ToxicNavi-IyakuDatasetBuilder/1.0",
                "Accept-Language": "ja,en;q=0.8",
            }
        )
        self.base_payload: Dict[str, str] = {}
        self.search_request_count = 0
        self.export_request_count = 0
        self.range_export_count = 0

    def initialize(self) -> None:
        page = self.session.get(IYAKU_SEARCH_URL, timeout=30)
        page.raise_for_status()
        self.base_payload = parse_html_form_defaults(page.text)

    def search_range(self, start_date: date, end_date: date) -> Tuple[int, str, Dict[str, str]]:
        payload = dict(self.base_payload)
        payload.update(
            {
                "nameWord": "",
                "iyakuHowtoNameSearchRadioValue": "3",  # 販売名のみ
                "howtoMatchRadioValue": "2",  # 前方一致
                "updateDocFrDt": start_date.strftime("%Y%m%d"),
                "updateDocToDt": end_date.strftime("%Y%m%d"),
                "ListRows": str(self.list_rows),
                "btnA.x": "0",
                "btnA.y": "0",
            }
        )
        response = self.session.post(IYAKU_SEARCH_URL, data=payload, timeout=45)
        response.raise_for_status()
        self.search_request_count += 1
        result_html = response.text
        count = parse_search_count(result_html)
        hidden = extract_hidden_inputs(result_html)
        return count, result_html, hidden

    def export_csv(self, hidden_inputs: Dict[str, str], left_condition: str = "") -> List[Dict[str, str]]:
        form: Dict[str, str] = {
            "searchNameTitle": "医療用医薬品 情報検索",
            "leftSearchName": "医薬品の添付文書等を調べる",
            "leftSearchCondition": left_condition,
            "rightSearchName": "関連文書を調べる",
            "logicalOperators": "or",
            "rightSearchCondition": "",
            # CSV は表示項目をすべて出す仕様。主要列を固定指定しておく。
            "exportCols": "0,1,2,3,4,5",
        }
        form.update(hidden_inputs)
        for i in range(19):
            form.setdefault(f"dispColumnsList[{i}]", "")

        response = self.session.post(IYAKU_EXPORT_CSV_URL, data=form, timeout=90)
        response.raise_for_status()
        self.export_request_count += 1
        return parse_csv_rows(response.text)

    def collect_rows_recursive(self, start_date: date, end_date: date, out_rows: List[Dict[str, str]]) -> None:
        count, _, hidden = self.search_range(start_date, end_date)
        range_label = f"{start_date.isoformat()}..{end_date.isoformat()}"
        print(f"range {range_label} count={count}")

        if count == 0:
            return

        if count <= self.max_search_count:
            left_condition = f"改訂年月日:{start_date.strftime('%Y%m%d')}〜{end_date.strftime('%Y%m%d')}"
            rows = self.export_csv(hidden, left_condition=left_condition)
            self.range_export_count += 1
            for row in rows:
                row["_query_start"] = start_date.isoformat()
                row["_query_end"] = end_date.isoformat()
            out_rows.extend(rows)
            print(f"  exported rows={len(rows)}")
            time.sleep(self.sleep_sec)
            return

        if start_date >= end_date:
            # 1日レンジでも上限超過する場合は、やむを得ず未取得としてスキップ。
            # 実運用では追加分割条件(販売名prefix)を導入可能。
            print(f"  skip day overflow: {range_label}")
            return

        days = (end_date - start_date).days
        mid = start_date + timedelta(days=days // 2)
        self.collect_rows_recursive(start_date, mid, out_rows)
        self.collect_rows_recursive(mid + timedelta(days=1), end_date, out_rows)


def build_products(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    product_map: Dict[Tuple[str, str, str], Dict[str, object]] = {}

    for row in rows:
        generic_name = normalize_text(row.get("一般名", ""))
        product_name = normalize_text(row.get("販売名", ""))
        manufacturer = normalize_text(row.get("製造販売業者等", ""))
        product_name = product_name.lstrip(",， ").strip()
        manufacturer = manufacturer.lstrip(",， ").strip()
        document_field = row.get("添付文書", "")
        guide_field = row.get("患者向医薬品ガイド／ワクチン接種を受ける人へのガイド", "")
        interview_field = row.get("インタビューフォーム", "")

        if not product_name:
            continue

        ingredients = [{"name": name, "amount": ""} for name in split_generic_components(generic_name)]
        doc_info = parse_doc_field(document_field)

        key = (generic_name, product_name, manufacturer)
        current = product_map.get(key)
        candidate = {
            "generic_name": generic_name,
            "product_name": product_name,
            "manufacturer": manufacturer,
            "classification": "医療用医薬品",
            "ingredient_text": generic_name,
            "ingredients": ingredients,
            "documents": {
                "raw": normalize_text(document_field),
                "has_pdf": doc_info["has_pdf"],
                "has_html": doc_info["has_html"],
                "has_xml": doc_info["has_xml"],
                "update_date": doc_info["update_date"],
            },
            "patient_guide": normalize_text(guide_field),
            "interview_form": normalize_text(interview_field),
            "source": {
                "query_start": row.get("_query_start", ""),
                "query_end": row.get("_query_end", ""),
                "search_url": IYAKU_SEARCH_URL,
            },
        }

        if current is None:
            product_map[key] = candidate
            continue

        # より新しいPDF日付を優先して上書き
        cur_date = current["documents"].get("update_date", "")
        new_date = candidate["documents"].get("update_date", "")
        if new_date and (not cur_date or new_date > cur_date):
            product_map[key] = candidate

    products = sorted(
        product_map.values(),
        key=lambda x: (x.get("product_name", ""), x.get("manufacturer", "")),
    )
    return products


def build_ingredient_index(products: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    index: Dict[str, Dict[str, object]] = {}
    for product in products:
        for ingredient in product.get("ingredients", []):
            name = normalize_text(ingredient.get("name", ""))
            if not name:
                continue
            if name not in index:
                index[name] = {"count": 0, "products": []}
            index[name]["count"] += 1
            index[name]["products"].append(
                {
                    "product_name": product.get("product_name", ""),
                    "generic_name": product.get("generic_name", ""),
                    "manufacturer": product.get("manufacturer", ""),
                }
            )

    for value in index.values():
        value["products"] = sorted(
            value["products"],
            key=lambda x: (x["product_name"], x["manufacturer"]),
        )

    return dict(sorted(index.items(), key=lambda x: x[0]))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_date_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="PMDA iyakuSearch データセット生成")
    parser.add_argument("--from-date", default="20100101", help="取得開始日 YYYYMMDD")
    parser.add_argument("--to-date", default=datetime.now(timezone.utc).strftime("%Y%m%d"), help="取得終了日 YYYYMMDD")
    parser.add_argument("--max-search-count", type=int, default=1000, help="検索1回で許容する最大件数")
    parser.add_argument("--list-rows", type=int, default=100, help="検索時の表示件数")
    parser.add_argument("--sleep-sec", type=float, default=0.05, help="リクエスト間待機秒")
    parser.add_argument("--output-dir", default="data", help="出力ディレクトリ")
    args = parser.parse_args()

    start_date = parse_date_yyyymmdd(args.from_date)
    end_date = parse_date_yyyymmdd(args.to_date)
    if start_date > end_date:
        raise ValueError("from-date must be <= to-date")

    fetcher = IyakuFetcher(
        list_rows=args.list_rows,
        max_search_count=args.max_search_count,
        sleep_sec=args.sleep_sec,
    )
    fetcher.initialize()

    raw_rows: List[Dict[str, str]] = []
    fetcher.collect_rows_recursive(start_date, end_date, raw_rows)
    products = build_products(raw_rows)
    ingredient_index = build_ingredient_index(products)

    output_dir = Path(args.output_dir)
    metadata = {
        "source": "PMDA 医療用医薬品 添付文書等情報検索(iyakuSearch)",
        "source_url": IYAKU_SEARCH_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "from_date": start_date.isoformat(),
        "to_date": end_date.isoformat(),
        "max_search_count": args.max_search_count,
        "search_requests": fetcher.search_request_count,
        "export_requests": fetcher.export_request_count,
        "exported_ranges": fetcher.range_export_count,
        "raw_export_rows": len(raw_rows),
        "unique_products": len(products),
        "unique_ingredients": len(ingredient_index),
    }

    write_json(
        output_dir / "pmda_iyaku_products.json",
        {
            "metadata": metadata,
            "products": products,
        },
    )
    write_json(
        output_dir / "pmda_iyaku_ingredient_index.json",
        {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_file": "pmda_iyaku_products.json",
                "ingredient_count": len(ingredient_index),
            },
            "ingredients": ingredient_index,
        },
    )

    print(f"saved: {output_dir / 'pmda_iyaku_products.json'}")
    print(f"saved: {output_dir / 'pmda_iyaku_ingredient_index.json'}")


if __name__ == "__main__":
    main()
