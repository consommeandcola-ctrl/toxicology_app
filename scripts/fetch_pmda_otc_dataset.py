#!/usr/bin/env python3
"""
PMDA 一般用医薬品・要指導医薬品の情報を取得し、
製品ごとの成分情報を JSON データセットとして保存するスクリプト。
"""

from __future__ import annotations

import argparse
import html
import json
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


BASE_URL = "https://www.pmda.go.jp"
SEARCH_URL = f"{BASE_URL}/PmdaSearch/otcSearch/"
PAGE_CHANGE_URL = f"{BASE_URL}/PmdaSearch/otcSearch/PageChangeRequest/{{page}}"
DETAIL_URL = f"{BASE_URL}/PmdaSearch/otcDetail/{{code}}"
GENERAL_URL = f"{BASE_URL}/PmdaSearch/otcDetail/GeneralList/{{code}}"
PDF_URL = f"{BASE_URL}/PmdaSearch/otcDetail/ResultDataSetPDF/{{code}}/A"
SUGGEST_LIST_URL = f"{BASE_URL}/PmdaSearch/js/data/otc/list_n.lib"


SEARCH_PAYLOAD_BASE = {
    "btnA.x": "0",
    "btnA.y": "0",
    "howtoMatchRadioValue": "2",  # 前方一致
    "tglOpFlg": "",
    "dispColumnsList[0]": "1",
    "dispColumnsList[1]": "2",
    "dispColumnsList[2]": "11",
    "dispColumnsList[3]": "6",
    "effectValue": "",
    "txtEffect": "",
    "txtEffectHowtoSearch": "and",
    "cautions": "",
    "cautionsHowtoSearch": "and",
    "updateDocFrDt": "年月日 [YYYYMMDD]",
    "updateDocToDt": "年月日 [YYYYMMDD]",
    "compNameWord": "",
    "dosage": "",
    "ingredient": "",
    "ingredientNotInclude": "",
    "additive": "",
    "additiveNotInclude": "",
    "risk": "",
    "howtoRdSearchSel": "or",
    "relationDoc1Sel": "",
    "relationDoc1check1": "on",
    "relationDoc1check2": "on",
    "relationDoc1Word": "検索語を入力",
    "relationDoc1HowtoSearch": "and",
    "relationDoc1FrDt": "年月 [YYYYMM]",
    "relationDoc1ToDt": "年月 [YYYYMM]",
    "relationDocHowtoSearchBetween12": "and",
    "relationDoc2Sel": "",
    "relationDoc2check1": "on",
    "relationDoc2check2": "on",
    "relationDoc2Word": "検索語を入力",
    "relationDoc2HowtoSearch": "and",
    "relationDoc2FrDt": "年月 [YYYYMM]",
    "relationDoc2ToDt": "年月 [YYYYMM]",
    "relationDocHowtoSearchBetween23": "and",
    "relationDoc3Sel": "",
    "relationDoc3check1": "on",
    "relationDoc3check2": "on",
    "relationDoc3Word": "検索語を入力",
    "relationDoc3HowtoSearch": "and",
    "relationDoc3FrDt": "年月 [YYYYMM]",
    "relationDoc3ToDt": "年月 [YYYYMM]",
    "ListRows": "100",
    "listCategory": "",
    "nameWord": "",
}


@dataclass
class SearchRow:
    code: str
    product_name: str
    manufacturer: str

    @property
    def general_url(self) -> str:
        return GENERAL_URL.format(code=self.code)

    @property
    def pdf_url(self) -> str:
        return PDF_URL.format(code=self.code)


def clean_text(fragment: str, keep_newline: bool = False) -> str:
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</p\s*>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"<[^>]+>", " ", fragment)
    fragment = html.unescape(fragment).replace("\u3000", " ")
    if keep_newline:
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in fragment.splitlines()]
        lines = [line for line in lines if line]
        return "\n".join(lines)
    fragment = re.sub(r"\s+", " ", fragment).strip()
    return fragment


def normalize_key(key: str) -> str:
    return re.sub(r"[^\w一-龥ぁ-んァ-ヶー]", "", key)


def extract_hidden_inputs(page_html: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for tag in re.findall(r"<input[^>]*type=\"hidden\"[^>]*>", page_html, flags=re.I):
        name_match = re.search(r"name=\"([^\"]+)\"", tag, flags=re.I)
        if not name_match:
            continue
        value_match = re.search(r"value=\"([^\"]*)\"", tag, flags=re.I)
        name = name_match.group(1)
        value = value_match.group(1) if value_match else ""
        data[name] = html.unescape(value)
    return data


def extract_rows_from_result_html(result_html: str) -> List[SearchRow]:
    rows: List[SearchRow] = []
    tr_list = re.findall(r"<tr class=['\"]TrColor[^'\"]*['\"]>(.*?)</tr>", result_html, flags=re.I | re.S)
    if not tr_list:
        tr_list = re.findall(r"<tr[^>]*>(.*?)</tr>", result_html, flags=re.I | re.S)

    for row_html in tr_list:
        name_match = re.search(
            r"/PmdaSearch/otcDetail/GeneralList/([^'\"/]+)['\"][^>]*>\s*(.*?)\s*</a>",
            row_html,
            flags=re.I | re.S,
        )
        if not name_match:
            continue

        code = name_match.group(1).strip()
        product_name = clean_text(name_match.group(2))
        manufacturer_match = re.search(
            r"margin-top:10px; margin-bottom:0px;[^>]*>\s*(.*?)\s*</div>",
            row_html,
            flags=re.I | re.S,
        )
        manufacturer = clean_text(manufacturer_match.group(1)) if manufacturer_match else ""

        rows.append(SearchRow(code=code, product_name=product_name, manufacturer=manufacturer))
    return rows


class DetailFieldParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fields: Dict[str, str] = {}
        self.current_head = ""
        self.capture_mode: Optional[str] = None
        self.capture_depth = 0
        self.buffer: List[str] = []

    def _start_capture(self, mode: str) -> None:
        self.capture_mode = mode
        self.capture_depth = 1
        self.buffer = []

    def _end_capture(self) -> None:
        text = "".join(self.buffer)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()
        if self.capture_mode == "head":
            self.current_head = text
        elif self.capture_mode == "deta" and self.current_head:
            self.fields[self.current_head] = text
            self.current_head = ""
        self.capture_mode = None
        self.capture_depth = 0
        self.buffer = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "td":
            if self.capture_mode is None:
                classes = attr.get("class", "").split()
                if "head" in classes:
                    self._start_capture("head")
                    return
                if "deta" in classes:
                    self._start_capture("deta")
                    return
            else:
                self.capture_depth += 1
                if self.capture_mode == "deta":
                    self.buffer.append("\t")
                return

        if self.capture_mode is None:
            return
        if tag.lower() in {"br", "tr", "p", "li", "div"}:
            self.buffer.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.capture_mode is None:
            return
        if tag.lower() == "td":
            self.capture_depth -= 1
            if self.capture_depth <= 0:
                self._end_capture()
            return
        if tag.lower() in {"tr", "p", "li", "div"}:
            self.buffer.append("\n")

    def handle_data(self, data: str) -> None:
        if self.capture_mode is not None:
            self.buffer.append(data)


def parse_detail_fields(detail_html: str) -> Dict[str, str]:
    parser = DetailFieldParser()
    parser.feed(detail_html)
    field_map: Dict[str, str] = {}
    for key, value in parser.fields.items():
        clean_key = clean_text(key)
        clean_value = clean_text(value, keep_newline=True)
        if clean_key:
            field_map[clean_key] = clean_value
    return field_map


def pick_field(field_map: Dict[str, str], candidates: List[str]) -> str:
    if not field_map:
        return ""
    normalized_to_value = {normalize_key(key): value for key, value in field_map.items()}
    for candidate in candidates:
        exact = normalized_to_value.get(normalize_key(candidate))
        if exact is not None:
            return exact
    for key, value in field_map.items():
        for candidate in candidates:
            if candidate in key:
                return value
    return ""


def parse_ingredients(ingredient_text: str) -> List[Dict[str, str]]:
    if not ingredient_text:
        return []

    text = ingredient_text.replace("（", "(").replace("）", ")")
    text = text.replace("：", " ").replace("　", " ")
    text = re.sub(r"\b(成分|分量|内訳)\b", " ", text)
    text = re.sub(r"\s+", " ", text)

    pattern = re.compile(
        r"([A-Za-z0-9一-龥ぁ-んァ-ヶー・αβγΑΒΓ\-\+／/\(\)]+)\s*"
        r"([0-9]+(?:\.[0-9]+)?\s*(?:mg|g|mL|ml|μg|µg|mcg|IU|国際単位|単位|mEq|%|％))"
    )
    seen = set()
    items: List[Dict[str, str]] = []
    for name, amount in pattern.findall(text):
        clean_name = name.strip().lstrip("・-")
        clean_name = clean_name.strip("()（）[]【】")
        if clean_name in {"成分", "分量", "内訳"}:
            continue
        if re.match(r"^[0-9０-９]", clean_name):
            continue
        if not re.search(r"[A-Za-z一-龥ぁ-んァ-ヶ]", clean_name):
            continue
        key = (clean_name, amount.strip())
        if not clean_name or key in seen:
            continue
        seen.add(key)
        items.append({"name": clean_name, "amount": amount.strip()})
    return items


def parse_additives(additive_text: str) -> List[str]:
    if not additive_text:
        return []
    parts = re.split(r"[、,，\n]", additive_text)
    cleaned = [part.strip() for part in parts if part.strip()]
    deduped = []
    seen = set()
    for item in cleaned:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def get_name_prefixes(session: requests.Session) -> List[str]:
    response = session.get(SUGGEST_LIST_URL, timeout=30)
    response.raise_for_status()

    # list_n.lib は UTF-8 で配布されている。
    text = response.content.decode("utf-8", errors="strict")
    names = [html.unescape(name) for name in re.findall(r"'([^']*)'", text)]
    prefixes = sorted({name[0] for name in names if name and name[0] != "�"})
    return prefixes


def search_prefix(session: requests.Session, prefix: str, list_rows: int, sleep_sec: float) -> Tuple[List[SearchRow], int]:
    payload = dict(SEARCH_PAYLOAD_BASE)
    payload["nameWord"] = prefix
    payload["ListRows"] = str(list_rows)

    response = session.post(SEARCH_URL, data=payload, timeout=30)
    response.raise_for_status()
    page_html = response.text

    hidden_data = extract_hidden_inputs(page_html)
    search_count = int(hidden_data.get("searchCnt", "0") or "0")
    total_pages = int(hidden_data.get("totalPages", "1") or "1")

    all_rows = extract_rows_from_result_html(page_html)
    for page in range(2, total_pages + 1):
        time.sleep(sleep_sec)
        page_response = session.post(
            PAGE_CHANGE_URL.format(page=page),
            data=hidden_data,
            timeout=30,
        )
        page_response.raise_for_status()
        payload_json = page_response.json()
        result_list_html = payload_json.get("ResultList", "")
        all_rows.extend(extract_rows_from_result_html(result_list_html))

    return all_rows, search_count


def fetch_detail(session: requests.Session, row: SearchRow, sleep_sec: float) -> Dict[str, object]:
    time.sleep(sleep_sec)
    response = session.get(DETAIL_URL.format(code=row.code), timeout=30)
    response.raise_for_status()
    detail_html = response.text

    field_map = parse_detail_fields(detail_html)
    ingredient_text = pick_field(field_map, ["成分分量", "成分・分量"])
    additives_text = pick_field(field_map, ["添加物"])

    record = {
        "code": row.code,
        "product_name": row.product_name,
        "manufacturer": row.manufacturer,
        "category": pick_field(field_map, ["薬効分類"]),
        "risk_class": pick_field(field_map, ["リスク区分"]),
        "dosage_form": pick_field(field_map, ["剤形"]),
        "classification": pick_field(field_map, ["医薬品区分"]),
        "ingredient_text": ingredient_text,
        "ingredients": parse_ingredients(ingredient_text),
        "additives": parse_additives(additives_text),
        "source": {
            "detail_html_url": DETAIL_URL.format(code=row.code),
            "general_url": row.general_url,
            "pdf_url": row.pdf_url,
        },
    }
    return record


def build_ingredient_index(products: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    index: Dict[str, Dict[str, object]] = defaultdict(lambda: {"count": 0, "products": []})
    for product in products:
        names = []
        for ingredient in product.get("ingredients", []):
            ingredient_name = ingredient.get("name", "").strip()
            if ingredient_name:
                names.append(ingredient_name)

        for ingredient_name in sorted(set(names)):
            index[ingredient_name]["count"] += 1
            index[ingredient_name]["products"].append(
                {
                    "code": product.get("code"),
                    "product_name": product.get("product_name"),
                }
            )
    # sort product list for stability
    for value in index.values():
        value["products"] = sorted(value["products"], key=lambda x: (x["product_name"] or "", x["code"] or ""))
    return dict(sorted(index.items(), key=lambda x: x[0]))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="PMDA OTC データ取得スクリプト")
    parser.add_argument("--max-products", type=int, default=200, help="取得する製品詳細の最大件数")
    parser.add_argument("--list-rows", type=int, default=100, help="検索一覧の1ページ表示件数")
    parser.add_argument("--sleep-sec", type=float, default=0.05, help="各リクエスト間の待機秒")
    parser.add_argument("--seed", type=int, default=20260213, help="接頭辞探索のシャッフルシード")
    parser.add_argument("--output-dir", default="data", help="出力先ディレクトリ")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "ToxicNavi-DatasetBuilder/1.0 (+https://github.com/consommeandcola-ctrl/toxicology_app)",
            "Accept-Language": "ja,en;q=0.8",
        }
    )

    # セッション初期化
    session.get(SEARCH_URL, timeout=30).raise_for_status()

    prefixes = get_name_prefixes(session)
    rng = random.Random(args.seed)
    rng.shuffle(prefixes)
    print(f"prefix count: {len(prefixes)}")

    rows_by_code: Dict[str, SearchRow] = {}
    total_hits = 0

    for idx, prefix in enumerate(prefixes, start=1):
        rows, search_count = search_prefix(session, prefix, args.list_rows, args.sleep_sec)
        total_hits += search_count
        for row in rows:
            if not row.code:
                continue
            if row.code not in rows_by_code:
                rows_by_code[row.code] = row

        print(
            f"[{idx}/{len(prefixes)}] prefix='{prefix}' hit={search_count} "
            f"unique_codes={len(rows_by_code)}"
        )

        if args.max_products > 0 and len(rows_by_code) >= args.max_products:
            break

    selected_rows = sorted(rows_by_code.values(), key=lambda row: (row.product_name, row.code))
    if args.max_products > 0:
        selected_rows = selected_rows[: args.max_products]

    print(f"detail fetch target: {len(selected_rows)} products")
    products: List[Dict[str, object]] = []
    failed_codes: List[str] = []

    for i, row in enumerate(selected_rows, start=1):
        try:
            product = fetch_detail(session, row, args.sleep_sec)
            products.append(product)
            if i % 20 == 0 or i == len(selected_rows):
                print(f"  detail progress: {i}/{len(selected_rows)}")
        except Exception as exc:  # noqa: BLE001
            failed_codes.append(row.code)
            print(f"  detail failed: code={row.code} error={exc}")

    ingredient_index = build_ingredient_index(products)

    metadata = {
        "source": "PMDA 一般用医薬品・要指導医薬品 添付文書等情報検索",
        "source_url": SEARCH_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "prefix_count": len(prefixes),
        "total_search_hits_across_prefixes": total_hits,
        "unique_codes_collected": len(rows_by_code),
        "detail_records": len(products),
        "detail_failed_codes": failed_codes,
    }

    output_dir = Path(args.output_dir)
    products_payload = {
        "metadata": metadata,
        "products": products,
    }
    write_json(output_dir / "pmda_otc_products.json", products_payload)
    write_json(
        output_dir / "pmda_otc_ingredient_index.json",
        {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_file": "pmda_otc_products.json",
                "ingredient_count": len(ingredient_index),
            },
            "ingredients": ingredient_index,
        },
    )

    print(f"saved: {output_dir / 'pmda_otc_products.json'}")
    print(f"saved: {output_dir / 'pmda_otc_ingredient_index.json'}")


if __name__ == "__main__":
    main()
