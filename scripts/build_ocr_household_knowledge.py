#!/usr/bin/env python3
"""
OCRテキストから家庭用品・医薬品中毒の構造化知識JSONを生成する。

入力:
  uploads/ocr_result_1770368005162.txt

出力:
  data/ocr_household_knowledge.json
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


PAGE_RE = re.compile(r"^=+\s*ページ\s*(\d+)\s*=+$")
TITLE_RE = re.compile(r"^(?P<title>.+?)\s*危険度(?:[:：・]\s*|[\s]*)?(?P<risk>.*)$")

SECTION_LABELS: Sequence[Tuple[str, str]] = (
    ("中毒症状", "symptoms"),
    ("処置法", "decontamination"),
    ("ポイント", "points"),
    ("基本的処置", "basic"),
    ("治療上の注意点", "treatment_notes"),
    ("治療", "treatment"),
    ("特記事項", "notes"),
    ("体内動態", "pk"),
    ("主な製品", "products"),
)

CRITICAL_KEYWORDS = [
    "ショック",
    "意識障害",
    "意識消失",
    "昏睡",
    "痙攣",
    "呼吸困難",
    "低酸素",
    "チアノーゼ",
    "肺浮腫",
    "低血圧",
    "不整脈",
    "腎不全",
    "肝不全",
]

TEST_KEYWORDS = [
    ("血液ガス", "血液ガス"),
    ("電解質", "電解質"),
    ("腎機能", "腎機能"),
    ("肝機能", "肝機能"),
    ("心電図", "心電図"),
    ("血圧", "循環動態"),
    ("呼吸", "呼吸状態"),
    ("SpO2", "SpO2"),
]


@dataclass
class Line:
    page: int
    text: str


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text or "")


def clean_inline(text: str) -> str:
    value = normalize(text)
    value = value.replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def split_tokens(text: str) -> List[str]:
    if not text:
        return []
    value = clean_inline(text)
    value = value.replace("症状 |", "")
    value = value.replace("処置法 |", "")
    parts = re.split(r"[|、,，/／・;；。]", value)
    items: List[str] = []
    for part in parts:
        token = clean_inline(part)
        token = token.strip("・-:：")
        if not token:
            continue
        if len(token) > 36:
            continue
        if token in {"中毒症状", "処置法", "基本的処置", "治療", "ポイント", "特記事項"}:
            continue
        if any(word in token for word in ["可能性", "場合", "記載", "文献", "症例", "資料"]):
            continue
        items.append(token)
    # 順序維持で重複除去
    dedup: List[str] = []
    seen = set()
    for token in items:
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(token)
    return dedup


def is_title_line(text: str) -> bool:
    line = clean_inline(text)
    if not line or "危険度" not in line:
        return False
    if line.startswith(("*", "・", "-", "「", "『")):
        return False
    if "危険度にも" in line or "危険度を" in line:
        return False
    if len(line) > 180:
        return False
    return True


def parse_lines(source: str) -> List[Line]:
    current_page = 0
    parsed: List[Line] = []
    for raw in source.splitlines():
        marker = PAGE_RE.match(raw.strip())
        if marker:
            current_page = int(marker.group(1))
            continue
        parsed.append(Line(page=current_page, text=raw.rstrip("\n")))
    return parsed


def cleanup_title(raw_title: str) -> str:
    title = clean_inline(raw_title)
    title = title.replace("危険度", " ")
    title = title.replace("|", " ")
    title = re.sub(r"^(医薬品|家庭用品|工業用品|農薬|自然毒|家庭用化学物質)\s*", "", title)
    title = re.sub(r"^[~\-\|:：\s]+", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def parse_title_info(line: str) -> Tuple[str, str, List[str]]:
    cleaned = clean_inline(line)
    matched = TITLE_RE.match(cleaned)
    title_part = cleanup_title(matched.group("title") if matched else cleaned)
    risk_text = clean_inline(matched.group("risk") if matched else "")

    japanese_name = clean_inline(re.split(r"\(", title_part, maxsplit=1)[0]).strip(" :：-")
    jp_start = re.search(r"[一-龥ぁ-んァ-ヶ].*", japanese_name)
    if jp_start:
        japanese_name = jp_start.group(0).strip()
    if not japanese_name:
        japanese_name = title_part

    component = ""
    for token in re.findall(r"\(([^)]{1,160})\)", title_part):
        token_clean = clean_inline(token)
        if re.search(r"[A-Za-z]", token_clean):
            component = token_clean
            break
    if not component:
        component = japanese_name

    aliases = [japanese_name]
    if title_part and title_part != japanese_name:
        aliases.append(title_part)
    if component and component not in aliases:
        aliases.append(component)
    if risk_text and re.search(r"[0-9A-Za-zぁ-んァ-ヶ一-龥]", risk_text):
        aliases.append(risk_text[:24])
    return japanese_name, component, aliases


def detect_section_label(line: str) -> Optional[str]:
    normalized = clean_inline(line).replace(" ", "")
    for label, key in SECTION_LABELS:
        if label in normalized:
            return key
    return None


def parse_mgkg_values(text_lines: Sequence[str]) -> List[float]:
    values: List[float] = []
    for line in text_lines:
        norm = clean_inline(line)
        # 範囲 (mg/kg)
        for lo, hi in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*(?:[-ー〜~]\s*)([0-9]+(?:\.[0-9]+)?)\s*mg/kg", norm, flags=re.I):
            values.extend([float(lo), float(hi)])
        # 単値 (mg/kg)
        for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*mg/kg", norm, flags=re.I):
            values.append(float(value))

        # 範囲 (g/kg) -> mg/kg
        for lo, hi in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*(?:[-ー〜~]\s*)([0-9]+(?:\.[0-9]+)?)\s*g/kg", norm, flags=re.I):
            values.extend([float(lo) * 1000, float(hi) * 1000])
        # 単値 (g/kg)
        for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*g/kg", norm, flags=re.I):
            values.append(float(value) * 1000)

    dedup = sorted({round(v, 4) for v in values if v > 0})
    return dedup


def build_thresholds(lines: Sequence[str]) -> Dict[str, Optional[float]]:
    values = parse_mgkg_values(lines)
    if not values:
        return {"caution": None, "toxic": None, "severe": None, "critical": None}

    toxic = values[0]
    severe = values[min(len(values) - 1, max(1, len(values) // 2))]
    critical = values[-1]

    if severe < toxic:
        severe = toxic * 2
    if critical < severe:
        critical = severe * 1.5

    caution = toxic * 0.5
    return {
        "caution": round(caution, 4),
        "toxic": round(toxic, 4),
        "severe": round(severe, 4),
        "critical": round(critical, 4),
    }


def build_critical_symptoms(symptoms: Sequence[str], extra_lines: Sequence[str]) -> List[str]:
    joined = " ".join(extra_lines)
    result: List[str] = []
    for symptom in symptoms:
        if any(keyword in symptom for keyword in CRITICAL_KEYWORDS):
            result.append(symptom)
    for keyword in CRITICAL_KEYWORDS:
        if keyword in joined and keyword not in result:
            result.append(keyword)
    dedup: List[str] = []
    seen = set()
    for item in result:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)
    return dedup[:12]


def build_treatment_payload(section: Dict[str, List[str]]) -> Dict[str, object]:
    treatment_lines = section.get("decontamination", []) + section.get("basic", []) + section.get("treatment", [])
    treatment_text = " ".join(treatment_lines)

    has_lavage = "胃洗浄" in treatment_text
    lavage_not = any(word in treatment_text for word in ["胃洗浄は不要", "胃洗浄不要", "胃洗浄は行わない", "胃洗浄禁忌"])
    has_charcoal = "活性炭" in treatment_text
    charcoal_not = any(word in treatment_text for word in ["活性炭不要", "活性炭は不要", "活性炭禁忌"])
    has_dialysis = any(word in treatment_text for word in ["血液透析", "血液吸着", "血液浄化"])
    antidote_none = any(word in treatment_text for word in ["解毒剤はない", "解毒剤なし", "特異的な治療法はない", "特異的な解毒剤はない"])

    other_items = split_tokens(" | ".join(section.get("treatment_notes", []) + section.get("points", []) + section.get("notes", [])))
    filtered_other_items = []
    for item in other_items:
        if len(item) < 3 or len(item) > 28:
            continue
        if any(word in item for word in ["ファイルシート", "IV", "文献", "参考", "危険度"]):
            continue
        filtered_other_items.append(item)
    others = [{"name": item, "severity_min": 2, "note": item} for item in filtered_other_items[:8]]

    return {
        "antidote": {
            "name": "特異的解毒剤なし" if antidote_none else "要個別確認",
            "indication": "資料由来の記載を参照して支持療法を判断",
        },
        "lavage": {
            "allow": bool(has_lavage and not lavage_not),
            "window_min": 120,
            "note": clean_inline(" / ".join(treatment_lines[:3])) or "資料記載を参照",
        },
        "charcoal": {
            "allow": bool(has_charcoal and not charcoal_not),
            "window_min": 120,
            "extended_window_min": 240,
            "note": clean_inline(" / ".join(treatment_lines[3:6])) or "資料記載を参照",
        },
        "dialysis": {
            "effective": bool(has_dialysis),
            "indication": "血液透析/血液吸着の記載あり" if has_dialysis else "有効性情報なし",
        },
        "other": others,
    }


def build_recommended_tests(block_lines: Sequence[str]) -> List[str]:
    blob = " ".join(block_lines)
    tests: List[str] = []
    for keyword, label in TEST_KEYWORDS:
        if keyword in blob and label not in tests:
            tests.append(label)
    if not tests:
        tests = ["バイタル", "血液ガス", "電解質"]
    return tests


def build_timeline(symptoms: Sequence[str], critical_symptoms: Sequence[str]) -> List[Dict[str, object]]:
    early = list(symptoms[:4]) if symptoms else ["情報不足"]
    mid = list(symptoms[2:8]) if len(symptoms) > 2 else list(symptoms[:6]) or ["情報不足"]
    late = list(critical_symptoms[:4]) if critical_symptoms else list(symptoms[:3]) or ["情報不足"]
    return [
        {"window": "0-2時間", "symptoms": early, "red_flags": list(critical_symptoms[:2])},
        {"window": "2-8時間", "symptoms": mid, "red_flags": list(critical_symptoms[:4])},
        {"window": "8時間以降", "symptoms": late, "red_flags": list(critical_symptoms[:6])},
    ]


def parse_entries(lines: Sequence[Line]) -> List[Dict[str, object]]:
    title_positions = [idx for idx, line in enumerate(lines) if is_title_line(line.text)]
    profiles: List[Dict[str, object]] = []

    for pos_idx, start in enumerate(title_positions):
        end = title_positions[pos_idx + 1] if pos_idx + 1 < len(title_positions) else len(lines)
        block = lines[start:end]
        if not block:
            continue

        ingredient_name, component, aliases = parse_title_info(block[0].text)
        if not ingredient_name:
            continue

        section: Dict[str, List[str]] = {"head": [clean_inline(block[0].text)]}
        current = "head"
        pages = {block[0].page}
        for row in block[1:]:
            pages.add(row.page)
            text = clean_inline(row.text)
            if not text:
                continue
            label = detect_section_label(text)
            if label:
                current = label
                section.setdefault(current, [])
                continue
            section.setdefault(current, []).append(text)

        symptom_lines = section.get("symptoms", [])
        symptom_tokens = split_tokens(" | ".join(symptom_lines))
        symptoms = symptom_tokens[:24] if symptom_tokens else ["情報不足"]
        critical_symptoms = build_critical_symptoms(symptoms, extra_lines=[line.text for line in block])

        thresholds = build_thresholds([line.text for line in block])
        treatment = build_treatment_payload(section)
        timeline = build_timeline(symptoms, critical_symptoms)

        product_aliases_raw = split_tokens(" | ".join(section.get("products", [])))
        product_aliases: List[str] = []
        for alias in product_aliases_raw:
            if len(alias) < 2 or len(alias) > 30:
                continue
            if any(word in alias for word in ["主な製品", "危険度", "ファイルシート", "文献", "強アルカリ性", "中性"]):
                continue
            if not re.search(r"[一-龥ぁ-んァ-ヶA-Za-z]", alias):
                continue
            product_aliases.append(alias)
        # 順序保持重複除去
        dedup_product_aliases: List[str] = []
        seen_alias = set()
        for alias in product_aliases:
            key = alias.lower()
            if key in seen_alias:
                continue
            seen_alias.add(key)
            dedup_product_aliases.append(alias)
        product_aliases = dedup_product_aliases[:24]
        recommended_tests = build_recommended_tests([line.text for line in block])

        pk_lines = section.get("pk", [])
        pk_summary = clean_inline(" / ".join(pk_lines[:4])) if pk_lines else "情報不足"
        notes_summary = clean_inline(" / ".join(section.get("notes", [])[:4])) if section.get("notes") else ""
        interpretation = clean_inline(" / ".join(section.get("points", [])[:3])) or "症候・曝露量を総合評価"

        profiles.append(
            {
                "ingredient_name": ingredient_name,
                "component": component,
                "aliases": aliases,
                "product_aliases": product_aliases,
                "toxic_threshold_mg_kg": thresholds,
                "symptoms": symptoms,
                "critical_symptoms": critical_symptoms,
                "symptom_timeline": timeline,
                "toxicokinetics": {
                    "tmaxHours": "情報不足",
                    "halfLifeHours": "情報不足",
                    "vdLKg": "情報不足",
                    "proteinBindingPct": "情報不足",
                    "metabolism": pk_summary,
                    "elimination": pk_summary,
                },
                "treatment": treatment,
                "analysis": {
                    "recommended_tests": recommended_tests,
                    "interpretation": interpretation,
                    "notes": notes_summary,
                },
                "evidence": {
                    "source": "ocr_result_1770368005162.txt",
                    "pages": sorted({page for page in pages if page > 0}),
                    "updated_at": datetime.now(timezone.utc).date().isoformat(),
                    "level": "ocr-auto",
                },
            }
        )
    return profiles


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="OCR家庭用品中毒データを構造化JSONへ変換")
    parser.add_argument(
        "--input",
        default="/home/ubuntu/.cursor/projects/workspace/uploads/ocr_result_1770368005162.txt",
        help="OCR入力テキストのパス",
    )
    parser.add_argument(
        "--output",
        default="data/ocr_household_knowledge.json",
        help="出力JSONパス",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    source = input_path.read_text(encoding="utf-8", errors="replace")
    lines = parse_lines(source)
    profiles = parse_entries(lines)

    payload = {
        "metadata": {
            "source_file": str(input_path),
            "compiled_at": datetime.now(timezone.utc).isoformat(),
            "description": "OCR抽出資料から自動生成した中毒知識データ",
            "entry_count": len(profiles),
        },
        "profiles": profiles,
    }

    write_json(output_path, payload)
    print(f"saved: {output_path} (profiles={len(profiles)})")


if __name__ == "__main__":
    main()
