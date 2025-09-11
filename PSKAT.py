#!/usr/bin/env python3
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIGURATION ==================
BASE_URL = "https://my_pure_instance.com/ws/api"       # Pure API base URL (no trailing slash)
API_KEY  = "My_API_Key"                           # Pure API key with proper privileges 

EXCEL_PATH = Path("data.xlsx")           # Excel file with a column 'UUID' containing the UUIDs for processing

# Behavior / limits
SLEEP = 0.35          # seconds between requests to the Pure API
DRY_RUN = False       # True = simulate, don't PUT
SKIP_EXISTING = True  # True = skip when keyword is already present
VERBOSE = True

# Authentication — Basic Auth (optional)
USE_BASIC_AUTH = False
USERNAME = "basic_auth_user"
PASSWORD = "basic_auth_psw"

# Selectable endpoints (resource paths)
COMMON_CONTENT_TYPES = [
    "research-outputs",
    "activities",
    "pressmedia",
    "projects",
    "data-sets",
    "prizes",
    "equipment",
    "impacts",
    "persons",
    "organizations",          
    "external-organizations",
]

# Logging
LOG_DIR = Path("logs")

# Retries
RETRY_TOTAL = 5
RETRY_BACKOFF = 0.5
# ===================================================


def banner():
    print(r"""
             ____
           / . . \     hiss...
           \  ---<
            \  /
       ______/ /
      / \   /\/
     /   \_/  \
    /          \

      Pure Structured Keyword Adder Tool
                     (PSKAT)

       _______
      /      /,
     /      //
    /______//
    (______(/
    """)


# ---------- HTTP helpers ----------
def mount_retries(session: requests.Session, total=RETRY_TOTAL, backoff=RETRY_BACKOFF) -> None:
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "PUT", "POST", "PATCH"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def req(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers.setdefault("accept", "application/json")
    if API_KEY:
        headers.setdefault("api-key", API_KEY)
    if method.upper() in ("POST", "PUT", "PATCH"):
        headers.setdefault("Content-Type", "application/json")
    auth = (USERNAME, PASSWORD) if USE_BASIC_AUTH else None
    return session.request(method=method, url=url, headers=headers, auth=auth, timeout=60, **kwargs)


# ---------- IO / UI helpers ----------
def load_uuids_from_excel(path: Path) -> List[str]:
    df = pd.read_excel(path)
    cols = {c.strip().lower(): c for c in df.columns}
    if "uuid" not in cols:
        raise ValueError("Excel must contain a 'UUID' column.")
    uuids = (
        df[cols["uuid"]]
        .astype(str)
        .str.strip()
        .replace({"": None})
        .dropna()
        .tolist()
    )
    return uuids


def choose_from_list(items: List[Any], label_fn) -> int:
    for i, item in enumerate(items, start=1):
        print(f"[{i}] {label_fn(item)}")
    while True:
        sel = input("Choose a number: ").strip()
        if sel.isdigit():
            idx = int(sel)
            if 1 <= idx <= len(items):
                return idx - 1
        print(f"Please enter a number between 1 and {len(items)}.")


def choose_multi_from_list(items: List[Any], label_fn) -> List[int]:
    for i, item in enumerate(items, start=1):
        print(f"[{i}] {label_fn(item)}")
    print("Select one or more numbers (comma-separated), or type 'all'.")
    while True:
        sel = input("Your choice: ").strip().lower()
        if sel == "all":
            return list(range(len(items)))
        try:
            idxs = [int(x.strip()) - 1 for x in sel.split(",") if x.strip()]
            if idxs and all(0 <= i < len(items) for i in idxs):
                return idxs
        except Exception:
            pass
        print("Please enter valid numbers like '1,3,5' or 'all'.")


# ---------- Merge helpers ----------
def normalize_kw(txt: str) -> str:
    return (txt or "").strip().casefold()


def ensure_group(keyword_groups: List[Dict[str, Any]], logical_name: str,
                 type_discriminator: str, group_name: Optional[Dict[str, str]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any], bool]:
    """Ensure a keyword group exists and return (groups, group_ref, created_flag)."""
    created = False
    target = None
    for g in keyword_groups or []:
        if g.get("logicalName") == logical_name:
            target = g
            break
    if target is None:
        target = {"typeDiscriminator": type_discriminator, "logicalName": logical_name}
        if group_name:
            target["name"] = group_name
        keyword_groups = (keyword_groups or []) + [target]
        created = True
    return keyword_groups, target, created


def ensure_classification_in_group(
    keyword_groups: List[Dict[str, Any]],
    group_logical_name: str,
    classification_uri: str,
    classification_term: Optional[Dict[str, str]] = None,
    group_type: str = "ClassificationsKeywordGroup",
    group_name: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    changed = False
    keyword_groups, target, _ = ensure_group(keyword_groups, group_logical_name, group_type, group_name)
    target.setdefault("classifications", [])
    if not any(c.get("uri") == classification_uri for c in target["classifications"]):
        entry = {"uri": classification_uri}
        if classification_term:
            entry["term"] = classification_term
        target["classifications"].append(entry)
        changed = True
    return keyword_groups, changed


def ensure_free_keywords_in_group_simple(
    keyword_groups: List[Dict[str, Any]],
    group_logical_name: str,
    new_terms: List[str],
    locale: str = "en_GB",
    group_type_hint: Optional[Dict[str, str]] = None,
    group_name: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Append free keywords for group-level FreeKeywordsKeywordGroup.

    Preferred/modern Pure shape (as seen on datasets):
      {
        "typeDiscriminator": "FreeKeywordsKeywordGroup",
        "logicalName": "...",
        "keywords": [
          { "locale": "en_GB", "freeKeywords": ["a","b"] },
          { "locale": "da_DK", "freeKeywords": ["x","y"] }
        ]
      }

    We also tolerate:
      - "freeKeywords": [ { "locale": "...", "freeKeywords": [...] } ]
      - legacy items like { "term": { "en_GB": "..." } } (we won't create this shape,
        but if present we'll append in that style only if neither keywords/freeKeywords
        locale-bucket arrays exist).
    """
    changed = False

    # ensure group
    created = False
    target = None
    for g in keyword_groups or []:
        if g.get("logicalName") == group_logical_name:
            target = g
            break
    if target is None:
        target = {
            "typeDiscriminator": "FreeKeywordsKeywordGroup",
            "logicalName": group_logical_name,
        }
        if group_name:
            target["name"] = group_name
        # Default to the modern locale-bucket structure
        target["keywords"] = []
        keyword_groups = (keyword_groups or []) + [target]
        created = True
        changed = True

    # Decide which array we should use
    array_key = None
    if isinstance(target.get("keywords"), list):
        array_key = "keywords"
    elif isinstance(target.get("freeKeywords"), list):
        # some installations use "freeKeywords" at the group level with the same locale-bucket shape
        array_key = "freeKeywords"

    # If neither exists, create the modern one
    if not array_key:
        target["keywords"] = []
        array_key = "keywords"
        changed = True

    # If we detect the locale-bucket shape (preferred)
    def _is_locale_bucket_list(lst):
        return lst and isinstance(lst[0], dict) and "locale" in lst[0] and "freeKeywords" in lst[0]

    bucket_list = target.get(array_key, [])
    if _is_locale_bucket_list(bucket_list) or array_key in ("keywords", "freeKeywords"):
        # find/create correct locale bucket
        locale_bucket = None
        for b in bucket_list:
            if b.get("locale") == locale:
                locale_bucket = b
                break
        if locale_bucket is None:
            locale_bucket = {"locale": locale, "freeKeywords": []}
            bucket_list.append(locale_bucket)
            changed = True

        existing = { (kw or "").strip().casefold() for kw in locale_bucket.get("freeKeywords", []) }
        for t in new_terms:
            norm = (t or "").strip()
            if not norm:
                continue
            if norm.casefold() in existing:
                continue
            locale_bucket["freeKeywords"].append(norm)
            existing.add(norm.casefold())
            changed = True

        # save back
        target[array_key] = bucket_list
        return keyword_groups, changed

    # Fallback: legacy "term" style (we won't create it, but we can append if it's what's there)
    items = target.get(array_key, [])
    if items and isinstance(items[0], dict) and "term" in items[0]:
        existing = set()
        for item in items:
            term = item.get("term", {})
            for v in term.values():
                existing.add((v or "").strip().casefold())
        for t in new_terms:
            norm = (t or "").strip()
            if not norm:
                continue
            if norm.casefold() in existing:
                continue
            items.append({"term": {locale: norm}})
            existing.add(norm.casefold())
            changed = True
        target[array_key] = items
        return keyword_groups, changed

    # If we got here, create the modern shape and recurse once
    target["keywords"] = []
    return ensure_free_keywords_in_group_simple(
        keyword_groups, group_logical_name, new_terms, locale, group_type_hint, group_name
    )


def ensure_full_group_additions(
    keyword_groups: List[Dict[str, Any]],
    group_logical_name: str,
    selected_classifications: List[Tuple[str, Optional[Dict[str, str]]]],  # list of (uri, term)
    free_terms: List[str],
    locale: str,
    group_name: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str], List[str], bool]:
    """
    For FullKeywordGroup:
      - ensure keywordContainers exists
      - for each selected classification uri, ensure a container and append free terms under the right locale
      - return (groups, added_cls_uris, added_free_terms, changed)
    """
    changed = False
    added_cls_uris: List[str] = []
    added_free_terms: List[str] = []

    keyword_groups, group, _ = ensure_group(keyword_groups, group_logical_name, "FullKeywordGroup", group_name)
    containers = group.setdefault("keywordContainers", [])

    # helper: find container by structuredKeyword.uri
    def find_container(uri: str) -> Optional[Dict[str, Any]]:
        for c in containers:
            sk = c.get("structuredKeyword") or {}
            if sk.get("uri") == uri:
                return c
        return None

    # For each selected classification, ensure container + (optional) free terms
    for uri, term in selected_classifications:
        cont = find_container(uri)
        if cont is None:
            cont = {"structuredKeyword": {"uri": uri}}
            if term:
                cont["structuredKeyword"]["term"] = term
            cont["freeKeywords"] = []  # array of {locale, freeKeywords:[...]}
            containers.append(cont)
            added_cls_uris.append(uri)
            changed = True

        # Ensure locale bucket inside freeKeywords
        # Pure format: freeKeywords: [ { locale: "da_DK", freeKeywords: ["x","y"] }, ... ]
        cont.setdefault("freeKeywords", [])
        loc_entry = None
        for fk in cont["freeKeywords"]:
            if fk.get("locale") == locale:
                loc_entry = fk
                break
        if loc_entry is None:
            loc_entry = {"locale": locale, "freeKeywords": []}
            cont["freeKeywords"].append(loc_entry)

        # Append unique free terms
        existing = {normalize_kw(t) for t in loc_entry.get("freeKeywords", [])}
        for t in free_terms:
            if t and normalize_kw(t) not in existing:
                loc_entry["freeKeywords"].append(t)
                existing.add(normalize_kw(t))
                added_free_terms.append(t)
                changed = True

    return keyword_groups, added_cls_uris, added_free_terms, changed


# ---------- Logging ----------
def init_logging() -> Tuple[Path, Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = LOG_DIR / f"pskat_log_{ts}.csv"
    xlsx_path = LOG_DIR / f"pskat_log_{ts}.xlsx"
    return csv_path, xlsx_path


def write_logs(rows: List[Dict[str, Any]], csv_path: Path, xlsx_path: Path) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    preferred = [
        "timestamp", "content_type", "uuid", "action", "http_status", "reason",
        "version_used", "group_logical_name",
        "classifications_added", "free_keywords_added",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    df.to_csv(csv_path, index=False, encoding="utf-8")
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="log", index=False)


# ---------- Content type picker ----------
def pick_content_type() -> str:
    print("\nSelect content type (resource path):\n")
    items = COMMON_CONTENT_TYPES + ["<Other – type your own>"]
    idx = choose_from_list(items, lambda x: x)
    if items[idx] == "<Other – type your own>":
        while True:
            custom = input("Enter resource path (e.g., research-outputs): ").strip().strip("/")
            if custom:
                return custom
            print("Please enter a non-empty resource path.")
    return items[idx]


# ================== MAIN ==================
def main():
    banner()

    base = BASE_URL.rstrip("/")
    csv_path, xlsx_path = init_logging()
    log_rows: List[Dict[str, Any]] = []

    # Read UUIDs
    try:
        uuids = load_uuids_from_excel(EXCEL_PATH)
    except Exception as e:
        print(f"ERROR reading Excel: {e}", file=sys.stderr)
        sys.exit(1)
    if not uuids:
        print("No UUIDs found in Excel.", file=sys.stderr)
        sys.exit(1)

    with requests.Session() as s:
        mount_retries(s)

        # Pick content type
        resource = pick_content_type().strip("/")

        # Fetch allowed keyword group configurations
        cfg_url = f"{base}/{resource}/allowed-keyword-group-configurations"
        print(f"\nFetching allowed keyword group configurations for '{resource}' …")
        r = req(s, "GET", cfg_url)
        if not r.ok:
            print(f"ERROR fetching configurations: {r.status_code} {r.text[:300]}", file=sys.stderr)
            sys.exit(1)
        configurations = r.json().get("configurations", [])
        if not configurations:
            print("No configurations returned for this content type.", file=sys.stderr)
            sys.exit(1)

        print("\nSelect the keyword group:\n")
        cfg_idx = choose_from_list(
            configurations,
            lambda c: f"{c.get('name',{}).get('en_GB') or c.get('logicalName')} "
                      f"(logicalName={c.get('logicalName')}, type={c.get('keywordGroupType')})"
        )
        chosen_cfg = configurations[cfg_idx]
        cfg_id = chosen_cfg.get("pureId")
        group_logical_name = chosen_cfg.get("logicalName")
        group_type = chosen_cfg.get("keywordGroupType") or "ClassificationsKeywordGroup"
        group_name = chosen_cfg.get("name")
        allow_userdefined = bool(chosen_cfg.get("allowUserdefinedKeywords"))

        # Capabilities
        is_full = group_type == "FullKeywordGroup"
        is_classifications_capable = is_full or group_type == "ClassificationsKeywordGroup"
        is_free_capable = is_full or allow_userdefined or group_type == "FreeKeywordsKeywordGroup"

        # Prepare classification choices (multi) if available
        class_list: List[Dict[str, Any]] = []
        cls_indices: List[int] = []
        if is_classifications_capable:
            classes_url = f"{base}/{resource}/allowed-keyword-group-configurations/{cfg_id}/classifications"
            print(f"\nFetching classifications for configuration {cfg_id} …")
            r = req(s, "GET", classes_url)
            if not r.ok:
                print(f"ERROR fetching classifications: {r.status_code} {r.text[:300]}", file=sys.stderr)
                sys.exit(1)
            class_list = r.json().get("classifications", [])
            if not class_list:
                print("No classifications available for the selected configuration.", file=sys.stderr)
                sys.exit(1)

            print("\nSelect classification(s) to add/append:\n")
            cls_indices = choose_multi_from_list(
                class_list,
                lambda c: f"{(c.get('term') or {}).get('en_GB') or c.get('uri')} (uri={c.get('uri')})"
            )

        # Free keywords prompt if allowed
        FREE_KWS: List[str] = []
        FREE_LOCALE = "en_GB"
        if is_free_capable:
            print("\nEnter free keywords to add (comma-separated). Leave empty to skip free keywords.")
            kw_line = input("Keywords: ").strip()
            if kw_line:
                FREE_KWS = [k.strip() for k in kw_line.split(",") if k.strip()]
                loc = input("Locale for these keywords [en_GB/da_DK] (default en_GB): ").strip() or "en_GB"
                FREE_LOCALE = loc if loc in ("en_GB", "da_DK") else "en_GB"

        # Build selected classifications (uri, term) list
        selected_cls: List[Tuple[str, Optional[Dict[str, str]]]] = []
        for i in cls_indices:
            c = class_list[i]
            selected_cls.append((c.get("uri"), c.get("term")))

        # Summary
        print("\nSummary:")
        print(f"  Content type:       {resource}")
        print(f"  Group logicalName:  {group_logical_name}")
        print(f"  Group type:         {group_type} (userdefined={allow_userdefined})")
        if selected_cls:
            labels = [ (t or {}).get("en_GB") or u for (u,t) in selected_cls ]
            print(f"  Classifications:    {', '.join(labels)}")
        if FREE_KWS:
            print(f"  Free keywords:      {', '.join(FREE_KWS)} [{FREE_LOCALE}]")
        print(f"  UUIDs to process:   {len(uuids)}")
        if DRY_RUN:
            print("  DRY-RUN mode:       ON")
        print()

        successes = 0
        skipped = 0
        failed: List[Tuple[str, str]] = []

        # Process UUIDs
        for i, uuid in enumerate(uuids, start=1):
            print(f"[{i}/{len(uuids)}] {uuid}")
            now = datetime.now().isoformat(timespec="seconds")
            get_url = f"{base}/{resource}/{uuid}"
            try:
                r = req(s, "GET", get_url)
                if r.status_code == 404:
                    msg = "Not found (404)"
                    print(f"  - {msg}. Skipping.")
                    skipped += 1
                    log_rows.append({
                        "timestamp": now, "content_type": resource, "uuid": uuid,
                        "action": "skipped", "http_status": 404, "reason": msg,
                        "version_used": None, "group_logical_name": group_logical_name,
                        "classifications_added": "", "free_keywords_added": "",
                    })
                    time.sleep(SLEEP)
                    continue
                if not r.ok:
                    raise RuntimeError(f"GET failed: {r.status_code} {r.text[:300]}")

                obj = r.json()
                version = obj.get("version")
                groups = obj.get("keywordGroups", [])

                changed = False
                added_cls_uris: List[str] = []
                added_free_terms: List[str] = []

                if is_full:
                    # FullKeywordGroup: containers per structuredKeyword, free terms nested per locale
                    groups, cls_uris, free_added, ch = ensure_full_group_additions(
                        groups,
                        group_logical_name=group_logical_name,
                        selected_classifications=selected_cls,
                        free_terms=FREE_KWS,
                        locale=FREE_LOCALE,
                        group_name=group_name,
                    )
                    added_cls_uris.extend(cls_uris)
                    added_free_terms.extend(free_added)
                    changed = changed or ch

                elif selected_cls and is_classifications_capable:
                    # ClassificationsKeywordGroup
                    for (uri, term) in selected_cls:
                        groups, ch = ensure_classification_in_group(
                            groups,
                            group_logical_name=group_logical_name,
                            classification_uri=uri,
                            classification_term=term,
                            group_type="ClassificationsKeywordGroup",
                            group_name=group_name,
                        )
                        if ch:
                            added_cls_uris.append(uri)
                        changed = changed or ch

                if FREE_KWS and not is_full and is_free_capable:
                    # Pure free-keywords-at-group-level case
                    groups, ch = ensure_free_keywords_in_group_simple(
                        groups,
                        group_logical_name=group_logical_name,
                        new_terms=FREE_KWS,
                        locale=FREE_LOCALE,
                        group_type_hint="FreeKeywordsKeywordGroup",
                        group_name=group_name,
                    )
                    if ch:
                        added_free_terms.extend(FREE_KWS)
                    changed = changed or ch

                if not changed:
                    msg = "Nothing to add (already present)"
                    print(f"  - {msg}.")
                    if SKIP_EXISTING:
                        skipped += 1
                        log_rows.append({
                            "timestamp": now, "content_type": resource, "uuid": uuid,
                            "action": "skipped", "http_status": 200, "reason": msg,
                            "version_used": version, "group_logical_name": group_logical_name,
                            "classifications_added": "", "free_keywords_added": "",
                        })
                        time.sleep(SLEEP)
                        continue

                put_body = {"keywordGroups": groups}

                if DRY_RUN:
                    print("  - DRY-RUN: would PUT updated keywordGroups.")
                    successes += 1
                    log_rows.append({
                        "timestamp": now, "content_type": resource, "uuid": uuid,
                        "action": "dry-run", "http_status": None, "reason": "no PUT (dry-run)",
                        "version_used": version, "group_logical_name": group_logical_name,
                        "classifications_added": ";".join(added_cls_uris),
                        "free_keywords_added": ";".join(added_free_terms),
                    })
                else:
                    headers = {}
                    if version:
                        headers["If-Match"] = version
                    put_url = f"{base}/{resource}/{uuid}"
                    r = req(s, "PUT", put_url, headers=headers, data=json.dumps(put_body))
                    if r.status_code in (200, 204):
                        print("  - Updated ✓")
                        successes += 1
                        log_rows.append({
                            "timestamp": now, "content_type": resource, "uuid": uuid,
                            "action": "updated", "http_status": r.status_code, "reason": "",
                            "version_used": version, "group_logical_name": group_logical_name,
                            "classifications_added": ";".join(added_cls_uris),
                            "free_keywords_added": ";".join(added_free_terms),
                        })
                    elif r.status_code == 412:
                        msg = "Version mismatch (412 Precondition Failed)"
                        print(f"  - FAILED: {msg}.")
                        failed.append((uuid, msg))
                        log_rows.append({
                            "timestamp": now, "content_type": resource, "uuid": uuid,
                            "action": "failed", "http_status": 412, "reason": msg,
                            "version_used": version, "group_logical_name": group_logical_name,
                            "classifications_added": ";".join(added_cls_uris),
                            "free_keywords_added": ";".join(added_free_terms),
                        })
                    else:
                        msg = f"{r.status_code} {r.text[:300]}"
                        print(f"  - FAILED: {msg}")
                        failed.append((uuid, msg))
                        log_rows.append({
                            "timestamp": now, "content_type": resource, "uuid": uuid,
                            "action": "failed", "http_status": r.status_code, "reason": msg,
                            "version_used": version, "group_logical_name": group_logical_name,
                            "classifications_added": ";".join(added_cls_uris),
                            "free_keywords_added": ";".join(added_free_terms),
                        })

                time.sleep(SLEEP)

            except Exception as e:
                msg = str(e)
                print(f"  - ERROR: {msg}")
                failed.append((uuid, msg))
                log_rows.append({
                    "timestamp": now, "content_type": resource, "uuid": uuid,
                    "action": "failed", "http_status": None, "reason": msg,
                    "version_used": None, "group_logical_name": group_logical_name,
                    "classifications_added": "",
                    "free_keywords_added": "",
                })
                time.sleep(SLEEP)

        # Write logs
        write_logs(log_rows, csv_path, xlsx_path)

        # Summary
        print("\nDone.")
        print(f"  Success: {successes}")
        print(f"  Skipped: {skipped}")
        print(f"  Failed:  {len(failed)}")
        print(f"\nLogs written to:\n  - {csv_path}\n  - {xlsx_path}")
        if failed:
            for u, why in failed[:20]:
                print(f"    - {u}: {why}")
            if len(failed) > 20:
                print(f"    (+{len(failed)-20} more)")


if __name__ == "__main__":
    main()