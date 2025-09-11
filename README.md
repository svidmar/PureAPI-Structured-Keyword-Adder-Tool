# PSKAT – Pure Structured Keyword Adder Tool

`PSKAT.py` is a Python tool for enriching content in **Elsevier Pure** with structured or free keywords.  
It supports multiple content types (e.g. research outputs, datasets, organizations, etc.), appends new keywords without overwriting existing ones, and ensures version-safe updates.

---

## ✨ Features
- Read a list of UUIDs from an Excel file
- Fetch each item from Pure via the REST API
- Append structured keywords (classification-based) and/or free keywords
- Works with hybrid keyword groups (structured + free)
- Version locking to prevent accidental overwrites
- Supports **Basic Auth** authentication (if relevant)
- Handles different content types (research-outputs, datasets, organizations, etc.)
- Dry-run mode for safe simulation before making changes

---

## 📦 Requirements
- Python 3.8+
- Dependencies (install via pip):
  ```bash
  pip install requests pandas openpyxl
  ```

---

## ⚙️ Configuration
Open the script (`PSKAT.py`) and adjust the variables at the top:

- `BASE_URL` → Pure API base URL (e.g. `https://my-pure-instance.com/ws/api`)
- `API_KEY` → Your Pure API key
- `USE_BASIC_AUTH` → Set `True` to use Basic Auth as well as the API key
- `USERNAME` / `PASSWORD` → Only needed if `USE_BASIC_AUTH=True`
- `EXCEL_FILE` → Path to your Excel file containing UUIDs
- `DRY_RUN` → `True` to simulate changes without sending PUT requests

Excel file format:
```text
UUID
123e4567-e89b-12d3-a456-426614174000
abcdef12-3456-7890-abcd-ef1234567890
```

---

## ▶️ Usage

Run the tool:
```bash
python PSKAT.py
```

Interactive prompts will let you:
1. Select content type (`research-outputs`, `data-sets`, etc.)
2. Choose a keyword group from the allowed list
3. If applicable, choose classifications from the allowed list
4. Optionally add free keywords

---

## 🛡️ Safety
- PUT requests include the **version** of each object (`If-Match`) to avoid overwriting concurrent edits.
- Existing keywords are preserved (append-only behavior).
- If a UUID does not exist in Pure, it is skipped with a warning.
- `DRY_RUN=True` lets you preview changes safely.

---


## 🤝 Notes
- Test in a **non-production Pure environment** before applying in production.
- Some content types have different keyword group structures (the script handles both Free and Classification groups).
- For long runs, consider using `tee` to capture logs:
  ```bash
  python PSKAT.py | tee run.log
  ```

---

## 📜 License
MIT License – feel free to modify and share.
