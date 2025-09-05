import os
import re
import io
import zipfile
import shutil
import tempfile
import subprocess
import json
from pathlib import Path
import pandas as pd
import streamlit as st
from datetime import date

# ---------- Konfiguracja ----------
APP_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = APP_DIR / "Dane"
CONFIG_PATH = APP_DIR / ".sprzedaz_config.json"

st.set_page_config(page_title="Sprzedaż wg zleceniobiorcy i produktu", layout="wide")
st.title("📊 Sprzedaż wg zleceniobiorcy i produktu")
st.caption("W folderze **Dane/** trzymaj dzienne raporty (.xlsx/.xls/.csv/.txt). Ostatnio użyta ścieżka zapisuje się w pliku **.sprzedaz_config.json**.")

# ---------- Konfig: odczyt/zapis ----------
def load_config() -> dict:
    cfg = {"data_dir": str(DEFAULT_DATA_DIR), "soffice_path": ""}
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                file_cfg = json.load(f)
                if isinstance(file_cfg, dict):
                    cfg.update({k: str(v) for k, v in file_cfg.items()})
    except Exception as e:
        st.warning(f"Nie udało się wczytać konfiguracji: {e}")
    return cfg

def save_config(data_dir: Path, soffice_path: str) -> None:
    try:
        tmp_path = CONFIG_PATH.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"data_dir": str(data_dir), "soffice_path": soffice_path}, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, CONFIG_PATH)
    except Exception as e:
        st.warning(f"Nie udało się zapisać konfiguracji: {e}")

# ---------- Naprawa XLSX bez Excela ----------
def _sanitize_xml_text(text: str) -> str:
    out = []
    for ch in text:
        cp = ord(ch)
        if ch in ('\t', '\n', '\r') or (0x20 <= cp <= 0xD7FF) or (0xE000 <= cp <= 0xFFFD):
            out.append(ch)
    return "".join(out)

def _patch_workbook_xml(content: str) -> str:
    def repl(m):
        prefix, val, suffix = m.group(1), m.group(2), m.group(3)
        if val not in {"hidden", "visible", "veryHidden"}:
            return f"{prefix}visible{suffix}"
        return m.group(0)
    return re.sub(r'(<sheet[^>]*\sstate=")([^"]+)(")', repl, content)

def repair_xlsx_zip(src_path: Path) -> Path | None:
    tmp_dir = None
    try:
        tmp_dir = Path(tempfile.mkdtemp(prefix="xlsxfix_"))
        extract_dir = tmp_dir / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(extract_dir)

        wb_xml = extract_dir / "xl" / "workbook.xml"
        if wb_xml.exists():
            txt = wb_xml.read_text(encoding="utf-8", errors="ignore")
            txt = _sanitize_xml_text(txt)
            txt = _patch_workbook_xml(txt)
            wb_xml.write_text(txt, encoding="utf-8")

        for p in extract_dir.rglob("*.xml"):
            try:
                s = p.read_text(encoding="utf-8", errors="ignore")
                cleaned = _sanitize_xml_text(s)
                if cleaned != s:
                    p.write_text(cleaned, encoding="utf-8")
            except Exception:
                pass

        repaired = src_path.with_suffix(".repaired.xlsx")
        with zipfile.ZipFile(repaired, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for root, _, files in os.walk(extract_dir):
                for name in files:
                    fp = Path(root) / name
                    arcname = fp.relative_to(extract_dir)
                    zout.write(fp, arcname.as_posix())
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return repaired
    except Exception:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return None

def convert_with_soffice(src_path: Path, soffice_path: Path) -> Path | None:
    try:
        out_dir = Path(tempfile.mkdtemp(prefix="xlscsv_"))
        cmd = [str(soffice_path), "--headless", "--convert-to", "csv", "--outdir", str(out_dir), str(src_path)]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=90)
        candidate = out_dir / (src_path.stem + ".csv")
        if candidate.exists():
            return candidate
        return None
    except Exception:
        return None

# ---------- Odczyt plików ----------
def _try_read_excel_openpyxl(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl")

def _try_read_excel_calamine(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="calamine")

def read_any_table(path: Path, soffice_path: Path | None) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".csv", ".txt"]:
        return pd.read_csv(path)
    if ext in [".xls", ".xlsx"]:
        last_err = None
        try:
            return _try_read_excel_openpyxl(path)
        except Exception as e:
            last_err = e
        try:
            return _try_read_excel_calamine(path)
        except Exception as e:
            last_err = e
        repaired = repair_xlsx_zip(path)
        if repaired and repaired.exists():
            try:
                return _try_read_excel_openpyxl(repaired)
            except Exception as e:
                last_err = e
            try:
                return _try_read_excel_calamine(repaired)
            except Exception as e:
                last_err = e
        if soffice_path and soffice_path.exists():
            csv_path = convert_with_soffice(path, soffice_path)
            if csv_path and csv_path.exists():
                try:
                    return pd.read_csv(csv_path)
                except Exception as e:
                    last_err = e
        raise RuntimeError(f"Nie udało się odczytać pliku {path.name}. Ostatni błąd: {last_err}")
    raise ValueError(f"Nieobsługiwane rozszerzenie pliku: {ext}")

@st.cache_data(show_spinner=False)
def load_all_data(data_dir: Path, soffice_path: Path | None) -> pd.DataFrame:
    files = []
    for patt in ("*.xlsx", "*.xls", "*.csv", "*.txt"):
        files.extend(sorted(data_dir.glob(patt)))

    # Dedup: jeśli istnieje wersja .repaired.xlsx i oryginał, bierzemy tylko repaired
    selected_files = []
    bycanon = {}
    for p in files:
        if p.suffix.lower() == ".xlsx" and (p.name.endswith(".repaired.xlsx") or p.name.endswith(".xlsx")):
            canon = p.name[:-len(".repaired.xlsx")] + ".xlsx" if p.name.endswith(".repaired.xlsx") else p.name
            bycanon.setdefault(canon, []).append(p)
        else:
            selected_files.append(p)
    for canon, plist in bycanon.items():
        repaired = [pp for pp in plist if pp.name.endswith(".repaired.xlsx")]
        if repaired:
            selected_files.append(sorted(repaired)[0])
        else:
            selected_files.append(sorted(plist)[0])
    files = sorted(selected_files)

    if not files:
        st.info("W folderze **Dane/** nie znaleziono plików .xlsx/.xls/.csv/.txt")
        return pd.DataFrame()

    frames, failures = [], []
    for p in files:
        try:
            df = read_any_table(p, soffice_path=soffice_path)
            df["__source_file"] = p.name
            frames.append(df)
        except Exception as e:
            failures.append((p.name, str(e)))

    if failures:
        with st.expander("❗ Pliki z błędami (kliknij aby rozwinąć)"):
            for name, err in failures:
                st.error(f"{name} → {err}")

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    data.columns = [str(c).strip() for c in data.columns]
    if "Quantity" in data.columns:
        data["Quantity"] = pd.to_numeric(data["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in data.columns:
        data["NetAmount"] = pd.to_numeric(data["NetAmount"], errors="coerce")
    return data

# ---------- Wydobycie daty ----------
DATE_CANDIDATE_SUBSTRINGS = ["date", "czas", "data", "time"]

def _first_valid_datetime_series(df: pd.DataFrame) -> pd.Series | None:
    for col in df.columns:
        lc = str(col).lower()
        if any(tok in lc for tok in DATE_CANDIDATE_SUBSTRINGS):
            ser = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            if ser.notna().mean() > 0.5:
                return ser
    return None

def _date_from_filename(name: str) -> date | None:
    m = re.search(r"(\d{2})[._-](\d{2})[._-](\d{4})", name)
    if m:
        d, mth, y = map(int, m.groups())
        try:
            return date(y, mth, d)
        except ValueError:
            return None
    return None

def add__date_column(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    ser = _first_valid_datetime_series(out)
    if ser is not None:
        out["__date"] = ser.dt.date
    else:
        dates = []
        for fname in out.get("__source_file", pd.Series(["unknown"] * len(out))):
            parsed = _date_from_filename(str(fname))
            dates.append(parsed)
        out["__date"] = dates
    return out

# =============== TABS (podstrony) ===============
tab_dane, tab_pivot, tab_indy, tab_best, tab_comp = st.tabs(
    ["🗂️ Dane", "📈 Tabela przestawna", "👤 Wyniki indywidualne", "🏆 Najlepsi", "🧮 Kreator Konkursów"]
)

# ---------- Zakładka: Dane ----------
with tab_dane:
    st.subheader("🗂️ Ustawienia źródła danych")
    _cfg = load_config()
    data_dir_str = st.text_input("📁 Folder z danymi", value=_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
    data_dir = Path(data_dir_str)
    soffice_str = st.text_input("🧰 Ścieżka do LibreOffice soffice.exe (opcjonalnie)", value=_cfg.get("soffice_path", ""), placeholder=r"C:\Program Files\LibreOffice\program\soffice.exe")
    soffice_path = Path(soffice_str) if soffice_str.strip() else None

    col_reload, col_preview = st.columns([1, 1])
    with col_reload:
        reload_clicked = st.button("🔄 Wczytaj/odśwież dane", type="primary")
    with col_preview:
        show_preview = st.toggle("Pokaż podgląd danych", value=False)

    if reload_clicked or "cached_df" not in st.session_state:
        save_config(data_dir=data_dir, soffice_path=soffice_str)
        with st.spinner("Wczytywanie danych..."):
            st.session_state["cached_df"] = load_all_data(data_dir, soffice_path)

    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.info("Brak danych w pamięci. Ustaw folder i kliknij **Wczytaj/odśwież dane**.")
    else:
        st.success(f"Wczytano {len(df):,} wierszy.".replace(",", " "))
        if show_preview:
            st.dataframe(df.head(300), use_container_width=True)

# ---------- Zakładka: Tabela przestawna ----------
with tab_pivot:
    st.subheader("📈 Tabela wskaźników")
    _cfg = load_config()
    data_dir = Path(_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
    soffice_path = Path(_cfg.get("soffice_path", "")) if _cfg.get("soffice_path", "").strip() else None

    if "cached_df" not in st.session_state or st.session_state["cached_df"].empty:
        with st.spinner("Wczytywanie danych z ostatniej zapamiętanej ścieżki..."):
            st.session_state["cached_df"] = load_all_data(data_dir, soffice_path)

    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Przejdź do zakładki **Dane** i wczytaj pliki.")
        st.stop()

    df = add__date_column(df)
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        st.caption(f"Źródło: **{data_dir}**")
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="pivot_date")
        if isinstance(picked, tuple) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask].copy()
    else:
        st.warning("Nie udało się odnaleźć kolumny daty — użyjemy wszystkich wierszy.")
        dff = df.copy()
        d_from = dff["__date"].min() if "__date" in dff.columns else ""
        d_to = dff["__date"].max() if "__date" in dff.columns else ""

    required_cols = {"UserFullName", "ProductName", "Quantity"}
    if not required_cols.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    import unicodedata
    def _norm_key(x):
        if x is None:
            return ""
        s = str(x)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower()
        s = "".join(ch for ch in s if ch.isalnum())
        return s

    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    users_sorted = sorted([u for u in dff["UserFullName"].dropna().unique()])

    # % Extra Sos
    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})
    extra_by_user = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum()
    base_by_user = dff.loc[mask_base].groupby("UserFullName")["Quantity"].sum()
    extra = extra_by_user.reindex(users_sorted, fill_value=0)
    base = base_by_user.reindex(users_sorted, fill_value=0)
    pct_extra = (extra / base.replace(0, pd.NA)) * 100
    pct_extra = pct_extra.astype("Float64").round(1)

    # % Popcorny smakowe
    flavored_list = ["BEKON-SER", "BEKON-SER/SOL", "CHEDDAR/SOL", "KARMEL.", "KARMEL/BEKON.", "KARMEL/CHEDDAR.", "KARMEL/SOL.", "SER-CHEDDAR"]
    base_pop_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l", "KubekPopcorn6,5l"]
    flavored_norm = set(_norm_key(x) for x in flavored_list)
    base_pop_norm = set(_norm_key(x) for x in base_pop_list)
    mask_flavored_pop = dff["__pnorm"].isin(flavored_norm)
    mask_base_pop = dff["__pnorm"].isin(base_pop_norm)
    flavored_by_user = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum()
    base_pop_by_user = dff.loc[mask_base_pop].groupby("UserFullName")["Quantity"].sum()
    flavored_qty = flavored_by_user.reindex(users_sorted, fill_value=0)
    base_pop_qty = base_pop_by_user.reindex(users_sorted, fill_value=0)
    pct_popcorny_smakowe = (flavored_qty / base_pop_qty.replace(0, pd.NA)) * 100
    pct_popcorny_smakowe = pct_popcorny_smakowe.astype("Float64").round(1)

    # % ShareCorn
    share_num_list = ["KubekPopcorn6,5l"]
    share_den_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l"]
    share_num_norm = set(_norm_key(x) for x in share_num_list)
    share_den_norm = set(_norm_key(x) for x in share_den_list)
    mask_share_num = dff["__pnorm"].isin(share_num_norm)
    mask_share_den = dff["__pnorm"].isin(share_den_norm)
    share_num_by_user = dff.loc[mask_share_num].groupby("UserFullName")["Quantity"].sum()
    share_den_by_user = dff.loc[mask_share_den].groupby("UserFullName")["Quantity"].sum()
    share_num_qty = share_num_by_user.reindex(users_sorted, fill_value=0)
    share_den_qty = share_den_by_user.reindex(users_sorted, fill_value=0)
    pct_sharecorn = (share_num_qty / share_den_qty.replace(0, pd.NA)) * 100
    pct_sharecorn = pct_sharecorn.astype("Float64").round(1)

    # Liczba transakcji i średnia wartość transakcji (z wykluczeniem POS)
    tx_df = dff.copy()
    warn_msgs = []
    if "PosName" in tx_df.columns:
        mask_excl = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~mask_excl].copy()
    else:
        warn_msgs.append("Brak kolumny PosName — liczba/średnia transakcji liczona bez wykluczeń POS.")

    has_txid = "TransactionId" in tx_df.columns
    has_net = "NetAmount" in tx_df.columns
    if not has_txid:
        warn_msgs.append("Brak kolumny TransactionId — nie można policzyć liczby/średniej transakcji.")
    if not has_net:
        warn_msgs.append("Brak kolumny NetAmount — nie można policzyć średniej wartości transakcji.")

    if has_txid:
        tx_count = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype("Int64")
    else:
        tx_count = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Int64")

    if has_txid and has_net:
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True)
        s = grp.sum(min_count=1)
        f = grp.first()
        per_tx_total = f.where(nun <= 1, s)
        revenue = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted).astype("Float64")
        avg_value = (revenue / tx_count.astype("Float64").replace(0, pd.NA)).astype("Float64").round(2)
    else:
        avg_value = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Float64")

    # Finalna tabela + kolejność kolumn + sortowanie
    result = pd.DataFrame(index=users_sorted)
    result["Liczba transakcji"] = tx_count.reindex(users_sorted)
    result["Średnia wartość transakcji"] = avg_value.reindex(users_sorted)
    result["% Extra Sos"] = pct_extra.reindex(users_sorted)
    result["% Popcorny smakowe"] = pct_popcorny_smakowe.reindex(users_sorted)
    result["% ShareCorn"] = pct_sharecorn.reindex(users_sorted)
    desired_order = ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn"]
    result = result[desired_order]
    result_sorted = result.sort_values(by="Średnia wartość transakcji", ascending=False, na_position="last")

    if warn_msgs:
        for m in warn_msgs:
            st.warning(m)

    # --- Wiersz: Średnia kina ---
    try:
        base_sum = float(base.sum(skipna=True))
        extra_sum = float(extra.sum(skipna=True))
        pct_extra_cinema = (extra_sum / base_sum * 100) if base_sum else None

        pop_base_sum = float(base_pop_qty.sum(skipna=True))
        pop_flav_sum = float(flavored_qty.sum(skipna=True))
        pct_popcorny_cinema = (pop_flav_sum / pop_base_sum * 100) if pop_base_sum else None

        share_den_sum = float(share_den_qty.sum(skipna=True))
        share_num_sum = float(share_num_qty.sum(skipna=True))
        pct_sharecorn_cinema = (share_num_sum / share_den_sum * 100) if share_den_sum else None

        if has_txid:
            global_tx_count = int(tx_df["TransactionId"].nunique())
        else:
            global_tx_count = None
        if has_txid and has_net:
            global_revenue = float(per_tx_total.sum(min_count=1))
            global_avg_value = (global_revenue / global_tx_count) if global_tx_count else None
        else:
            global_avg_value = None

        summary_row = pd.DataFrame({
            "Liczba transakcji": [global_tx_count],
            "Średnia wartość transakcji": [None if global_avg_value is None else round(global_avg_value, 2)],
            "% Extra Sos": [None if pct_extra_cinema is None else round(pct_extra_cinema, 1)],
            "% Popcorny smakowe": [None if pct_popcorny_cinema is None else round(pct_popcorny_cinema, 1)],
            "% ShareCorn": [None if pct_sharecorn_cinema is None else round(pct_sharecorn_cinema, 1)],
        }, index=["Średnia kina"])
        final_df = pd.concat([summary_row, result_sorted], axis=0)
    except Exception:
        final_df = result_sorted

    # --- Render tabeli ---
    try:
        import numpy as _np
        def _fmt_pct(x):
            return "" if x is None or (isinstance(x, float) and _np.isnan(x)) else f"{x:.1f} %"
        def _fmt_pln(x):
            if x is None or (isinstance(x, float) and _np.isnan(x)):
                return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
            return s + " zł"

        def _bold_and_shade(row):
            if row.name == "Średnia kina":
                return ['font-weight: 700; background-color: #f3f4f6' for _ in row]
            return ['' for _ in row]

        styled = (
            final_df
            .style
            .format({
                "% Extra Sos": _fmt_pct,
                "% Popcorny smakowe": _fmt_pct,
                "% ShareCorn": _fmt_pct,
                "Średnia wartość transakcji": _fmt_pln,
            })
            .apply(_bold_and_shade, axis=1)
        )
        st.dataframe(styled, use_container_width=True)
    except Exception:
        res_disp = final_df.copy()
        def _pln(x):
            import pandas as _pd
            if x is None or (isinstance(x, float) and _pd.isna(x)):
                return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
            return s + " zł"
        for col in ["% Extra Sos", "% Popcorny smakowe", "% ShareCorn"]:
            res_disp[col] = res_disp[col].map(lambda x: f"{x:.1f} %" if pd.notna(x) else "")
        res_disp["Średnia wartość transakcji"] = res_disp["Średnia wartość transakcji"].map(_pln)
        st.dataframe(res_disp, use_container_width=True)

    # --- Eksport do XLSX ---
    try:
        buffer = io.BytesIO()
        out_df = final_df.copy()
        try:
            import xlsxwriter  # noqa: F401
            engine_name = "xlsxwriter"
        except ModuleNotFoundError:
            engine_name = "openpyxl"
        with pd.ExcelWriter(buffer, engine=engine_name) as writer:
            out_df.to_excel(writer, index=True, sheet_name="Wskaźniki")
            if engine_name == "xlsxwriter":
                wb = writer.book
                ws = writer.sheets["Wskaźniki"]
                fmt_bold = wb.add_format({"bold": True})
                fmt_pct = wb.add_format({"num_format": "0.0 %"})
                fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
                fmt_int = wb.add_format({"num_format": "0"})
                col_names = ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn"]
                for j, name in enumerate(col_names, start=1):
                    width = 22 if name != "Liczba transakcji" else 18
                    if name == "Liczba transakcji":
                        ws.set_column(j, j, width, fmt_int)
                    elif name == "Średnia wartość transakcji":
                        ws.set_column(j, j, width, fmt_pln)
                    else:
                        ws.set_column(j, j, width, fmt_pct)
                ws.set_row(0, None, fmt_bold)
        st.download_button(
            "⬇️ Pobierz XLSX",
            data=buffer.getvalue(),
            file_name=f"Wskazniki_{d_from}_{d_to}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as _exp_err:
        st.warning(f"Nie udało się przygotować pliku XLSX do pobrania: {_exp_err}")

    # --- Szybkie zapytanie ---
    with st.expander("🎯 Szybkie zapytanie: zleceniobiorca + produkt", expanded=False):
        users = sorted([u for u in dff.get("UserFullName", pd.Series(dtype=str)).dropna().unique()])
        prods = sorted([p for p in dff.get("ProductName", pd.Series(dtype=str)).dropna().unique()])
        left, right = st.columns(2)
        with left:
            sel_user_simple = st.selectbox("Zleceniobiorca (UserFullName)", options=users, index=0 if users else None, placeholder="Wybierz osobę...")
        with right:
            sel_prod_simple = st.selectbox("Produkt (ProductName)", options=prods, index=0 if prods else None, placeholder="Wybierz produkt...")
        do_show = st.button("Pokaż", type="secondary")
        if do_show:
            required_cols = {"UserFullName", "ProductName", "Quantity"}
            if not required_cols.issubset(dff.columns):
                st.error("Brak wymaganych kolumn w danych.")
            elif sel_user_simple is None or sel_prod_simple is None:
                st.warning("Wybierz zarówno zleceniobiorcę jak i produkt.")
            else:
                subset2 = dff[(dff["UserFullName"] == sel_user_simple) & (dff["ProductName"] == sel_prod_simple)]
                total_qty2 = float(subset2["Quantity"].sum()) if not subset2.empty else 0.0
                st.metric(label="Suma sprzedanych sztuk (po filtrach daty)", value=f"{total_qty2:,.0f}".replace(",", " "))

# ---------- Zakładka: Wyniki indywidualne ----------
with tab_indy:
    st.subheader("👤 Wyniki indywidualne")

    _cfg = load_config()
    data_dir = Path(_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
    soffice_path = Path(_cfg.get("soffice_path", "")) if _cfg.get("soffice_path", "").strip() else None

    if "cached_df" not in st.session_state or st.session_state["cached_df"].empty:
        with st.spinner("Wczytywanie danych z ostatniej zapamiętanej ścieżki..."):
            st.session_state["cached_df"] = load_all_data(data_dir, soffice_path)

    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Przejdź do zakładki **Dane** i wczytaj pliki.")
        st.stop()

    df = add__date_column(df)
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        cols = st.columns([1,1,2])
        with cols[0]:
            picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="indy_date")
        with cols[1]:
            users_all = sorted([u for u in df.get("UserFullName", pd.Series(dtype=str)).dropna().unique()])
            sel_user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None, key="indy_user")
        d_from, d_to = (picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d))
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask].copy()
    else:
        st.warning("Nie udało się odnaleźć kolumny daty — użyjemy wszystkich wierszy.")
        dff = df.copy()
        users_all = sorted([u for u in df.get("UserFullName", pd.Series(dtype=str)).dropna().unique()])
        sel_user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None, key="indy_user_nodate")
        d_from = dff["__date"].min() if "__date" in dff.columns else ""
        d_to = dff["__date"].max() if "__date" in dff.columns else ""

    if not users_all:
        st.info("Brak nazw w kolumnie **UserFullName**.")
        st.stop()

    import unicodedata
    def _norm_key(x):
        if x is None:
            return ""
        s = str(x)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower()
        s = "".join(ch for ch in s if ch.isalnum())
        return s
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)

    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})

    flavored_list = ["BEKON-SER", "BEKON-SER/SOL", "CHEDDAR/SOL", "KARMEL.", "KARMEL/BEKON.", "KARMEL/CHEDDAR.", "KARMEL/SOL.", "SER-CHEDDAR"]
    base_pop_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l", "KubekPopcorn6,5l"]
    flavored_norm = set(_norm_key(x) for x in flavored_list)
    base_pop_norm = set(_norm_key(x) for x in base_pop_list)
    mask_flavored_pop = dff["__pnorm"].isin(flavored_norm)
    mask_base_pop = dff["__pnorm"].isin(base_pop_norm)

    share_num_list = ["KubekPopcorn6,5l"]
    share_den_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l"]
    share_num_norm = set(_norm_key(x) for x in share_num_list)
    share_den_norm = set(_norm_key(x) for x in share_den_list)
    mask_share_num = dff["__pnorm"].isin(share_num_norm)
    mask_share_den = dff["__pnorm"].isin(share_den_norm)

    # --- KINO (średnie w wybranym okresie) ---
    try:
        base_sum = float(dff.loc[mask_base, "Quantity"].sum())
        extra_sum = float(dff.loc[mask_extra, "Quantity"].sum())
        pct_extra_cinema = (extra_sum / base_sum * 100) if base_sum else None

        pop_base_sum = float(dff.loc[mask_base_pop, "Quantity"].sum())
        pop_flav_sum = float(dff.loc[mask_flavored_pop, "Quantity"].sum())
        pct_popcorny_cinema = (pop_flav_sum / pop_base_sum * 100) if pop_base_sum else None

        share_den_sum = float(dff.loc[mask_share_den, "Quantity"].sum())
        share_num_sum = float(dff.loc[mask_share_num, "Quantity"].sum())
        pct_sharecorn_cinema = (share_num_sum / share_den_sum * 100) if share_den_sum else None

        tx_df_all = dff.copy()
        if "PosName" in tx_df_all.columns:
            m_ex = tx_df_all["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
            tx_df_all = tx_df_all.loc[~m_ex].copy()
        has_txid = "TransactionId" in tx_df_all.columns
        has_net = "NetAmount" in tx_df_all.columns
        if has_txid and has_net:
            grp_all = tx_df_all.groupby("TransactionId")["NetAmount"]
            nun_all = grp_all.nunique(dropna=True)
            s_all = grp_all.sum(min_count=1)
            f_all = grp_all.first()
            per_tx_total_all = f_all.where(nun_all <= 1, s_all)
            global_tx_count = int(tx_df_all["TransactionId"].nunique())
            global_revenue = float(per_tx_total_all.sum(min_count=1))
            avg_tr_cinema = (global_revenue / global_tx_count) if global_tx_count else None
        else:
            avg_tr_cinema = None
    except Exception:
        pct_extra_cinema = pct_popcorny_cinema = pct_sharecorn_cinema = avg_tr_cinema = None

    # --- OSOBA ---
    dff_u = dff[dff["UserFullName"] == sel_user].copy()
    tx_count_u = None
    try:
        base_u = float(dff_u.loc[mask_base, "Quantity"].sum())
        extra_u = float(dff_u.loc[mask_extra, "Quantity"].sum())
        pct_extra_u = (extra_u / base_u * 100) if base_u else None

        pop_base_u = float(dff_u.loc[mask_base_pop, "Quantity"].sum())
        pop_flav_u = float(dff_u.loc[mask_flavored_pop, "Quantity"].sum())
        pct_popcorny_u = (pop_flav_u / pop_base_u * 100) if pop_base_u else None

        share_den_u = float(dff_u.loc[mask_share_den, "Quantity"].sum())
        share_num_u = float(dff_u.loc[mask_share_num, "Quantity"].sum())
        pct_sharecorn_u = (share_num_u / share_den_u * 100) if share_den_u else None

        tx_df_u = dff_u.copy()
        if "PosName" in tx_df_u.columns:
            m_ex_u = tx_df_u["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
            tx_df_u = tx_df_u.loc[~m_ex_u].copy()
        has_txid_u = "TransactionId" in tx_df_u.columns
        has_net_u = "NetAmount" in tx_df_u.columns
        if has_txid_u and has_net_u:
            grp_u = tx_df_u.groupby("TransactionId")["NetAmount"]
            nun_u = grp_u.nunique(dropna=True)
            s_u = grp_u.sum(min_count=1)
            f_u = grp_u.first()
            per_tx_total_u = f_u.where(nun_u <= 1, s_u)
            tx_count_u = int(tx_df_u["TransactionId"].nunique())
            revenue_u = float(per_tx_total_u.sum(min_count=1))
            avg_tr_u = (revenue_u / tx_count_u) if tx_count_u else None
        else:
            avg_tr_u = None
    except Exception:
        pct_extra_u = pct_popcorny_u = pct_sharecorn_u = avg_tr_u = None

    # --- Prezentacja wyników ---
    def _fmt_pct(x):
        return "" if x is None else f"{x:.1f} %"
    def _fmt_pln(x):
        return "" if x is None else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
    def _fmt_diff_pct(u, c):
        if u is None or c is None:
            return ""
        diff = u - c
        sign = "+" if diff >= 0 else "−"
        return f"{sign}{abs(diff):.1f} p.p."
    def _fmt_diff_pln(u, c):
        if u is None or c is None:
            return ""
        diff = u - c
        sign = "+" if diff >= 0 else "−"
        s = f"{abs(diff):,.2f}".replace(",", " ").replace(".", ",")
        return f"{sign}{s} zł"

    rows = [
        ["Średnia wartość transakcji", avg_tr_u, avg_tr_cinema, _fmt_diff_pln(avg_tr_u, avg_tr_cinema)],
        ["% Extra Sos", pct_extra_u, pct_extra_cinema, _fmt_diff_pct(pct_extra_u, pct_extra_cinema)],
        ["% Popcorny smakowe", pct_popcorny_u, pct_popcorny_cinema, _fmt_diff_pct(pct_popcorny_u, pct_popcorny_cinema)],
        ["% ShareCorn", pct_sharecorn_u, pct_sharecorn_cinema, _fmt_diff_pct(pct_sharecorn_u, pct_sharecorn_cinema)],
    ]
    df_view = pd.DataFrame(rows, columns=["Wskaźnik", sel_user, "Średnia kina", "Δ vs kino"])
    disp = df_view.copy()
    disp.loc[disp["Wskaźnik"] == "Średnia wartość transakcji", sel_user] = disp.loc[disp["Wskaźnik"] == "Średnia wartość transakcji", sel_user].map(_fmt_pln)
    disp.loc[disp["Wskaźnik"] == "Średnia wartość transakcji", "Średnia kina"] = disp.loc[disp["Wskaźnik"] == "Średnia wartość transakcji", "Średnia kina"].map(_fmt_pln)
    mask_pct_rows = disp["Wskaźnik"] != "Średnia wartość transakcji"
    disp.loc[mask_pct_rows, sel_user] = disp.loc[mask_pct_rows, sel_user].map(_fmt_pct)
    disp.loc[mask_pct_rows, "Średnia kina"] = disp.loc[mask_pct_rows, "Średnia kina"].map(_fmt_pct)

    st.markdown(f"**Okres:** {d_from} → {d_to}")
    st.markdown(f"**Zleceniobiorca:** {sel_user}")
    # Metryka liczby transakcji
    _tx_val = "-" if tx_count_u is None else f"{tx_count_u:,}".replace(",", " ")
    st.metric("Liczba transakcji", _tx_val)
    st.dataframe(disp, use_container_width=True, hide_index=True)

    # --- Wykresy ---
    st.markdown("### 📊 Wykresy porównawcze")

    # Wykres pieniędzy
    val_user = avg_tr_u if avg_tr_u is not None else 0.0
    val_kino = avg_tr_cinema if avg_tr_cinema is not None else 0.0
    st.caption("Średnia wartość transakcji")
    import altair as alt
    _green = "#16a34a"
    _red = "#dc2626"
    _gray = "#6b7280"
    _user_color = _gray
    if avg_tr_cinema is not None and avg_tr_u is not None:
        _user_color = _green if avg_tr_u >= avg_tr_cinema else _red
    def _fmt_pl_diff(v):
        s = f"{abs(v):,.2f}".replace(",", " ").replace(".", ",")
        return s + " zł"
    _diff_money = None
    if (avg_tr_cinema is not None) and (avg_tr_u is not None):
        _d = avg_tr_u - avg_tr_cinema
        _diff_money = ("+" if _d >= 0 else "−") + _fmt_pl_diff(_d)
    df_chart_money = pd.DataFrame([
        {"Kto": sel_user, "Wartość": val_user, "kolor": _user_color, "label": (_diff_money or ""), "label_color": _user_color},
        {"Kto": "Średnia kina", "Wartość": val_kino, "kolor": _gray, "label": "", "label_color": _gray},
    ])
    base_money = alt.Chart(df_chart_money)
    bars_money = base_money.mark_bar(size=28).encode(
        x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
        y=alt.Y("Wartość:Q", title="zł"),
        color=alt.Color("kolor:N", legend=None, scale=None),
        tooltip=[alt.Tooltip("Kto:N"), alt.Tooltip("Wartość:Q", format=",.2f")]
    )
    labels_money = base_money.mark_text(dy=-6, size=18).encode(
        x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
        y=alt.Y("Wartość:Q"),
        text=alt.Text("label:N"),
        color=alt.Color("label_color:N", legend=None, scale=None)
    )
    _ref_df = pd.DataFrame({"ref":[val_kino]})
    rule_money = alt.Chart(_ref_df).mark_rule(strokeDash=[6,4], color=_gray, opacity=0.8).encode(y="ref:Q")
    chart_money = (bars_money + labels_money + rule_money).properties(width=780, height=450)
    st.altair_chart(chart_money, use_container_width=False)

    # Wskaźniki % z linią odniesienia
    st.caption("Wskaźniki procentowe")
    def _fmt_pp(v):
        s = f"{abs(v):.1f}".replace(".", ",")
        return s + " p.p."
    rows = []
    _metrics = ["% Extra Sos", "% Popcorny smakowe", "% ShareCorn"]
    _user_vals_raw = [pct_extra_u, pct_popcorny_u, pct_sharecorn_u]
    _kino_vals_raw = [pct_extra_cinema, pct_popcorny_cinema, pct_sharecorn_cinema]
    for m, uraw, craw in zip(_metrics, _user_vals_raw, _kino_vals_raw):
        uval = uraw if uraw is not None else 0.0
        cval = craw if craw is not None else 0.0
        _ucol = _gray
        if (uraw is not None) and (craw is not None):
            _ucol = _green if uval >= cval else _red
            _diff = uval - cval
            _label = ("+" if _diff >= 0 else "−") + _fmt_pp(_diff)
        else:
            _label = ""
        rows.append({"Wskaźnik": m, "Kto": sel_user, "Wartość": uval, "kolor": _ucol, "diff_label": _label, "label_color": _ucol})
        rows.append({"Wskaźnik": m, "Kto": "Średnia kina", "Wartość": cval, "kolor": _gray, "diff_label": "", "label_color": _gray})
    df_chart_pct = pd.DataFrame(rows)
    base_pct = alt.Chart(df_chart_pct)
    bars_pct = base_pct.mark_bar(size=28).encode(
        x=alt.X("Kto:N", title="", sort=[sel_user, "Średnia kina"]),
        y=alt.Y("Wartość:Q", title="%"),
        color=alt.Color("kolor:N", legend=None, scale=None),
        tooltip=[alt.Tooltip("Wskaźnik:N"), alt.Tooltip("Kto:N"), alt.Tooltip("Wartość:Q", format=".1f")]
    )
    labels_pct = base_pct.mark_text(dy=-6, size=16).encode(
        x=alt.X("Kto:N", title="", sort=[sel_user, "Średnia kina"]),
        y=alt.Y("Wartość:Q"),
        text=alt.Text("diff_label:N"),
        color=alt.Color("label_color:N", legend=None, scale=None)
    )
    rule_pct = base_pct.transform_filter(alt.datum.Kto == "Średnia kina").mark_rule(
        strokeDash=[6,4], color=_gray, opacity=0.8
    ).encode(
        y="Wartość:Q"
    )
    chart_pct = (bars_pct + labels_pct + rule_pct).properties(width=360, height=480).facet(
        column=alt.Column("Wskaźnik:N", header=alt.Header(title=None))
    )
    st.altair_chart(chart_pct, use_container_width=True)

# ---------- Zakładka: Najlepsi ----------
with tab_best:
    st.subheader("🏆 Najlepsi — ranking wg wskaźników")

    _cfg = load_config()
    data_dir = Path(_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
    soffice_path = Path(_cfg.get("soffice_path", "")) if _cfg.get("soffice_path", "").strip() else None
    if "cached_df" not in st.session_state or st.session_state["cached_df"].empty:
        with st.spinner("Wczytywanie danych z ostatniej zapamiętanej ścieżki..."):
            st.session_state["cached_df"] = load_all_data(data_dir, soffice_path)

    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Przejdź do zakładki **Dane** i wczytaj pliki.")
        st.stop()

    df = add__date_column(df)
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="best_date")
        if isinstance(picked, tuple) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask].copy()
    else:
        st.warning("Nie udało się odnaleźć kolumny daty — użyjemy wszystkich wierszy.")
        dff = df.copy()

    required_cols = {"UserFullName", "ProductName", "Quantity"}
    if not required_cols.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    import unicodedata
    def _norm_key(x):
        if x is None:
            return ""
        s = str(x)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower()
        s = "".join(ch for ch in s if ch.isalnum())
        return s
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)

    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})

    flavored_list = ["BEKON-SER", "BEKON-SER/SOL", "CHEDDAR/SOL", "KARMEL.", "KARMEL/BEKON.", "KARMEL/CHEDDAR.", "KARMEL/SOL.", "SER-CHEDDAR"]
    base_pop_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l", "KubekPopcorn6,5l"]
    flavored_norm = set(_norm_key(x) for x in flavored_list)
    base_pop_norm = set(_norm_key(x) for x in base_pop_list)
    mask_flavored_pop = dff["__pnorm"].isin(flavored_norm)
    mask_base_pop = dff["__pnorm"].isin(base_pop_norm)

    share_num_list = ["KubekPopcorn6,5l"]
    share_den_list = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l"]
    share_num_norm = set(_norm_key(x) for x in share_num_list)
    share_den_norm = set(_norm_key(x) for x in share_den_list)
    mask_share_num = dff["__pnorm"].isin(share_num_norm)
    mask_share_den = dff["__pnorm"].isin(share_den_norm)

    users_sorted = sorted([u for u in dff["UserFullName"].dropna().unique()])

    def style_over_avg(df_in: pd.DataFrame, avg_val: float, is_pct: bool) -> pd.io.formats.style.Styler:
        import numpy as _np
        def _fmt_pct(x):
            return "" if x is None or (isinstance(x, float) and _np.isnan(x)) else f"{x:.1f} %"
        def _fmt_pln(x):
            if x is None or (isinstance(x, float) and _np.isnan(x)):
                return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
            return s + " zł"
        def _color(v):
            try:
                return "background-color: #dcfce7; font-weight: 600" if (v is not None and not pd.isna(v) and (avg_val is not None) and (not pd.isna(avg_val)) and v >= avg_val) else ""
            except Exception:
                return ""
        sty = df_in.style.applymap(_color, subset=["Wartość"])
        if is_pct:
            sty = sty.format({"Wartość": _fmt_pct})
        else:
            sty = sty.format({"Wartość": _fmt_pln})
        return sty

    # % Extra Sos
    extra_by_user = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum()
    base_by_user = dff.loc[mask_base].groupby("UserFullName")["Quantity"].sum()
    extra = extra_by_user.reindex(users_sorted, fill_value=0)
    base = base_by_user.reindex(users_sorted, fill_value=0)
    tbl_extra = (extra / base.replace(0, pd.NA) * 100).astype("Float64")
    avg_extra = (float(dff.loc[mask_extra, "Quantity"].sum()) / float(dff.loc[mask_base, "Quantity"].sum()) * 100) if dff.loc[mask_base, "Quantity"].sum() else None
    df_extra = pd.DataFrame({"Wartość": tbl_extra}).sort_values("Wartość", ascending=False, na_position="last")
    st.markdown("#### % Extra Sos")
    if avg_extra is not None:
        st.caption(f"Średnia kina: **{avg_extra:.1f} %**")
    st.dataframe(style_over_avg(df_extra, avg_extra, is_pct=True), use_container_width=True)

    # % Popcorny smakowe
    flavored_by_user = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum()
    base_pop_by_user = dff.loc[mask_base_pop].groupby("UserFullName")["Quantity"].sum()
    flavored_qty = flavored_by_user.reindex(users_sorted, fill_value=0)
    base_pop_qty = base_pop_by_user.reindex(users_sorted, fill_value=0)
    tbl_pop = (flavored_qty / base_pop_qty.replace(0, pd.NA) * 100).astype("Float64")
    avg_pop = (float(dff.loc[mask_flavored_pop, "Quantity"].sum()) / float(dff.loc[mask_base_pop, "Quantity"].sum()) * 100) if dff.loc[mask_base_pop, "Quantity"].sum() else None
    df_pop = pd.DataFrame({"Wartość": tbl_pop}).sort_values("Wartość", ascending=False, na_position="last")
    st.markdown("#### % Popcorny smakowe")
    if avg_pop is not None:
        st.caption(f"Średnia kina: **{avg_pop:.1f} %**")
    st.dataframe(style_over_avg(df_pop, avg_pop, is_pct=True), use_container_width=True)

    # % ShareCorn
    share_num_by_user = dff.loc[mask_share_num].groupby("UserFullName")["Quantity"].sum()
    share_den_by_user = dff.loc[mask_share_den].groupby("UserFullName")["Quantity"].sum()
    share_num_qty = share_num_by_user.reindex(users_sorted, fill_value=0)
    share_den_qty = share_den_by_user.reindex(users_sorted, fill_value=0)
    tbl_share = (share_num_qty / share_den_qty.replace(0, pd.NA) * 100).astype("Float64")
    den_sum = float(dff.loc[mask_share_den, "Quantity"].sum())
    num_sum = float(dff.loc[mask_share_num, "Quantity"].sum())
    avg_share = (num_sum / den_sum * 100) if den_sum else None
    df_share = pd.DataFrame({"Wartość": tbl_share}).sort_values("Wartość", ascending=False, na_position="last")
    st.markdown("#### % ShareCorn")
    if avg_share is not None:
        st.caption(f"Średnia kina: **{avg_share:.1f} %**")
    st.dataframe(style_over_avg(df_share, avg_share, is_pct=True), use_container_width=True)

    # Średnia wartość transakcji
    tx_df = dff.copy()
    if "PosName" in tx_df.columns:
        mask_excl = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~mask_excl].copy()
    has_txid = "TransactionId" in tx_df.columns
    has_net = "NetAmount" in tx_df.columns

    st.markdown("#### Średnia wartość transakcji")
    if has_txid and has_net:
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True)
        s = grp.sum(min_count=1)
        f = grp.first()
        per_tx_total = f.where(nun <= 1, s)
        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique()
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64")

        grp_all = tx_df.groupby("TransactionId")["NetAmount"]
        nun_all = grp_all.nunique(dropna=True)
        s_all = grp_all.sum(min_count=1)
        f_all = grp_all.first()
        per_tx_total_all = f_all.where(nun_all <= 1, s_all)
        global_tx_count = tx_df["TransactionId"].nunique()
        global_revenue = float(per_tx_total_all.sum(min_count=1))
        avg_global = (global_revenue / global_tx_count) if global_tx_count else None

        df_avg = pd.DataFrame({"Wartość": avg_by_user.reindex(users_sorted)}).sort_values("Wartość", ascending=False, na_position="last")
        if avg_global is not None:
            st.caption(f"Średnia kina: **{avg_global:,.2f} zł**".replace(",", " ").replace(".", ","))
        st.dataframe(style_over_avg(df_avg, avg_global, is_pct=False), use_container_width=True)
    else:
        st.info("Brak kolumn TransactionId lub NetAmount — nie można policzyć średniej wartości transakcji.")

# ---------- Zakładka: Kreator Konkursów ----------
with tab_comp:
    st.subheader("🧮 Kreator Konkursów")

    # Dane i zakres
    _cfg = load_config()
    data_dir = Path(_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
    soffice_path = Path(_cfg.get("soffice_path", "")) if _cfg.get("soffice_path", "").strip() else None
    if "cached_df" not in st.session_state or st.session_state["cached_df"].empty:
        with st.spinner("Wczytywanie danych z ostatniej zapamiętanej ścieżki..."):
            st.session_state["cached_df"] = load_all_data(data_dir, soffice_path)
    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Przejdź do zakładki **Dane** i wczytaj pliki.")
        st.stop()

    df = add__date_column(df)
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="contest_date")
        if isinstance(picked, tuple) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask].copy()
    else:
        st.warning("Nie udało się odnaleźć kolumny daty — użyjemy wszystkich wierszy.")
        dff = df.copy()
        d_from = dff["__date"].min() if "__date" in dff.columns else ""
        d_to = dff["__date"].max() if "__date" in dff.columns else ""

    required_cols = {"UserFullName", "ProductName", "Quantity"}
    if not required_cols.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    products_all = sorted([p for p in dff.get("ProductName", pd.Series(dtype=str)).dropna().unique()])
    users_sorted = sorted([u for u in dff.get("UserFullName", pd.Series(dtype=str)).dropna().unique()])

    # --- Grupa produktów: Popcorny Smakowe ---
    import unicodedata
    def _norm_key(x):
        if x is None:
            return ""
        s = str(x)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower()
        s = "".join(ch for ch in s if ch.isalnum())
        return s
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    _flavored_list = ["BEKON-SER", "BEKON-SER/SOL", "CHEDDAR/SOL", "KARMEL.", "KARMEL/BEKON.", "KARMEL/CHEDDAR.", "KARMEL/SOL.", "SER-CHEDDAR"]
    _flavored_norm = set(_norm_key(x) for x in _flavored_list)
    mask_flavored_pop = dff["__pnorm"].isin(_flavored_norm)
    GROUP_FLAVORED = "Popcorny Smakowe"
    products_all_ext = [GROUP_FLAVORED] + products_all

    st.markdown("### 2–3) Produkty i punktacja")
    if "contest_products" not in st.session_state:
        st.session_state["contest_products"] = [None]
        st.session_state["contest_points"] = [1.0]

    cols_btn = st.columns([1,1,4])
    with cols_btn[0]:
        if st.button("➕ Dodaj produkt"):
            st.session_state["contest_products"].append(None)
            st.session_state["contest_points"].append(1.0)
    with cols_btn[1]:
        if st.button("➖ Usuń ostatni", disabled=len(st.session_state["contest_products"])<=1):
            st.session_state["contest_products"].pop()
            st.session_state["contest_points"].pop()

    for i, _ in enumerate(st.session_state["contest_products"]):
        c1, c2 = st.columns([3,1])
        with c1:
            st.session_state["contest_products"][i] = st.selectbox(
                f"Produkt #{i+1}",
                options=products_all_ext,
                index=(products_all_ext.index(st.session_state['contest_products'][i]) if st.session_state['contest_products'][i] in products_all_ext else None),
                placeholder="Wybierz produkt...",
                key=f"contest_prod_{i}"
            )
        with c2:
            st.session_state["contest_points"][i] = st.number_input(
                f"Punkty #{i+1}",
                min_value=-10000.0, max_value=10000.0, value=float(st.session_state["contest_points"][i]), step=0.5,
                key=f"contest_pts_{i}"
            )

    st.divider()

    st.markdown("### 5) Mianownik współczynnika")
    den_mode = st.selectbox("Wybierz mianownik", ["Liczba transakcji", "Wybrany produkt", "Stała 1"], key="contest_den_mode")
    den_prod = None
    if den_mode == "Wybrany produkt":
        den_prod = st.selectbox("Produkt dla mianownika", options=products_all_ext, placeholder="Wybierz produkt...", key="contest_den_prod")
    if den_mode == "Liczba transakcji":
        st.caption("Liczba unikatowych TransactionId po wykluczeniu POS: Bonarka CAF1/VIP1.")

    if st.button("🧮 Oblicz ranking", type="primary"):
        pairs = []
        for prod, pts in zip(st.session_state["contest_products"], st.session_state["contest_points"]):
            if prod is not None and pts is not None and float(pts) != 0.0:
                pairs.append((prod, float(pts)))
        if not pairs:
            st.warning("Dodaj co najmniej jedną pozycję z niezerową punktacją.")
            st.stop()

        import pandas as _pd
        num = _pd.Series(0.0, index=users_sorted, dtype="float")
        for prod, pts in pairs:
            if prod == GROUP_FLAVORED:
                s = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum()
            else:
                s = dff.loc[dff["ProductName"] == prod].groupby("UserFullName")["Quantity"].sum()
            s = s.reindex(users_sorted, fill_value=0).astype(float) * pts
            num = num.add(s, fill_value=0.0)

        if den_mode == "Liczba transakcji":
            tx_df = dff.copy()
            if "PosName" in tx_df.columns:
                mask_excl = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
                tx_df = tx_df.loc[~mask_excl].copy()
            if "TransactionId" not in tx_df.columns:
                st.error("Brak kolumny TransactionId — nie można użyć mianownika 'Liczba transakcji'.")
                st.stop()
            den = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype(float)
        elif den_mode == "Wybrany produkt":
            if not den_prod:
                st.error("Wybierz produkt dla mianownika.")
                st.stop()
            if den_prod == GROUP_FLAVORED:
                den = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0).astype(float)
            else:
                den = dff.loc[dff["ProductName"] == den_prod].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0).astype(float)
        else:
            den = _pd.Series(1.0, index=users_sorted, dtype="float")

        res = (num / den.replace(0, _pd.NA)).astype("Float64")
        wynik_pct = (res * 100).astype("Float64")

        # Liczba transakcji (po wykluczeniu POS), niezależnie od mianownika
        tx_df_all = dff.copy()
        if "PosName" in tx_df_all.columns:
            _m_ex_all = tx_df_all["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
            tx_df_all = tx_df_all.loc[~_m_ex_all].copy()
        if "TransactionId" in tx_df_all.columns:
            tx_count_all = tx_df_all.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0)
        else:
            tx_count_all = _pd.Series([_pd.NA]*len(users_sorted), index=users_sorted, dtype="Float64")

        out = _pd.DataFrame({
            "Wynik": res,
            "Wynik (%)": wynik_pct,
            "Licznik (pkt)": num,
            "Mianownik": den,
            "Transakcje": tx_count_all
        }).sort_values("Wynik", ascending=False, na_position="last")

        # Dodaj kolumnę miejsca (1,2,3,...) na początku
        out.insert(0, "Miejsce", range(1, len(out) + 1))

        # Formatowanie do wyświetlenia
        disp = out.copy()
        def _fmt2(x):
            try:
                import math
                if x is None or (hasattr(x, "__float__") and math.isnan(float(x))):
                    return ""
            except Exception:
                pass
            return f"{float(x):,.2f}".replace(",", " ").replace(".", ",")
        def _fmt_pct(x):
            try:
                import math
                if x is None or (hasattr(x, "__float__") and math.isnan(float(x))):
                    return ""
            except Exception:
                pass
            return f"{float(x):,.1f} %".replace(".", ",")
        def _fmt_int(x):
            try:
                import math
                v = int(x)
                return f"{v:,}".replace(",", " ")
            except Exception:
                return ""

        disp["Wynik (%)"] = disp["Wynik (%)"].map(_fmt_pct)
        disp["Licznik (pkt)"] = disp["Licznik (pkt)"].map(_fmt2)
        disp["Mianownik"] = disp["Mianownik"].map(_fmt2)
        disp["Transakcje"] = disp["Transakcje"].map(_fmt_int)

        st.markdown(f"**Okres:** {d_from} → {d_to}")
        st.markdown("**Ranking zleceniobiorców (malejąco):**")

        # Wyświetlenie: Miejsce z ikonami medali i kolorowaniem TOP3
        disp2 = disp.reset_index(names="Zleceniobiorca")
        disp2 = disp2[["Miejsce", "Zleceniobiorca", "Wynik (%)", "Licznik (pkt)", "Mianownik", "Transakcje"]]
        def _place_with_medal(m):
            try:
                mi = int(m)
            except Exception:
                return m
            return f"{mi} 🥇" if mi == 1 else (f"{mi} 🥈" if mi == 2 else (f"{mi} 🥉" if mi == 3 else f"{mi}"))
        disp2["Miejsce"] = disp2["Miejsce"].map(_place_with_medal)
        disp2 = disp2.reset_index(drop=True)

        def _row_style_top3(row):
            try:
                import re as _re
                m = int(_re.match(r"\d+", str(row["Miejsce"])).group(0))
            except Exception:
                return [""] * len(row)
            if m == 1:
                style = "background-color:#fff4b8; font-weight:700"
            elif m == 2:
                style = "background-color:#e5e7eb; font-weight:600"
            elif m == 3:
                style = "background-color:#fde7c2; font-weight:600"
            else:
                style = ""
            return [style] * len(row)
        sty = disp2.style.apply(_row_style_top3, axis=1)
        try:
            sty = sty.hide(axis="index")
        except Exception:
            pass
        st.dataframe(sty, use_container_width=True, hide_index=True)

        # Eksport do XLSX (z wartościami liczbowymi)
        try:
            import io
            buf = io.BytesIO()
            export_df = out.reset_index().rename(columns={"index":"Zleceniobiorca"})
            export_df = export_df[["Miejsce", "Zleceniobiorca", "Wynik (%)", "Licznik (pkt)", "Mianownik", "Transakcje"]].copy()
            try:
                import xlsxwriter  # noqa: F401
                engine_name = "xlsxwriter"
            except ModuleNotFoundError:
                engine_name = "openpyxl"
            with pd.ExcelWriter(buf, engine=engine_name) as writer:
                export_df.to_excel(writer, index=False, sheet_name="Ranking")
                if engine_name == "xlsxwriter":
                    wb = writer.book
                    ws = writer.sheets["Ranking"]
                    fmt_pct = wb.add_format({"num_format": "0.0 %"})
                    fmt_num = wb.add_format({"num_format": "0.00"})
                    fmt_int = wb.add_format({"num_format": "0"})
                    ws.set_column("A:A", 9, fmt_int)   # Miejsce
                    ws.set_column("B:B", 28)           # Zleceniobiorca
                    ws.set_column("C:C", 12, fmt_pct)  # Wynik (%)
                    ws.set_column("D:D", 16, fmt_num)  # Licznik (pkt)
                    ws.set_column("E:E", 14, fmt_num)  # Mianownik
                    ws.set_column("F:F", 13, fmt_int)  # Transakcje
                    fmt_header = wb.add_format({"bold": True})
                    ws.set_row(0, None, fmt_header)

            st.download_button(
                "⬇️ Pobierz ranking (XLSX)",
                data=buf.getvalue(),
                file_name=f"Konkurs_{d_from}_{d_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as _ex:
            st.warning(f"Nie udało się przygotować eksportu XLSX: {_ex}")
