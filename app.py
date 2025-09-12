import os
import re
import io
import zipfile
import shutil
import tempfile
import json
from pathlib import Path
from datetime import date

import pandas as pd
import streamlit as st
import altair as alt

# =================== DEPLOY NOTE (Streamlit Cloud) ===================
# 1) Wgraj pliki: app.py + requirements.txt (+ runtime.txt) do GitHub.
# 2) Streamlit Community Cloud → New app → wskaż repo/branch/app.py.
# 3) (Opcjonalnie) hasło: Settings → Secrets dodaj: PASSWORD="TwojeHaslo".
# 4) W chmurze korzystaj z trybu "Wgrywanie plików" (zakładka "Dane").
# =====================================================================

st.set_page_config(page_title="CineStats — sprzedaż i wskaźniki", layout="wide")
st.title("🎬 CineStats — sprzedaż i wskaźniki")

# ---------- Prosta ochrona hasłem (opcjonalna) ----------
if "PASSWORD" in st.secrets:
    if "AUTHED" not in st.session_state:
        pw = st.text_input("Hasło", type="password")
        ok = st.button("Zaloguj")
        if ok:
            if pw == st.secrets["PASSWORD"]:
                st.session_state["AUTHED"] = True
                st.rerun()
            else:
                st.error("Nieprawidłowe hasło.")
        st.stop()

# ---------- Konfig lokalny ----------
APP_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = APP_DIR / "Dane"
CONFIG_PATH = APP_DIR / ".sprzedaz_config.json"

def load_config() -> dict:
    cfg = {"data_dir": str(DEFAULT_DATA_DIR)}
    try:
        if CONFIG_PATH.exists():
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
    except Exception as e:
        st.warning(f"Nie udało się wczytać konfiguracji: {e}")
    return cfg

def save_config(data_dir: Path) -> None:
    try:
        tmp = CONFIG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps({"data_dir": str(data_dir)}, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, CONFIG_PATH)
    except Exception as e:
        st.warning(f"Nie udało się zapisać konfiguracji: {e}")

# ---------- Pomocnicze: naprawa xlsx ----------
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

# ---------- Wczytywanie danych ----------
def _read_csv_any(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        return pd.read_csv(path)

def read_any_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".csv", ".txt"]:
        return _read_csv_any(path)
    if ext in [".xlsx"]:
        last_err = None
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            last_err = e
        rep = repair_xlsx_zip(path)
        if rep and rep.exists():
            try:
                return pd.read_excel(rep, engine="openpyxl")
            except Exception as e:
                last_err = e
        raise RuntimeError(f"Nie udało się odczytać pliku {path.name}. Ostatni błąd: {last_err}")
    if ext in [".xls"]:
        raise RuntimeError("Format .xls nieobsługiwany w tej wersji online. Zapisz jako .xlsx lub .csv.")
    raise ValueError(f"Nieobsługiwane rozszerzenie pliku: {ext}")

def _dedup_repaired(paths: list[Path]) -> list[Path]:
    bycanon, others = {}, []
    for p in paths:
        if p.suffix.lower() == ".xlsx" and (p.name.endswith(".repaired.xlsx") or p.name.endswith(".xlsx")):
            canon = p.name[:-len(".repaired.xlsx")] + ".xlsx" if p.name.endswith(".repaired.xlsx") else p.name
            bycanon.setdefault(canon, []).append(p)
        else:
            others.append(p)
    out = []
    for canon, plist in bycanon.items():
        rep = [pp for pp in plist if pp.name.endswith(".repaired.xlsx")]
        out.append(sorted(rep)[0] if rep else sorted(plist)[0])
    out.extend(others)
    return sorted(out)

@st.cache_data(show_spinner=False)
def _load_from_paths(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    frames, failures = [], []
    for p in files:
        try:
            df = read_any_table(p)
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

@st.cache_data(show_spinner=False)
def load_all_data_from_dir(data_dir: Path) -> pd.DataFrame:
    files = []
    for patt in ("*.xlsx", "*.csv", "*.txt"):
        files.extend(sorted(data_dir.glob(patt)))
    files = _dedup_repaired(files)
    return _load_from_paths(files)

def save_uploads_to_tmp(uploaded_files) -> list[Path]:
    tmpdir = Path(tempfile.mkdtemp(prefix="uploads_"))
    out = []
    for uf in uploaded_files:
        target = tmpdir / uf.name
        with open(target, "wb") as f:
            f.write(uf.read())
        out.append(target)
    return _dedup_repaired(out)

# ---------- Data helpers ----------
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
            dates.append(_date_from_filename(str(fname)))
        out["__date"] = dates
    return out

# ---------- Normalizacja nazw produktów ----------
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

# ---------- Wspólne maski/produkty ----------
FLAVORED_LIST = ["BEKON-SER", "BEKON-SER/SOL", "CHEDDAR/SOL", "KARMEL.", "KARMEL/BEKON.", "KARMEL/CHEDDAR.", "KARMEL/SOL.", "SER-CHEDDAR"]
FLAVORED_NORM = set(_norm_key(x) for x in FLAVORED_LIST)
BASE_POP_LIST = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l", "KubekPopcorn6,5l"]
BASE_POP_NORM = set(_norm_key(x) for x in BASE_POP_LIST)
SHARE_NUM_LIST = ["KubekPopcorn6,5l"]
SHARE_DEN_LIST = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l"]
SHARE_NUM_NORM = set(_norm_key(x) for x in SHARE_NUM_LIST)
SHARE_DEN_NORM = set(_norm_key(x) for x in SHARE_DEN_LIST)


# Zestawy (do KPI "% Zestawy")
SETS_LIST = ["XLOffer+", "Sredni+", "Duzy+", "Family1+1", "Duet+", "MAXI+", "Szkolny+"]
SETS_NORM = set(_norm_key(x) for x in SETS_LIST)

# =============== TABS (podstrony) ===============
tab_dane, tab_pivot, tab_indy, tab_best, tab_comp, tab_cafe, tab_vip = st.tabs(["🗂️ Dane", "📈 Tabela przestawna", "👤 Wyniki indywidualne", "🏆 Najlepsi", "🧮 Kreator Konkursów", "☕ Cafe Stats", "VIP stats"])

# ---------- Zakładka: Dane ----------
with tab_dane:
    st.subheader("🗂️ Ustawienia źródła danych")
    data_mode = st.radio("Źródło danych", ["Wgrywanie plików", "Folder lokalny"], horizontal=True, index=0)

    if data_mode == "Wgrywanie plików":
        uploaded = st.file_uploader("Wrzuć pliki (.xlsx/.csv/.txt)", type=["xlsx","csv","txt"], accept_multiple_files=True)
        if st.button("🔄 Wczytaj/odśwież dane", type="primary"):
            if not uploaded:
                st.warning("Dodaj przynajmniej jeden plik.")
            else:
                paths = save_uploads_to_tmp(uploaded)
                with st.spinner("Wczytywanie danych..."):
                    st.session_state["cached_df"] = _load_from_paths(paths)
        st.info("W chmurze (Streamlit Cloud) to tryb zalecany.")

    else:
        _cfg = load_config()
        data_dir_str = st.text_input("📁 Folder z danymi (lokalnie)", value=_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
        data_dir = Path(data_dir_str)
        if st.button("🔄 Wczytaj/odśwież dane", type="primary", key="reload_local"):
            save_config(data_dir=data_dir)
            with st.spinner("Wczytywanie danych..."):
                st.session_state["cached_df"] = load_all_data_from_dir(data_dir)

    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.info("Brak danych w pamięci. Wybierz tryb i wczytaj pliki.")
    else:
        st.success(f"Wczytano {len(df):,} wierszy.".replace(",", " "))
        st.dataframe(df.head(300), use_container_width=True)

def ensure_data_or_stop():
    df = st.session_state.get("cached_df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Przejdź do zakładki **Dane** i wczytaj pliki.")
        st.stop()
    return df


# ---------- POS helpers (CAF/VIP) ----------
def _exclude_caf_vip(df: pd.DataFrame) -> pd.DataFrame:
    """Usuwa wiersze z PosName zawierającym CAF lub VIP (dowolne kino)."""
    if "PosName" in df.columns:
        m = df["PosName"].astype(str).str.contains("CAF|VIP", case=False, regex=True, na=False)
        return df.loc[~m].copy()
    return df.copy()

def _keep_caf(df: pd.DataFrame) -> pd.DataFrame:
    """Zostawia tylko wiersze z PosName zawierającym CAF (dowolne kino)."""
    if "PosName" in df.columns:
        m = df["PosName"].astype(str).str.contains("CAF", case=False, regex=True, na=False)
        return df.loc[m].copy()
    return df.iloc[0:0].copy()

def _keep_vip(df: pd.DataFrame) -> pd.DataFrame:
    """Zostawia tylko wiersze z PosName zawierającym VIP (dowolne kino)."""
    if "PosName" in df.columns:
        m = df["PosName"].astype(str).str.contains("VIP", case=False, regex=True, na=False)
        return df.loc[m].copy()
    return df.iloc[0:0].copy()



# ---------- Zakładka: Tabela przestawna ----------
with tab_pivot:
    st.subheader("📈 Tabela wskaźników")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    # Zakres dat
    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="pivot_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
        dff = _exclude_caf_vip(dff)
    else:
        df_all = df.copy()

        bar_df  = _exclude_caf_vip(df_all)

        cafe_df = _keep_caf(df_all)

        vip_df  = _keep_vip(df_all)

        dff = bar_df

    users_sorted = sorted(dff.get("UserFullName", pd.Series(dtype=str)).dropna().unique())
    required = {"UserFullName", "ProductName", "Quantity"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    users_sorted = sorted(dff["UserFullName"].dropna().unique())

    # % Extra Sos
    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})
    extra_by_user = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum()
    base_by_user = dff.loc[mask_base].groupby("UserFullName")["Quantity"].sum()
    extra = extra_by_user.reindex(users_sorted, fill_value=0)
    base = base_by_user.reindex(users_sorted, fill_value=0)
    pct_extra = (extra / base.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % Popcorny smakowe
    mask_flavored_pop = dff["__pnorm"].isin(FLAVORED_NORM)
    mask_base_pop = dff["__pnorm"].isin(BASE_POP_NORM)
    
    mask_sets = dff["__pnorm"].isin(SETS_NORM)
    flavored_qty = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    base_pop_qty = dff.loc[mask_base_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    pct_popcorny = (flavored_qty / base_pop_qty.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % ShareCorn
    mask_share_num = dff["__pnorm"].isin(SHARE_NUM_NORM)
    mask_share_den = dff["__pnorm"].isin(SHARE_DEN_NORM)
    mask_sets = dff["__pnorm"].isin(SETS_NORM)

    # Liczba transakcji BAR (bez CAF/VIP) dla każdego zleceniobiorcy
    tx_bar_df = dff.copy()
    if "PosName" in tx_bar_df.columns:
        _m_ex_bar = tx_bar_df["PosName"].astype(str).str.contains("CAF|VIP", case=False, regex=True, na=False)
        tx_bar_df = tx_bar_df.loc[~_m_ex_bar].copy()
    if "TransactionId" in tx_bar_df.columns:
        tx_bar_count_by_user = tx_bar_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype("Int64")
    else:
        tx_bar_count_by_user = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Int64")
    share_num_qty = dff.loc[mask_share_num].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    share_den_qty = dff.loc[mask_share_den].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    pct_sharecorn = (share_num_qty / share_den_qty.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # POS wykluczenia
    tx_df = dff.copy()
    if "PosName" in tx_df.columns:
        m_ex = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~m_ex].copy()

    # Liczba transakcji i średnia wartość transakcji
    if "TransactionId" in tx_df.columns:
        tx_count = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype("Int64")
    else:
        tx_count = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Int64")

    if ("TransactionId" in tx_df.columns) and ("NetAmount" in tx_df.columns):
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True)
        s = grp.sum(min_count=1)
        f = grp.first()
        per_tx_total = f.where(nun <= 1, s)
        revenue = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted).astype("Float64")
        avg_value = (revenue / tx_count.astype("Float64").replace(0, pd.NA)).astype("Float64").round(2)
    else:
        avg_value = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Float64")
        
    # % Zestawy = (suma zestawów) / (liczba transakcji bar) * 100
    sets_by_user = dff.loc[mask_sets].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    pct_sets = (sets_by_user / tx_count.astype("Float64").replace(0, pd.NA) * 100).astype("Float64").round(1)

    # Finalna tabela
    result = pd.DataFrame(index=users_sorted)
    result["Liczba transakcji"] = tx_count
    result["Średnia wartość transakcji"] = avg_value
    result["% Extra Sos"] = pct_extra
    result["% Popcorny smakowe"] = pct_popcorny
    result["% ShareCorn"] = pct_sharecorn
    
    result["% Zestawy"] = pct_sets
    order = ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn", "% Zestawy"]
    
    result = result[order]
    result_sorted = result.sort_values(by="Średnia wartość transakcji", ascending=False, na_position="last")

    # Średnia kina
    try:
        pct_extra_c = float(dff.loc[mask_extra, "Quantity"].sum()) / float(dff.loc[mask_base, "Quantity"].sum()) * 100 if dff.loc[mask_base, "Quantity"].sum() else None
        pct_pop_c = float(dff.loc[mask_flavored_pop, "Quantity"].sum()) / float(dff.loc[mask_base_pop, "Quantity"].sum()) * 100 if dff.loc[mask_base_pop, "Quantity"].sum() else None
        den_sum = float(dff.loc[mask_share_den, "Quantity"].sum())
        num_sum = float(dff.loc[mask_share_num, "Quantity"].sum())
        pct_share_c = num_sum / den_sum * 100 if den_sum else None
        
        
        sets_sum = float(dff.loc[mask_sets, "Quantity"].sum())
        sets_den = int(tx_df["TransactionId"].nunique()) if "TransactionId" in tx_df.columns else 0
        pct_sets_c = (sets_sum / sets_den * 100) if sets_den else None
        if "TransactionId" in tx_df.columns and "NetAmount" in tx_df.columns:
            grp_all = tx_df.groupby("TransactionId")["NetAmount"]
            nun_all = grp_all.nunique(dropna=True)
            s_all = grp_all.sum(min_count=1)
            f_all = grp_all.first()
            per_tx_all = f_all.where(nun_all <= 1, s_all)
            avg_c = float(per_tx_all.sum(min_count=1)) / int(tx_df["TransactionId"].nunique()) if int(tx_df["TransactionId"].nunique()) else None
        else:
            avg_c = None

        summary_row = pd.DataFrame({
            "Liczba transakcji": [int(tx_df["TransactionId"].nunique()) if "TransactionId" in tx_df.columns else None],
            "Średnia wartość transakcji": [None if avg_c is None else round(avg_c, 2)],
            "% Extra Sos": [None if pct_extra_c is None else round(pct_extra_c, 1)],
            "% Popcorny smakowe": [None if pct_pop_c is None else round(pct_pop_c, 1)],
            "% ShareCorn": [None if pct_share_c is None else round(pct_share_c, 1)],
            "% Zestawy": [None if pct_sets_c is None else round(pct_sets_c, 1)],
        }, index=["Średnia kina"])
        final_df = pd.concat([summary_row, result_sorted], axis=0)
    except Exception:
        final_df = result_sorted

    # Styl + eksport
    def _fmt_pct(x):
        return "" if pd.isna(x) else f"{x:.1f} %"
    def _fmt_pln(x):
        return "" if pd.isna(x) else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
    def _bold_and_shade(row):
        return ['font-weight:700; background-color:#f3f4f6' for _ in row] if row.name == "Średnia kina" else ['' for _ in row]

    styled = final_df.style.format({
        "% Extra Sos": _fmt_pct, "% Popcorny smakowe": _fmt_pct, "% ShareCorn": _fmt_pct, "% Zestawy": _fmt_pct,
        "Średnia wartość transakcji": _fmt_pln
    }).apply(_bold_and_shade, axis=1)
    st.dataframe(styled, use_container_width=True)

    try:
        buffer = io.BytesIO()
        out_df = final_df.copy()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            out_df.to_excel(writer, index=True, sheet_name="Wskaźniki")
            wb = writer.book; ws = writer.sheets["Wskaźniki"]
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
        st.download_button("⬇️ Pobierz XLSX (tabela przestawna)", data=buffer.getvalue(),
                           file_name="Wskazniki.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as ex:
        st.warning(f"Nie udało się przygotować XLSX: {ex}")

    with st.expander("🎯 Szybkie zapytanie: zleceniobiorca + produkt", expanded=False):
        users = sorted(dff.get("UserFullName", pd.Series(dtype=str)).dropna().unique())
        prods = sorted(dff.get("ProductName", pd.Series(dtype=str)).dropna().unique())
        left, right = st.columns(2)
        with left:
            sel_user_simple = st.selectbox("Zleceniobiorca", options=users, index=0 if users else None, placeholder="Wybierz osobę...")
        with right:
            sel_prod_simple = st.selectbox("Produkt", options=prods, index=0 if prods else None, placeholder="Wybierz produkt...")
        if st.button("Pokaż", type="secondary"):
            subset2 = dff[(dff["UserFullName"] == sel_user_simple) & (dff["ProductName"] == sel_prod_simple)]
            total_qty2 = float(subset2["Quantity"].sum()) if not subset2.empty else 0.0
            st.metric(label="Suma sprzedanych sztuk (po filtrach daty)", value=f"{total_qty2:,.0f}".replace(",", " "))

# ---------- Zakładka: Wyniki indywidualne ----------
with tab_indy:
    st.subheader("👤 Wyniki indywidualne")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        cols = st.columns([1,1,2])
        with cols[0]:
            picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="indy_date")
        with cols[1]:
            users_all = sorted(df.get("UserFullName", pd.Series(dtype=str)).dropna().unique())
            sel_user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None, key="indy_user")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        df_all = df.loc[mask].copy()

        bar_df  = _exclude_caf_vip(df_all)

        cafe_df = _keep_caf(df_all)

        vip_df  = _keep_vip(df_all)

        dff = bar_df
    else:
        st.warning("Brak dat — używam wszystkich wierszy.")
        dff = df.copy()
        users_all = sorted(df.get("UserFullName", pd.Series(dtype=str)).dropna().unique())
        sel_user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None, key="indy_user_nodate")

    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})
    mask_flavored_pop = dff["__pnorm"].isin(FLAVORED_NORM)
    mask_base_pop = dff["__pnorm"].isin(BASE_POP_NORM)
    mask_share_num = dff["__pnorm"].isin(SHARE_NUM_NORM)
    mask_share_den = dff["__pnorm"].isin(SHARE_DEN_NORM)

    # Liczba transakcji BAR (bez CAF/VIP) dla każdego zleceniobiorcy
    tx_bar_df = dff.copy()
    if "PosName" in tx_bar_df.columns:
        _m_ex_bar = tx_bar_df["PosName"].astype(str).str.contains("CAF|VIP", case=False, regex=True, na=False)
        tx_bar_df = tx_bar_df.loc[~_m_ex_bar].copy()
    if "TransactionId" in tx_bar_df.columns:
        tx_bar_count_by_user = tx_bar_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype("Int64")
    else:
        tx_bar_count_by_user = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Int64")

    # KINO
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
        if ("TransactionId" in tx_df_all.columns) and ("NetAmount" in tx_df_all.columns):
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

        # % Zestawy — kino (bar): suma zestawów / liczba transakcji bar
        sets_sum = float(dff.loc[mask_sets, "Quantity"].sum()) if "Quantity" in dff.columns else 0.0
        sets_den = int(tx_df_all["TransactionId"].nunique()) if "TransactionId" in tx_df_all.columns else 0
        pct_sets_cinema = (sets_sum / sets_den * 100) if sets_den else None
    except Exception:
        pct_extra_cinema = pct_popcorny_cinema = pct_sharecorn_cinema = pct_sets_cinema = avg_tr_cinema = None

    # OSOBA
    dff_u = dff[dff["UserFullName"] == sel_user].copy()
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
        if ("TransactionId" in tx_df_u.columns) and ("NetAmount" in tx_df_u.columns):
            grp_u = tx_df_u.groupby("TransactionId")["NetAmount"]
            nun_u = grp_u.nunique(dropna=True)
            s_u = grp_u.sum(min_count=1)
            f_u = grp_u.first()
            per_tx_total_u = f_u.where(nun_u <= 1, s_u)
            tx_count_u = int(tx_df_u["TransactionId"].nunique())
            revenue_u = float(per_tx_total_u.sum(min_count=1))
            avg_tr_u = (revenue_u / tx_count_u) if tx_count_u else None
            # % Zestawy — użytkownik (bar): suma zestawów / liczba transakcji bar użytkownika
            try:
                sets_sum_u = float(dff_u.loc[mask_sets, "Quantity"].sum())
            except Exception:
                sets_sum_u = 0.0
            pct_sets_u = (sets_sum_u / tx_count_u * 100) if tx_count_u else None

        else:
            avg_tr_u = None; tx_count_u = None
    except Exception:
        pct_extra_u = pct_popcorny_u = pct_sharecorn_u = pct_sets_u = avg_tr_u = None; tx_count_u = None

    def _fmt_pct(x): return "" if x is None else f"{x:.1f} %"
    def _fmt_pln(x): return "" if x is None else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
    def _fmt_diff_pp(u, c):
        if u is None or c is None: return ""
        d = u - c; s = "+" if d>=0 else "−"; return f"{s}{abs(d):.1f} p.p."
    def _fmt_diff_pln(u, c):
        if u is None or c is None: return ""
        d = u - c; s = "+" if d>=0 else "−"; v = f"{abs(d):,.2f}".replace(",", " ").replace(".", ","); return f"{s}{v} zł"

    
    # --- Averages for bar/cafe/vip (user & cinema)
    def _avg_user_for(frame, user):
        if not {"TransactionId","NetAmount","UserFullName"}.issubset(frame.columns):
            return None
        f = frame[frame["UserFullName"] == user]
        if f.empty: return None
        g = f.groupby("TransactionId")["NetAmount"]
        per_tx = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        txc = int(f["TransactionId"].nunique())
        return None if txc==0 else round(float(per_tx.sum(min_count=1))/txc, 2)

    def _avg_cinema_for(frame):
        if not {"TransactionId","NetAmount"}.issubset(frame.columns):
            return None
        g = frame.groupby("TransactionId")["NetAmount"]
        per_tx = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        txc = int(frame["TransactionId"].nunique())
        return None if txc==0 else round(float(per_tx.sum(min_count=1))/txc, 2)

    avg_tr_bar_u = avg_tr_u
    avg_tr_bar_cinema = avg_tr_cinema
    avg_tr_cafe_u = _avg_user_for(cafe_df, sel_user)
    avg_tr_cafe_cinema = _avg_cinema_for(cafe_df)
    avg_tr_vip_u = _avg_user_for(vip_df, sel_user)
    avg_tr_vip_cinema = _avg_cinema_for(vip_df)

    

    # Dostępność danych dla wykresów
    has_bar  = (avg_tr_bar_u is not None)
    has_cafe = (avg_tr_cafe_u is not None)
    has_vip  = (avg_tr_vip_u is not None)
    has_any_money = has_bar or has_cafe or has_vip
    has_pct = any(v is not None for v in [pct_extra_u, pct_popcorny_u, pct_sharecorn_u])
    
    rows = [
        ["Średnia wartość transakcji bar",  avg_tr_bar_u,  avg_tr_bar_cinema,  _fmt_diff_pln(avg_tr_bar_u,  avg_tr_bar_cinema)],
        ["Średnia wartość transakcji cafe", avg_tr_cafe_u, avg_tr_cafe_cinema, _fmt_diff_pln(avg_tr_cafe_u, avg_tr_cafe_cinema)],
        ["Średnia wartość transakcji vip",  avg_tr_vip_u,  avg_tr_vip_cinema,  _fmt_diff_pln(avg_tr_vip_u,  avg_tr_vip_cinema)],
        ["% Extra Sos",           pct_extra_u,      pct_extra_cinema,      _fmt_diff_pp(pct_extra_u,      pct_extra_cinema)],
        ["% Popcorny smakowe",    pct_popcorny_u,   pct_popcorny_cinema,   _fmt_diff_pp(pct_popcorny_u,   pct_popcorny_cinema)],
        ["% ShareCorn",           pct_sharecorn_u,  pct_sharecorn_cinema,  _fmt_diff_pp(pct_sharecorn_u,  pct_sharecorn_cinema)],
            ["% Zestawy",           pct_sets_u,       pct_sets_cinema,       _fmt_diff_pp(pct_sets_u,       pct_sets_cinema)],
]
    df_view = pd.DataFrame(rows, columns=["Wskaźnik", sel_user, "Średnia kina", "Δ vs kino"])

    
    # Ukryj wiersze bez danych dla wybranej osoby (np. brak danych w segmencie cafe/VIP)
    df_view = df_view[df_view[sel_user].notna()].copy()
    
    st.markdown("#### Zestawienie")
    # Trzy metryki: bar / cafe / vip
    def _count_tx(frame, user):
        if "TransactionId" not in frame.columns: return None
        sub = frame[frame.get("UserFullName","") == user]
        return int(sub["TransactionId"].nunique()) if not sub.empty else 0
    tx_bar  = _count_tx(bar_df,  sel_user)
    tx_cafe = _count_tx(cafe_df, sel_user)
    tx_vip  = _count_tx(vip_df,  sel_user)
    c1,c2,c3 = st.columns(3)
    def _fmt_int(x):
        return "-" if (x is None) else f"{x:,}".replace(",", " ")
    c1.metric("Liczba transakcji — bar (bez CAF/VIP)", _fmt_int(tx_bar))
    c2.metric("Liczba transakcji — cafe (CAF)", _fmt_int(tx_cafe))
    c3.metric("Liczba transakcji — VIP", _fmt_int(tx_vip))

    # Formatowanie tabeli: PLN dla 3 pierwszych wierszy, % dla reszty
    disp = df_view.copy()
    money_mask = disp["Wskaźnik"].str.startswith("Średnia wartość transakcji")
    disp.loc[money_mask, [sel_user, "Średnia kina"]] = disp.loc[money_mask, [sel_user, "Średnia kina"]].applymap(_fmt_pln)
    disp.loc[~money_mask, [sel_user, "Średnia kina"]] = disp.loc[~money_mask, [sel_user, "Średnia kina"]].applymap(_fmt_pct)
    st.dataframe(disp, use_container_width=True, hide_index=True)


    # Wykresy
    st.markdown("### 📊 Wykresy porównawcze")
    _green, _red, _gray = "#16a34a", "#dc2626", "#6b7280"

    # Pieniądze
    val_user = avg_tr_u if avg_tr_u is not None else 0.0
    val_kino = avg_tr_cinema if avg_tr_cinema is not None else 0.0
    _color_user = _gray
    if avg_tr_u is not None and avg_tr_cinema is not None:
        _color_user = _green if avg_tr_u >= avg_tr_cinema else _red
    _diff_money = ""
    if avg_tr_u is not None and avg_tr_cinema is not None:
        d = avg_tr_u - avg_tr_cinema
        s = "+" if d >= 0 else "−"
        _diff_money = s + f"{abs(d):,.2f}".replace(",", " ").replace(".", ",") + " zł"
    df_chart_money = pd.DataFrame([
        {"Kto": sel_user, "Wartość": val_user, "kolor": _color_user, "label": _diff_money, "label_color": _color_user},
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
    ref_df = pd.DataFrame({"ref":[val_kino]})
    rule_money = alt.Chart(ref_df).mark_rule(strokeDash=[6,4], color=_gray, opacity=0.8).encode(y="ref:Q")
    
    # Trzy wykresy obok siebie: bar / cafe / VIP
    def _money_df(orig_user, orig_kino):
        # build DF + colors + diff label
        _u = 0.0 if orig_user is None else float(orig_user)
        _k = 0.0 if orig_kino is None else float(orig_kino)
        _col = _gray
        if (orig_user is not None) and (orig_kino is not None):
            _col = _green if orig_user >= orig_kino else _red
        _lab = ""
        if (orig_user is not None) and (orig_kino is not None):
            d = orig_user - orig_kino
            s = "+" if d >= 0 else "−"
            _lab = s + f"{abs(d):,.2f}".replace(",", " ").replace(".", ",") + " zł"
        return pd.DataFrame([
            {"Kto": sel_user, "Wartość": _u, "kolor": _col, "label": _lab, "label_color": _col},
            {"Kto": "Średnia kina", "Wartość": _k, "kolor": _gray, "label": "", "label_color": _gray},
        ])

    def _money_chart(df_local):
        base = alt.Chart(df_local)
        bars = base.mark_bar(size=28).encode(
            x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
            y=alt.Y("Wartość:Q", title="zł"),
            color=alt.Color("kolor:N", legend=None, scale=None),
            tooltip=[alt.Tooltip("Kto:N"), alt.Tooltip("Wartość:Q", format=",.2f")]
        )
        labels = base.mark_text(dy=-6, size=16).encode(
            x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
            y=alt.Y("Wartość:Q"),
            text=alt.Text("label:N"),
            color=alt.Color("label_color:N", legend=None, scale=None)
        )
        ref = alt.Chart(pd.DataFrame({"ref":[float(df_local.loc[df_local['Kto']=='Średnia kina','Wartość'].iloc[0])] })).mark_rule(
            strokeDash=[6,4], color=_gray, opacity=0.8
        ).encode(y="ref:Q")
        return (bars + labels + ref).properties(width=360, height=320)

    
    # Renderuj tylko wykresy z danymi użytkownika
    charts = []
    if has_bar:
        charts.append(("Średnia wartość transakcji — bar", avg_tr_u, avg_tr_cinema))
    if has_cafe:
        charts.append(("Średnia wartość transakcji — cafe", avg_tr_cafe_u, avg_tr_cafe_cinema))
    if has_vip:
        charts.append(("Średnia wartość transakcji — VIP", avg_tr_vip_u, avg_tr_vip_cinema))

    if charts:
        cols = st.columns(len(charts))
        for col, (title, uval, cval) in zip(cols, charts):
            with col:
                st.markdown(f"#### {title}")
                df_local = _money_df(uval, cval)
                st.altair_chart(_money_chart(df_local), use_container_width=False)
    else:
        st.info("Brak danych do wykresów średniej wartości transakcji dla wybranej osoby.")

# Wskaźniki % (facet)
    if has_pct:
        st.caption("Wskaźniki procentowe")
        metrics = ["% Extra Sos", "% Popcorny smakowe", "% ShareCorn"]
        user_vals = [pct_extra_u, pct_popcorny_u, pct_sharecorn_u]
        cinema_vals = [pct_extra_cinema, pct_popcorny_cinema, pct_sharecorn_cinema]
        rows = []
        for m, u, c in zip(metrics, user_vals, cinema_vals):
            if u is None:
                continue  # pomiń wykres dla wskaźnika bez danych użytkownika
            uval = u
            cval = 0.0 if c is None else c
            _green, _red, _gray = "#16a34a", "#dc2626", "#6b7280"
            ucol = _gray if (c is None) else (_green if uval >= cval else _red)
            label = ""
            if c is not None:
                d = uval - cval
                s = "+" if d >= 0 else "−"
                label = s + f"{abs(d):.1f}".replace(".", ",") + " p.p."
            rows.append({"Wskaźnik": m, "Kto": sel_user, "Wartość": float(uval), "kolor": ucol, "diff_label": label, "label_color": ucol})
            rows.append({"Wskaźnik": m, "Kto": "Średnia kina", "Wartość": float(cval), "kolor": _gray, "diff_label": "", "label_color": _gray})

        if rows:
            df_chart_pct = pd.DataFrame(rows)
            base_pct = alt.Chart(df_chart_pct)
            bars_pct = base_pct.mark_bar(size=28).encode(
                x=alt.X("Kto:N", title="", sort=[sel_user, "Średnia kina"]),
                y=alt.Y("Wartość:Q", title="%"),
                color=alt.Color("kolor:N", legend=None, scale=None),
                tooltip=[alt.Tooltip("Wskaźnik:N"), alt.Tooltip("Kto:N"), alt.Tooltip("Wartość:Q", format=".1f")]
            )
            labels_pct = base_pct.mark_text(dy=-6, size=18).encode(
                x=alt.X("Kto:N", title="", sort=[sel_user, "Średnia kina"]),
                y=alt.Y("Wartość:Q"),
                text=alt.Text("diff_label:N"),
                color=alt.Color("label_color:N", legend=None, scale=None)
            )
            rule_pct = base_pct.transform_filter(alt.datum.Kto == "Średnia kina").mark_rule(strokeDash=[6,4], color="#6b7280", opacity=0.8).encode(y="Wartość:Q")
            chart_pct = (bars_pct + labels_pct + rule_pct).properties(width=360, height=480).facet(column=alt.Column("Wskaźnik:N", header=alt.Header(title=None)))
            st.altair_chart(chart_pct, use_container_width=True)
        else:
            st.info("Brak danych do wykresów wskaźników procentowych dla wybranej osoby.")
    else:
        st.info("Brak danych do wykresów wskaźników procentowych dla wybranej osoby.")


# ---------- Zakładka: Najlepsi ----------
with tab_best:
    st.subheader("🏆 Najlepsi — ranking wg wskaźników")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="best_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        df_all = df.loc[mask].copy()

        bar_df  = _exclude_caf_vip(df_all)

        cafe_df = _keep_caf(df_all)

        vip_df  = _keep_vip(df_all)

        dff = bar_df
    else:
        dff = df.copy()

    required = {"UserFullName", "ProductName", "Quantity"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    users_sorted = sorted(dff["UserFullName"].dropna().unique())
    mask_extra = dff["__pnorm"] == "extranachossauce"
    mask_base = dff["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})
    mask_flavored_pop = dff["__pnorm"].isin(FLAVORED_NORM)
    mask_base_pop = dff["__pnorm"].isin(BASE_POP_NORM)
    mask_share_num = dff["__pnorm"].isin(SHARE_NUM_NORM)
    mask_share_den = dff["__pnorm"].isin(SHARE_DEN_NORM)

    # Liczba transakcji BAR (bez CAF/VIP) dla każdego zleceniobiorcy
    tx_bar_df = dff.copy()
    if "PosName" in tx_bar_df.columns:
        _m_ex_bar = tx_bar_df["PosName"].astype(str).str.contains("CAF|VIP", case=False, regex=True, na=False)
        tx_bar_df = tx_bar_df.loc[~_m_ex_bar].copy()
    if "TransactionId" in tx_bar_df.columns:
        tx_bar_count_by_user = tx_bar_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype("Int64")
    else:
        tx_bar_count_by_user = pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Int64")

    def style_over_avg(df_in: pd.DataFrame, avg_val: float, is_pct: bool) -> pd.io.formats.style.Styler:
        def _fmt_pct(x): return "" if pd.isna(x) else f"{x:.1f} %"
        def _fmt_pln(x):
            if pd.isna(x): return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ","); return s + " zł"
        def _color(v):
            try:
                return "background-color: #dcfce7; font-weight: 600" if (not pd.isna(v) and not pd.isna(avg_val) and v >= avg_val) else ""
            except Exception:
                return ""
        sty = df_in.style.applymap(_color, subset=["Wartość"])
        return sty.format({"Wartość": _fmt_pct if is_pct else _fmt_pln})

    # % Extra Sos
    extra = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    base = dff.loc[mask_base].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    tbl_extra = (extra / base.replace(0, pd.NA) * 100).astype("Float64")
    avg_extra = (float(dff.loc[mask_extra, "Quantity"].sum()) / float(dff.loc[mask_base, "Quantity"].sum()) * 100) if dff.loc[mask_base, "Quantity"].sum() else None
    df_extra = pd.DataFrame({"Wartość": tbl_extra, "Liczba transakcji bar": tx_bar_count_by_user}).sort_values("Wartość", ascending=False, na_position="last")
    df_extra = df_extra.rename_axis("Zleceniobiorca").reset_index()[["Zleceniobiorca","Liczba transakcji bar","Wartość"]]
    st.markdown("#### % Extra Sos")
    if avg_extra is not None: st.caption(f"Średnia kina: **{avg_extra:.1f} %**")
    st.dataframe(style_over_avg(df_extra, avg_extra, is_pct=True), use_container_width=True, hide_index=True)

    # % Popcorny smakowe
    flavored = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    base_pop = dff.loc[mask_base_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    tbl_pop = (flavored / base_pop.replace(0, pd.NA) * 100).astype("Float64")
    avg_pop = (float(dff.loc[mask_flavored_pop, "Quantity"].sum()) / float(dff.loc[mask_base_pop, "Quantity"].sum()) * 100) if dff.loc[mask_base_pop, "Quantity"].sum() else None
    df_pop = pd.DataFrame({"Wartość": tbl_pop, "Liczba transakcji bar": tx_bar_count_by_user}).sort_values("Wartość", ascending=False, na_position="last")
    df_pop = df_pop.rename_axis("Zleceniobiorca").reset_index()[["Zleceniobiorca","Liczba transakcji bar","Wartość"]]
    st.markdown("#### % Popcorny smakowe")
    if avg_pop is not None: st.caption(f"Średnia kina: **{avg_pop:.1f} %**")
    st.dataframe(style_over_avg(df_pop, avg_pop, is_pct=True), use_container_width=True, hide_index=True)

    # % ShareCorn
    share_num_qty = dff.loc[mask_share_num].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    share_den_qty = dff.loc[mask_share_den].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0)
    tbl_share = (share_num_qty / share_den_qty.replace(0, pd.NA) * 100).astype("Float64")
    den_sum = float(dff.loc[mask_share_den, "Quantity"].sum()); num_sum = float(dff.loc[mask_share_num, "Quantity"].sum())
    avg_share = (num_sum / den_sum * 100) if den_sum else None
    df_share = pd.DataFrame({"Wartość": tbl_share, "Liczba transakcji bar": tx_bar_count_by_user}).sort_values("Wartość", ascending=False, na_position="last")
    df_share = df_share.rename_axis("Zleceniobiorca").reset_index()[["Zleceniobiorca","Liczba transakcji bar","Wartość"]]
    st.markdown("#### % ShareCorn")
    if avg_share is not None: st.caption(f"Średnia kina: **{avg_share:.1f} %**")
    st.dataframe(style_over_avg(df_share, avg_share, is_pct=True), use_container_width=True, hide_index=True)

    # Średnia wartość transakcji
    tx_df = dff.copy()
    if "PosName" in tx_df.columns:
        mask_excl = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~mask_excl].copy()
    st.markdown("#### Średnia wartość transakcji")
    if "TransactionId" in tx_df.columns and "NetAmount" in tx_df.columns:
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx_total = f.where(nun <= 1, s)
        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique()
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64")

        grp_all = tx_df.groupby("TransactionId")["NetAmount"]
        nun_all = grp_all.nunique(dropna=True); s_all = grp_all.sum(min_count=1); f_all = grp_all.first()
        per_tx_total_all = f_all.where(nun_all <= 1, s_all)
        global_tx_count = tx_df["TransactionId"].nunique()
        global_revenue = float(per_tx_total_all.sum(min_count=1))
        avg_global = (global_revenue / global_tx_count) if global_tx_count else None

        df_avg = pd.DataFrame({"Wartość": avg_by_user.reindex(users_sorted), "Liczba transakcji bar": tx_count_by_user.reindex(users_sorted, fill_value=0).astype("Int64")}).sort_values("Wartość", ascending=False, na_position="last")
        df_avg = df_avg.rename_axis("Zleceniobiorca").reset_index()[["Zleceniobiorca","Liczba transakcji bar","Wartość"]]
        def _fmt_pln(x):
            if pd.isna(x): return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ","); return s + " zł"
        def _color(v):
            try: return "background-color:#dcfce7; font-weight:600" if (not pd.isna(v) and avg_global is not None and v >= avg_global) else ""
            except Exception: return ""
        sty = df_avg.style.applymap(_color, subset=["Wartość"]).format({"Wartość": _fmt_pln})
        if avg_global is not None: st.caption(f"Średnia kina: **{avg_global:,.2f} zł**".replace(",", " ").replace(".", ","))
        st.dataframe(sty, use_container_width=True, hide_index=True)
    else:
        st.info("Brak kolumn TransactionId lub NetAmount — nie można policzyć średniej wartości transakcji.")

# ---------- Zakładka: Kreator Konkursów ----------
with tab_comp:
    st.subheader("🧮 Kreator Konkursów")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="contest_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask].copy()
    else:
        dff = df.copy()

    required = {"UserFullName", "ProductName", "Quantity"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    products_all = sorted(dff.get("ProductName", pd.Series(dtype=str)).dropna().unique())
    users_sorted = sorted(dff.get("UserFullName", pd.Series(dtype=str)).dropna().unique())

    # Grupa: Popcorny Smakowe
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    mask_flavored_pop = dff["__pnorm"].isin(FLAVORED_NORM)
    GROUP_FLAVORED = "Popcorny Smakowe"
    products_all_ext = [GROUP_FLAVORED] + products_all

    # UI: dynamiczne produkty + punkty
    st.markdown("### Produkty i punktacja")
    if "contest_products" not in st.session_state:
        st.session_state["contest_products"] = [None]
        st.session_state["contest_points"] = [1.0]

    cols_btn = st.columns([1,1,6])
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
                f"Produkt #{i+1}", options=products_all_ext,
                index=(products_all_ext.index(st.session_state['contest_products'][i]) if st.session_state['contest_products'][i] in products_all_ext else None),
                placeholder="Wybierz produkt...", key=f"contest_prod_{i}"
            )
        with c2:
            st.session_state["contest_points"][i] = st.number_input(
                f"Punkty #{i+1}", min_value=-10000.0, max_value=10000.0, value=float(st.session_state["contest_points"][i]), step=0.5, key=f"contest_pts_{i}"
            )

    st.divider()
    st.markdown("### Mianownik współczynnika")
    den_mode = st.selectbox("Wybierz mianownik", ["Liczba transakcji", "Wybrany produkt", "Stała 1"], key="contest_den_mode")
    den_prod = None
    if den_mode == "Wybrany produkt":
        den_prod = st.selectbox("Produkt dla mianownika", options=products_all_ext, placeholder="Wybierz produkt...", key="contest_den_prod")
    if den_mode == "Liczba transakcji":
        st.caption("Liczba unikatowych TransactionId po wykluczeniu POS: Bonarka CAF1/VIP1.")
    # Minimalna liczba transakcji (próg kwalifikacji)
    min_tx = st.number_input("Minimalna liczba transakcji", min_value=0, value=0, step=1)


    if st.button("🧮 Oblicz ranking", type="primary"):
        pairs = []
        for prod, pts in zip(st.session_state["contest_products"], st.session_state["contest_points"]):
            if prod is not None and pts is not None and float(pts) != 0.0:
                pairs.append((prod, float(pts)))
        if not pairs:
            st.warning("Dodaj co najmniej jedną pozycję z niezerową punktacją.")
            st.stop()

        num = pd.Series(0.0, index=users_sorted, dtype="float")
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
                st.error("Brak kolumny TransactionId — nie można użyć mianownika 'Liczba transakcji'."); st.stop()
            den = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0).astype(float)
        elif den_mode == "Wybrany produkt":
            if not den_prod:
                st.error("Wybierz produkt dla mianownika."); st.stop()
            if den_prod == GROUP_FLAVORED:
                den = dff.loc[mask_flavored_pop].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0).astype(float)
            else:
                den = dff.loc[dff["ProductName"] == den_prod].groupby("UserFullName")["Quantity"].sum().reindex(users_sorted, fill_value=0).astype(float)
        else:
            den = pd.Series(1.0, index=users_sorted, dtype="float")

        res = (num / den.replace(0, pd.NA)).astype("Float64")
        wynik_pct = (res * 100).astype("Float64")

        tx_df_all = dff.copy()
        if "PosName" in tx_df_all.columns:
            _m_ex_all = tx_df_all["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
            tx_df_all = tx_df_all.loc[~_m_ex_all].copy()
        tx_count_all = (tx_df_all.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted, fill_value=0)
                        if "TransactionId" in tx_df_all.columns else pd.Series([pd.NA]*len(users_sorted), index=users_sorted, dtype="Float64"))

                # Zbuduj pełny ranking (bez minimum), następnie podziel wg progu min_tx
        out_full = pd.DataFrame({
            "Wynik": res,
            "Wynik (%)": wynik_pct,
            "Licznik (pkt)": num,
            "Mianownik": den,
            "Transakcje": tx_count_all
        }).sort_values("Wynik", ascending=False, na_position="last")

        # Rozdział: kwalifikowani (>= min_tx) i poniżej progu (< min_tx)
        _tx_num = pd.to_numeric(out_full["Transakcje"], errors="coerce").fillna(0)
        mask_ok = _tx_num >= float(min_tx if 'min_tx' in locals() else 0)
        out_ok = out_full.loc[mask_ok].copy()
        out_low = out_full.loc[~mask_ok].copy()

        # --- Tabela zwycięzców (kwalifikowani) ---
        out_ok = out_ok.sort_values("Wynik", ascending=False, na_position="last")
        out_ok.insert(0, "Miejsce", range(1, len(out_ok)+1))
        disp_ok = out_ok.reset_index(names="Zleceniobiorca")[["Miejsce", "Zleceniobiorca", "Wynik (%)", "Licznik (pkt)", "Mianownik", "Transakcje"]]

        def _place_with_medal(m):
            try: mi = int(m)
            except Exception: return m
            return f"{mi} 🥇" if mi == 1 else (f"{mi} 🥈" if mi == 2 else (f"{mi} 🥉" if mi == 3 else f"{mi}"))
        disp_ok["Miejsce"] = disp_ok["Miejsce"].map(_place_with_medal)

        def _row_style_top3(row):
            try:
                import re as _re
                mm = int(_re.match(r"\d+", str(row["Miejsce"]))[0])
            except Exception:
                return [""] * len(row)
            if mm == 1: style = "background-color:#fff4b8; font-weight:700"
            elif mm == 2: style = "background-color:#e5e7eb; font-weight:600"
            elif mm == 3: style = "background-color:#fde7c2; font-weight:600"
            else: style = ""
            return [style]*len(row)

        n_ok = int(len(out_ok))
        st.markdown(f"#### ✅ Ranking — osoby spełniające minimum transakcji ({n_ok})")
        sty_ok = disp_ok.style.apply(_row_style_top3, axis=1)
        try: sty_ok = sty_ok.hide(axis="index")
        except Exception: pass
        st.dataframe(sty_ok, use_container_width=True, hide_index=True)

        # Eksport XLSX — zwycięzcy (tuż pod główną tabelą)
        try:
            buf_ok = io.BytesIO()
            export_ok = out_ok.reset_index().rename(columns={"index":"Zleceniobiorca"})[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik","Transakcje"]]
            with pd.ExcelWriter(buf_ok, engine="xlsxwriter") as writer:
                export_ok.to_excel(writer, index=False, sheet_name="Ranking")
                wb = writer.book; ws = writer.sheets["Ranking"]
                fmt_pct = wb.add_format({"num_format": "0.0 %"})
                fmt_num = wb.add_format({"num_format": "0.00"})
                fmt_int = wb.add_format({"num_format": "0"})
                ws.set_column("A:A", 9, fmt_int)
                ws.set_column("B:B", 28)
                ws.set_column("C:C", 12, fmt_pct)
                ws.set_column("D:D", 16, fmt_num)
                ws.set_column("E:E", 14, fmt_num)
                ws.set_column("F:F", 13, fmt_int)
                ws.set_row(0, None, wb.add_format({"bold": True}))
            st.download_button("⬇️ Pobierz ranking xlsx", data=buf_ok.getvalue(),
                               file_name="Konkurs_ranking.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować eksportu XLSX (ranking): {ex}")

        # --- Tabela poniżej progu (bez kolorów i medali) ---
        if not out_low.empty:
            out_low = out_low.sort_values("Wynik", ascending=False, na_position="last")
            out_low.insert(0, "Miejsce", range(1, len(out_low)+1))
            disp_low = out_low.reset_index(names="Zleceniobiorca")[["Miejsce", "Zleceniobiorca", "Wynik (%)", "Licznik (pkt)", "Mianownik", "Transakcje"]]
            n_low = int(len(out_low))
            st.markdown(f"#### ℹ️ Pozostali — poniżej minimalnej liczby transakcji ({n_low})")
            st.dataframe(disp_low, use_container_width=True, hide_index=True)

            # Eksport 'poniżej progu'
            try:
                buf_low = io.BytesIO()
                export_low = out_low.reset_index().rename(columns={"index":"Zleceniobiorca"})[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik","Transakcje"]]
                with pd.ExcelWriter(buf_low, engine="xlsxwriter") as writer:
                    export_low.to_excel(writer, index=False, sheet_name="PonizejProgu")
                    wb = writer.book; ws = writer.sheets["PonizejProgu"]
                    fmt_pct = wb.add_format({"num_format": "0.0 %"})
                    fmt_num = wb.add_format({"num_format": "0.00"})
                    fmt_int = wb.add_format({"num_format": "0"})
                    ws.set_column("A:A", 9, fmt_int)
                    ws.set_column("B:B", 28)
                    ws.set_column("C:C", 12, fmt_pct)
                    ws.set_column("D:D", 16, fmt_num)
                    ws.set_column("E:E", 14, fmt_num)
                    ws.set_column("F:F", 13, fmt_int)
                    ws.set_row(0, None, wb.add_format({"bold": True}))
                st.download_button("⬇️ Pobierz poniżej progu", data=buf_low.getvalue(),
                                   file_name="Konkurs_ponizej_progu.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as ex:
                st.warning(f"Nie udało się przygotować eksportu XLSX (poniżej progu): {ex}")

        # --- Eksport łączny (obie tabele do jednego pliku) ---
        try:
            buf_all = io.BytesIO()
            with pd.ExcelWriter(buf_all, engine="xlsxwriter") as writer:
                # Ranking
                export_ok2 = out_ok.reset_index().rename(columns={"index":"Zleceniobiorca"})[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik","Transakcje"]]
                export_ok2.to_excel(writer, index=False, sheet_name="Ranking")
                wb = writer.book
                # formaty wspólne
                fmt_pct = wb.add_format({"num_format": "0.0 %"})
                fmt_num = wb.add_format({"num_format": "0.00"})
                fmt_int = wb.add_format({"num_format": "0"})
                ws1 = writer.sheets["Ranking"]
                ws1.set_column("A:A", 9, fmt_int)
                ws1.set_column("B:B", 28)
                ws1.set_column("C:C", 12, fmt_pct)
                ws1.set_column("D:D", 16, fmt_num)
                ws1.set_column("E:E", 14, fmt_num)
                ws1.set_column("F:F", 13, fmt_int)
                ws1.set_row(0, None, wb.add_format({"bold": True}))
                # Poniżej progu
                if 'out_low' in locals() and not out_low.empty:
                    export_low2 = out_low.reset_index().rename(columns={"index":"Zleceniobiorca"})[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik","Transakcje"]]
                    export_low2.to_excel(writer, index=False, sheet_name="PonizejProgu")
                    ws2 = writer.sheets["PonizejProgu"]
                    ws2.set_column("A:A", 9, fmt_int)
                    ws2.set_column("B:B", 28)
                    ws2.set_column("C:C", 12, fmt_pct)
                    ws2.set_column("D:D", 16, fmt_num)
                    ws2.set_column("E:E", 14, fmt_num)
                    ws2.set_column("F:F", 13, fmt_int)
                    ws2.set_row(0, None, wb.add_format({"bold": True}))
            st.download_button("⬇️ Pobierz całość (XLSX)", data=buf_all.getvalue(),
                               file_name="Konkurs_calosc.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować eksportu XLSX (całość): {ex}")


# ---------- Zakładka: Cafe Stats ----------
with tab_cafe:
    st.subheader("☕ Cafe Stats — wszystkie kina (CAF)")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    # Zakres dat
    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="cafe_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
    else:
        dff = df.copy()

    required = {"UserFullName", "TransactionId", "NetAmount", "PosName"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn do obliczeń CAF: UserFullName, TransactionId, NetAmount, PosName.")
        st.stop()

    # Filtr: wszystkie rekordy z PosName zawierającym 'CAF'
    tx_df = _keep_caf(dff)

    if tx_df.empty:
        st.info("Brak danych dla POS zawierających 'CAF' w wybranym zakresie dat.")
    else:
        users_sorted = sorted(tx_df["UserFullName"].dropna().unique())
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx_total = f.where(nun <= 1, s)

        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted)
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64").round(2)

        grp_all = tx_df.groupby("TransactionId")["NetAmount"]
        nun_all = grp_all.nunique(dropna=True); s_all = grp_all.sum(min_count=1); f_all = grp_all.first()
        per_tx_all = f_all.where(nun_all <= 1, s_all)
        global_tx_count = int(tx_df["TransactionId"].nunique())
        global_revenue = float(per_tx_all.sum(min_count=1))
        avg_global = (global_revenue / global_tx_count) if global_tx_count else None

        result = pd.DataFrame(index=users_sorted)
        result["Liczba transakcji (CAF)"] = tx_count_by_user.astype("Int64")
        result["Średnia wartość transakcji (CAF)"] = avg_by_user
        result["Różnica"] = (avg_by_user - avg_global).astype("Float64").round(2) if avg_global is not None else pd.NA

        result_sorted = result.sort_values(by="Średnia wartość transakcji (CAF)", ascending=False, na_position="last")

        summary_row = pd.DataFrame({
            "Liczba transakcji (CAF)": [global_tx_count if global_tx_count else None],
            "Średnia wartość transakcji (CAF)": [None if avg_global is None else round(avg_global, 2)],
            "Różnica": [None],
        }, index=["Średnia (CAF — wszystkie kina)"])

        final_df = pd.concat([summary_row, result_sorted], axis=0)[["Liczba transakcji (CAF)","Średnia wartość transakcji (CAF)","Różnica"]]

        def _fmt_pln(x):
            return "" if pd.isna(x) else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
        def _row_style(row):
            if row.name == "Średnia (CAF — wszystkie kina)":
                return ['font-weight:700; background-color:#f3f4f6' for _ in row]
            try:
                diff = row.get("Różnica")
                if pd.isna(diff): return ['' for _ in row]
                if diff > 0:  return ['background-color:#dcfce7; font-weight:600' for _ in row]
                if diff < 0:  return ['background-color:#fee2e2; font-weight:600' for _ in row]
                return ['' for _ in row]
            except Exception:
                return ['' for _ in row]

        styled = final_df.style.format({"Średnia wartość transakcji (CAF)": _fmt_pln, "Różnica": _fmt_pln}).apply(_row_style, axis=1)
        st.dataframe(styled, use_container_width=True)

        # Eksport do XLSX
        try:
            buffer = io.BytesIO()
            out_df = final_df.copy()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                out_df.to_excel(writer, index=True, sheet_name="CafeStats")
                wb = writer.book; ws = writer.sheets["CafeStats"]
                fmt_bold = wb.add_format({"bold": True})
                fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
                fmt_int = wb.add_format({"num_format": "0"})
                ws.set_row(0, None, fmt_bold)
                ws.set_column("A:A", 32)
                ws.set_column("B:B", 20, fmt_int)
                ws.set_column("C:C", 28, fmt_pln)
                ws.set_column("D:D", 20, fmt_pln)
            st.download_button("⬇️ Pobierz XLSX (Cafe Stats)", data=buffer.getvalue(),
                               file_name="CafeStats.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować XLSX: {ex}")


# ---------- Zakładka: VIP stats ----------
with tab_vip:
    st.subheader("VIP stats — wszystkie kina (VIP)")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    # Zakres dat
    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="vip_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
    else:
        dff = df.copy()

    required = {"UserFullName", "TransactionId", "NetAmount", "PosName"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn do obliczeń VIP: UserFullName, TransactionId, NetAmount, PosName.")
        st.stop()

    # Filtr: wszystkie rekordy z PosName zawierającym 'VIP'
    tx_df = _keep_vip(dff)

    if tx_df.empty:
        st.info("Brak danych dla POS zawierających 'VIP' w wybranym zakresie dat.")
    else:
        users_sorted = sorted(tx_df["UserFullName"].dropna().unique())
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx_total = f.where(nun <= 1, s)

        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted)
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64").round(2)

        grp_all = tx_df.groupby("TransactionId")["NetAmount"]
        nun_all = grp_all.nunique(dropna=True); s_all = grp_all.sum(min_count=1); f_all = grp_all.first()
        per_tx_all = f_all.where(nun_all <= 1, s_all)
        global_tx_count = int(tx_df["TransactionId"].nunique())
        global_revenue = float(per_tx_all.sum(min_count=1))
        avg_global = (global_revenue / global_tx_count) if global_tx_count else None

        result = pd.DataFrame(index=users_sorted)
        result["Liczba transakcji (VIP)"] = tx_count_by_user.astype("Int64")
        result["Średnia wartość transakcji (VIP)"] = avg_by_user
        result["Różnica"] = (avg_by_user - avg_global).astype("Float64").round(2) if avg_global is not None else pd.NA

        result_sorted = result.sort_values(by="Średnia wartość transakcji (VIP)", ascending=False, na_position="last")

        summary_row = pd.DataFrame({
            "Liczba transakcji (VIP)": [global_tx_count if global_tx_count else None],
            "Średnia wartość transakcji (VIP)": [None if avg_global is None else round(avg_global, 2)],
            "Różnica": [None],
        }, index=["Średnia (VIP — wszystkie kina)"])

        final_df = pd.concat([summary_row, result_sorted], axis=0)[["Liczba transakcji (VIP)","Średnia wartość transakcji (VIP)","Różnica"]]

        def _fmt_pln(x):
            return "" if pd.isna(x) else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
        def _row_style(row):
            if row.name == "Średnia (VIP — wszystkie kina)":
                return ['font-weight:700; background-color:#f3f4f6' for _ in row]
            try:
                diff = row.get("Różnica")
                if pd.isna(diff): return ['' for _ in row]
                if diff > 0:  return ['background-color:#dcfce7; font-weight:600' for _ in row]
                if diff < 0:  return ['background-color:#fee2e2; font-weight:600' for _ in row]
                return ['' for _ in row]
            except Exception:
                return ['' for _ in row]

        styled = final_df.style.format({"Średnia wartość transakcji (VIP)": _fmt_pln, "Różnica": _fmt_pln}).apply(_row_style, axis=1)
        st.dataframe(styled, use_container_width=True)

        # Eksport do XLSX
        try:
            buffer = io.BytesIO()
            out_df = final_df.copy()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                out_df.to_excel(writer, index=True, sheet_name="VIPStats")
                wb = writer.book; ws = writer.sheets["VIPStats"]
                fmt_bold = wb.add_format({"bold": True})
                fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
                fmt_int = wb.add_format({"num_format": "0"})
                ws.set_row(0, None, fmt_bold)
                ws.set_column("A:A", 32)
                ws.set_column("B:B", 20, fmt_int)
                ws.set_column("C:C", 28, fmt_pln)
                ws.set_column("D:D", 20, fmt_pln)
            st.download_button("⬇️ Pobierz XLSX (VIP stats)", data=buffer.getvalue(),
                               file_name="VIPStats.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować XLSX: {ex}")
