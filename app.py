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
    if "__date" in df.columns:
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
# Mianownik % ShareCorn = wszystkie opakowania popcorn (5 kubków, łącznie z 6,5l),
# spójnie z mianownikiem "% Popcorny smakowe" (BASE_POP_LIST).
SHARE_DEN_LIST = ["KubekPopcorn1,5l", "KubekPopcorn2,3l", "KubekPopcorn4,2l", "KubekPopcorn5,2l", "KubekPopcorn6,5l"]
SHARE_NUM_NORM = set(_norm_key(x) for x in SHARE_NUM_LIST)
SHARE_DEN_NORM = set(_norm_key(x) for x in SHARE_DEN_LIST)

# --- Chipsy (Lay's / Cheetos) ---
# Uwaga: dopasowanie DOKŁADNE. W raportach jest ~31 pozycji zawierających "Lay's"/"Cheetos"
# (paczki 130g, tacki, popcorny smakowe, Sharecorny) — dopasowanie po fragmencie nazwy
# fałszowałoby wskaźnik. Liczymy wyłącznie pozycje rozszerzenia: "Lay's" i "Cheetos".
CHIPS_LIST = ["Lay's", "Cheetos"]
CHIPS_NORM = set(_norm_key(x) for x in CHIPS_LIST)
# Chipsy sprzedawane są tylko jako rozszerzenie do kubków 5,2l i 6,5l — tylko one w mianowniku.
CHIPS_DEN_LIST = ["KubekPopcorn5,2l", "KubekPopcorn6,5l"]
CHIPS_DEN_NORM = set(_norm_key(x) for x in CHIPS_DEN_LIST)


# Zestawy (do KPI "% Zestawy")
SETS_LIST = ["XLOffer+", "Sredni+", "Duzy+", "Family1+1", "Duet+", "MAXI+", "Szkolny+", "DuetShare+"]
SETS_NORM = set(_norm_key(x) for x in SETS_LIST)


# --- BULK (mapa nazw i znormalizowane klucze) ---
BULK_LABELS = {
    "Doti": "CzekoladkiDoti",
    "Haribo": "HariboPM",
    "Wawel": "WawelLuz100g",
}
BULK_NORM = set(_norm_key(x) for x in BULK_LABELS.values())

# =============== TABS (podstrony) ===============
tab_dane, tab_pivot, tab_indy, tab_best, tab_comp, tab_cafe, tab_vip, tab_props = st.tabs(["🗂️ Dane", "📈 Tabela przestawna", "👤 Wyniki indywidualne", "🏆 Najlepsi", "🧮 Kreator Konkursów", "☕ Cafe Stats", "👑 VIP stats", "🧩 Proporcje sprzedaży"])

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
                    st.session_state["cached_df"] = add__date_column(_load_from_paths(paths))
        st.info("W chmurze (Streamlit Cloud) to tryb zalecany.")

    else:
        _cfg = load_config()
        data_dir_str = st.text_input("📁 Folder z danymi (lokalnie)", value=_cfg.get("data_dir", str(DEFAULT_DATA_DIR)))
        data_dir = Path(data_dir_str)
        if st.button("🔄 Wczytaj/odśwież dane", type="primary", key="reload_local"):
            save_config(data_dir=data_dir)
            with st.spinner("Wczytywanie danych..."):
                st.session_state["cached_df"] = add__date_column(load_all_data_from_dir(data_dir))

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


# =============== WSPÓLNE OBLICZENIA WSKAŹNIKÓW (bar) ===============
# Jedno źródło prawdy dla KPI barowych używanych w zakładkach
# "Tabela przestawna", "Wyniki indywidualne" i "Najlepsi".
# Wejście: ramka BAROWA (już po _exclude_caf_vip). Wynik jest cache'owany,
# więc zmiana osoby/zakresu dat nie przelicza wszystkiego od nowa.
@st.cache_data(show_spinner=False)
def compute_bar_metrics(dff: pd.DataFrame) -> dict:
    empty = {"users": [], "per_user": pd.DataFrame(), "cinema": {}}
    if dff is None or dff.empty or "UserFullName" not in dff.columns:
        return empty

    d = dff.copy()
    d["__pnorm"] = d["ProductName"].map(_norm_key) if "ProductName" in d.columns else ""
    d["__q"] = pd.to_numeric(d.get("Quantity"), errors="coerce").fillna(0)
    users = sorted(d["UserFullName"].dropna().unique())

    m_extra = d["__pnorm"] == "extranachossauce"
    m_base  = d["__pnorm"].isin({"tackanachossrednia", "tackanachosduza"})
    m_flav  = d["__pnorm"].isin(FLAVORED_NORM)
    m_bpop  = d["__pnorm"].isin(BASE_POP_NORM)
    m_snum  = d["__pnorm"].isin(SHARE_NUM_NORM)
    m_sden  = d["__pnorm"].isin(SHARE_DEN_NORM)
    m_sets  = d["__pnorm"].isin(SETS_NORM)
    m_serowe = d["__pnorm"] == "nachosserowe"
    m_chips = d["__pnorm"].isin(CHIPS_NORM)
    m_chips_den = d["__pnorm"].isin(CHIPS_DEN_NORM)

    def _sum_by_user(mask):
        return d.loc[mask].groupby("UserFullName")["__q"].sum().reindex(users, fill_value=0)

    extra  = _sum_by_user(m_extra); base = _sum_by_user(m_base)
    flav   = _sum_by_user(m_flav);  bpop = _sum_by_user(m_bpop)
    snum   = _sum_by_user(m_snum);  sden = _sum_by_user(m_sden)
    sets_u = _sum_by_user(m_sets)
    serowe = _sum_by_user(m_serowe)
    chips  = _sum_by_user(m_chips)
    chips_den = _sum_by_user(m_chips_den)

    # Liczba transakcji na osobę (ramka jest już bez CAF/VIP)
    if "TransactionId" in d.columns:
        txc = d.groupby("UserFullName")["TransactionId"].nunique().reindex(users, fill_value=0)
    else:
        txc = pd.Series([0] * len(users), index=users)
    txc_f = txc.astype("Float64")

    pct_extra     = (extra / base.replace(0, pd.NA) * 100).astype("Float64")
    pct_popcorny  = (flav / bpop.replace(0, pd.NA) * 100).astype("Float64")
    pct_sharecorn = (snum / sden.replace(0, pd.NA) * 100).astype("Float64")
    pct_sets      = (sets_u / txc_f.replace(0, pd.NA) * 100).astype("Float64")
    # % Nachos Serowe – ten sam mianownik co % Extra Sos (tacki nachos: średnia + duża)
    pct_nachos_serowe = (serowe / base.replace(0, pd.NA) * 100).astype("Float64")
    # % Chipsy – Lay's + Cheetos / opakowania popcorn 5,2l + 6,5l
    pct_chipsy = (chips / chips_den.replace(0, pd.NA) * 100).astype("Float64")

    # Średnia wartość transakcji na osobę (dedup NetAmount na poziomie transakcji)
    if {"TransactionId", "NetAmount"}.issubset(d.columns):
        grp = d.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        per_tx = grp.first().where(grp.nunique(dropna=True) <= 1, grp.sum(min_count=1))
        revenue = per_tx.groupby("UserFullName").sum(min_count=1).reindex(users)
        avg_tx = (revenue / txc_f.replace(0, pd.NA)).astype("Float64")
    else:
        avg_tx = pd.Series([pd.NA] * len(users), index=users, dtype="Float64")

    per_user = pd.DataFrame({
        "tx_count": txc.astype("Int64"),
        "avg_tx": avg_tx,
        "pct_extra": pct_extra,
        "pct_popcorny": pct_popcorny,
        "pct_sharecorn": pct_sharecorn,
        "pct_sets": pct_sets,
        "pct_nachos_serowe": pct_nachos_serowe,
        "pct_chipsy": pct_chipsy,
    }, index=users)

    # Agregaty kina (nieokrąglone – zaokrąglenie po stronie widoków, jak wcześniej)
    def _ratio(num_sum, den_sum):
        return (num_sum / den_sum * 100) if den_sum else None

    tx_total = int(d["TransactionId"].nunique()) if "TransactionId" in d.columns else 0
    cinema = {
        "tx_count": tx_total,
        "pct_extra":     _ratio(float(d.loc[m_extra, "__q"].sum()), float(d.loc[m_base, "__q"].sum())),
        "pct_popcorny":  _ratio(float(d.loc[m_flav, "__q"].sum()),  float(d.loc[m_bpop, "__q"].sum())),
        "pct_sharecorn": _ratio(float(d.loc[m_snum, "__q"].sum()),  float(d.loc[m_sden, "__q"].sum())),
        "pct_sets":      _ratio(float(d.loc[m_sets, "__q"].sum()),  tx_total),
        "pct_nachos_serowe": _ratio(float(d.loc[m_serowe, "__q"].sum()), float(d.loc[m_base, "__q"].sum())),
        "pct_chipsy": _ratio(float(d.loc[m_chips, "__q"].sum()), float(d.loc[m_chips_den, "__q"].sum())),
    }
    if {"TransactionId", "NetAmount"}.issubset(d.columns) and tx_total:
        grp_all = d.groupby("TransactionId")["NetAmount"]
        per_tx_all = grp_all.first().where(grp_all.nunique(dropna=True) <= 1, grp_all.sum(min_count=1))
        cinema["avg_tx"] = float(per_tx_all.sum(min_count=1)) / tx_total
    else:
        cinema["avg_tx"] = None

    return {"users": users, "per_user": per_user, "cinema": cinema}


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
        dff = _exclude_caf_vip(df.loc[mask_d].copy())
    else:
        dff = _exclude_caf_vip(df.copy())

    st.markdown(
            """
            **Jak liczone są wskaźniki?**

            1) **Liczba transakcji** - tylko transakcje barowe
            2) **Średnia wartość transakcji** - średnia wartość **netto** transakcji barowych
            3) **% Extra Sos** – Suma sprzedanych extra sosów / sprzedane tacki nachos  
            4) **% Popcorn Smakowy** – Suma sprzedanych popcornów smakowych / sprzedane opakowania popcorn  
            5) **% Share Corn** – Suma sprzedanych popcornów Share / sprzedane opakowania popcorn  
            6) **% Zestawy** – Suma sprzedanych zestawów / wszystkie transakcje barowe *(więcej szczegółów dotyczących zestawów w podstronie „Proporcje Sprzedaży”)*  
            7) **% Nachos Serowe** – Suma sprzedanych nachos serowych / sprzedane tacki nachos  
            8) **% Chipsy** – Suma sprzedanych Lay's i Cheetos / sprzedane popcorny 5,2 i 6,5  
            """
        )

    required = {"UserFullName", "ProductName", "Quantity"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    m = compute_bar_metrics(dff)
    users_sorted = m["users"]
    pu = m["per_user"]
    cin = m["cinema"]

    # Tabela per-osoba (zaokrąglenia jak wcześniej)
    result = pd.DataFrame(index=users_sorted)
    if not pu.empty:
        result["Liczba transakcji"] = pu["tx_count"]
        result["Średnia wartość transakcji"] = pu["avg_tx"].round(2)
        result["% Extra Sos"] = pu["pct_extra"].round(1)
        result["% Popcorny smakowe"] = pu["pct_popcorny"].round(1)
        result["% ShareCorn"] = pu["pct_sharecorn"].round(1)
        result["% Zestawy"] = pu["pct_sets"].round(1)
        result["% Nachos Serowe"] = pu["pct_nachos_serowe"].round(1)
        result["% Chipsy"] = pu["pct_chipsy"].round(1)
    else:
        for c in ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn", "% Zestawy", "% Nachos Serowe", "% Chipsy"]:
            result[c] = pd.Series(dtype="Float64")

    order = ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn", "% Zestawy", "% Nachos Serowe", "% Chipsy"]
    result = result[order]
    result_sorted = result.sort_values(by="Średnia wartość transakcji", ascending=False, na_position="last")

    # Wiersz "Średnia kina"
    try:
        summary_row = pd.DataFrame({
            "Liczba transakcji": [cin.get("tx_count")],
            "Średnia wartość transakcji": [None if cin.get("avg_tx") is None else round(cin["avg_tx"], 2)],
            "% Extra Sos": [None if cin.get("pct_extra") is None else round(cin["pct_extra"], 1)],
            "% Popcorny smakowe": [None if cin.get("pct_popcorny") is None else round(cin["pct_popcorny"], 1)],
            "% ShareCorn": [None if cin.get("pct_sharecorn") is None else round(cin["pct_sharecorn"], 1)],
            "% Zestawy": [None if cin.get("pct_sets") is None else round(cin["pct_sets"], 1)],
            "% Nachos Serowe": [None if cin.get("pct_nachos_serowe") is None else round(cin["pct_nachos_serowe"], 1)],
            "% Chipsy": [None if cin.get("pct_chipsy") is None else round(cin["pct_chipsy"], 1)],
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
        "% Nachos Serowe": _fmt_pct,
        "% Chipsy": _fmt_pct,
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
            col_names = ["Liczba transakcji", "Średnia wartość transakcji", "% Extra Sos", "% Popcorny smakowe", "% ShareCorn", "% Zestawy", "% Nachos Serowe", "% Chipsy"]
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
    else:
        st.warning("Brak dat — używam wszystkich wierszy.")
        df_all = df.copy()
        users_all = sorted(df.get("UserFullName", pd.Series(dtype=str)).dropna().unique())
        sel_user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None, key="indy_user_nodate")

    bar_df  = _exclude_caf_vip(df_all)
    cafe_df = _keep_caf(df_all)
    vip_df  = _keep_vip(df_all)
    dff = bar_df

    # Wskaźniki barowe – wspólne, cache'owane obliczenia (jedno źródło prawdy)
    m = compute_bar_metrics(dff)
    pu = m["per_user"]
    cin = m["cinema"]

    # KINO
    pct_extra_cinema     = cin.get("pct_extra")
    pct_popcorny_cinema  = cin.get("pct_popcorny")
    pct_sharecorn_cinema = cin.get("pct_sharecorn")
    pct_sets_cinema      = cin.get("pct_sets")
    pct_nachos_serowe_cinema = cin.get("pct_nachos_serowe")
    pct_chipsy_cinema = cin.get("pct_chipsy")
    avg_tr_cinema        = cin.get("avg_tx")

    # OSOBA – wartości z tabeli per-osoba (bez maskowania podzbioru danych)
    if (sel_user is not None) and (sel_user in pu.index):
        _row = pu.loc[sel_user]
        def _val(col):
            v = _row.get(col)
            return None if pd.isna(v) else float(v)
        pct_extra_u     = _val("pct_extra")
        pct_popcorny_u  = _val("pct_popcorny")
        pct_sharecorn_u = _val("pct_sharecorn")
        pct_sets_u      = _val("pct_sets")
        pct_nachos_serowe_u = _val("pct_nachos_serowe")
        pct_chipsy_u = _val("pct_chipsy")
        avg_tr_u        = _val("avg_tx")
        tx_count_u      = 0 if pd.isna(_row.get("tx_count")) else int(_row.get("tx_count"))
    else:
        pct_extra_u = pct_popcorny_u = pct_sharecorn_u = pct_sets_u = pct_nachos_serowe_u = pct_chipsy_u = avg_tr_u = None
        tx_count_u = 0

    def _fmt_pct(x): return "" if x is None else f"{x:.1f} %"
    def _fmt_pln(x): return "" if x is None else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"
    def _fmt_diff_pp(u, c):
        if u is None or c is None: return ""
        d = u - c; s = "+" if d>=0 else "−"; return f"{s}{abs(d):.1f} p.p."
    def _fmt_diff_pln(u, c):
        if u is None or c is None: return ""
        d = u - c; s = "+" if d>=0 else "−"; v = f"{abs(d):,.2f}".replace(",", " ").replace(".", ","); return f"{s}{v} zł"

    # --- Średnie bar/cafe/vip (osoba i kino) ---
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
    has_pct  = any(v is not None for v in [pct_extra_u, pct_popcorny_u, pct_sharecorn_u, pct_nachos_serowe_u, pct_chipsy_u])

    rows = [
        ["Średnia wartość transakcji bar",  avg_tr_bar_u,  avg_tr_bar_cinema,  _fmt_diff_pln(avg_tr_bar_u,  avg_tr_bar_cinema)],
        ["Średnia wartość transakcji cafe", avg_tr_cafe_u, avg_tr_cafe_cinema, _fmt_diff_pln(avg_tr_cafe_u, avg_tr_cafe_cinema)],
        ["Średnia wartość transakcji vip",  avg_tr_vip_u,  avg_tr_vip_cinema,  _fmt_diff_pln(avg_tr_vip_u,  avg_tr_vip_cinema)],
        ["% Extra Sos",           pct_extra_u,      pct_extra_cinema,      _fmt_diff_pp(pct_extra_u,      pct_extra_cinema)],
        ["% Popcorny smakowe",    pct_popcorny_u,   pct_popcorny_cinema,   _fmt_diff_pp(pct_popcorny_u,   pct_popcorny_cinema)],
        ["% ShareCorn",           pct_sharecorn_u,  pct_sharecorn_cinema,  _fmt_diff_pp(pct_sharecorn_u,  pct_sharecorn_cinema)],
        ["% Zestawy",             pct_sets_u,       pct_sets_cinema,       _fmt_diff_pp(pct_sets_u,       pct_sets_cinema)],
        ["% Nachos Serowe",       pct_nachos_serowe_u, pct_nachos_serowe_cinema, _fmt_diff_pp(pct_nachos_serowe_u, pct_nachos_serowe_cinema)],
        ["% Chipsy",              pct_chipsy_u,     pct_chipsy_cinema,     _fmt_diff_pp(pct_chipsy_u,     pct_chipsy_cinema)],
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

    # Formatowanie tabeli: PLN dla wierszy "Średnia wartość transakcji", % dla reszty.
    # Budujemy całe kolumny stringów naraz (zamiast wstawiać stringi do kolumn float
    # przez .loc — pandas 3.x by to odrzucił).
    disp = df_view.copy()
    money_mask = disp["Wskaźnik"].str.startswith("Średnia wartość transakcji")
    for _col in [sel_user, "Średnia kina"]:
        disp[_col] = [
            ("" if pd.isna(v) else (_fmt_pln(v) if is_money else _fmt_pct(v)))
            for v, is_money in zip(disp[_col].tolist(), money_mask.tolist())
        ]
    # --- Kolorowanie kolumny różnicy vs. kino ---
    try:
        def __cs_num__(x):
            import math
            if x is None: return None
            if isinstance(x, (int, float)):
                try:
                    if math.isnan(x): return None
                except Exception: pass
                return float(x)
            s = str(x).strip().replace("−","-")
            for t in ("zł","p.p.","%"): s = s.replace(t,"")
            s = s.replace(" ","").replace("\u00A0","").lstrip("+").replace(",",".")
            try: return float(s)
            except Exception: return None

        def __cs_color__(v):
            n = __cs_num__(v)
            if n is None: return ""
            if n > 0:  return "background-color:#dcfce7; color:#065f46; font-weight:600;"
            if n < 0:  return "background-color:#fee2e2; color:#7f1d1d; font-weight:600;"
            return ""

        _cols = list(disp.columns)
        _delta_col = "Δ vs kino" if "Δ vs kino" in _cols else (_cols[-1] if _cols else None)
        if _delta_col is not None:
            _styled = disp.style.map(__cs_color__, subset=[_delta_col])
            st.dataframe(_styled, use_container_width=True, hide_index=True)
        else:
            st.dataframe(disp, use_container_width=True, hide_index=True)
    except Exception:
        st.dataframe(disp, use_container_width=True, hide_index=True)

    # Wykresy
    st.markdown("### 📊 Wykresy porównawcze")
    _green, _red, _gray = "#16a34a", "#dc2626", "#6b7280"

    def _money_df(orig_user, orig_kino):
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

    # Wskaźniki procentowe (facet: Extra Sos / Popcorny / ShareCorn)
    if has_pct:
        st.caption("Wskaźniki procentowe")
        metrics = ["% Extra Sos", "% Popcorny smakowe", "% ShareCorn", "% Nachos Serowe", "% Chipsy"]
        user_vals = [pct_extra_u, pct_popcorny_u, pct_sharecorn_u, pct_nachos_serowe_u, pct_chipsy_u]
        cinema_vals = [pct_extra_cinema, pct_popcorny_cinema, pct_sharecorn_cinema, pct_nachos_serowe_cinema, pct_chipsy_cinema]
        rows = []
        for mname, u, c in zip(metrics, user_vals, cinema_vals):
            if u is None:
                continue
            uval = u
            cval = 0.0 if c is None else c
            ucol = _gray if (c is None) else (_green if uval >= cval else _red)
            label = ""
            if c is not None:
                d = uval - cval
                s = "+" if d >= 0 else "−"
                label = s + f"{abs(d):.1f}".replace(".", ",") + " p.p."
            rows.append({"Wskaźnik": mname, "Kto": sel_user, "Wartość": float(uval), "kolor": ucol, "diff_label": label, "label_color": ucol})
            rows.append({"Wskaźnik": mname, "Kto": "Średnia kina", "Wartość": float(cval), "kolor": _gray, "diff_label": "", "label_color": _gray})

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

    # --- Osobny wykres: % Zestawy ---
    if pct_sets_u is not None:
        st.markdown("#### % Zestawy")
        uval = float(pct_sets_u)
        cval = 0.0 if (pct_sets_cinema is None) else float(pct_sets_cinema)
        ucol = _gray if (pct_sets_cinema is None) else (_green if uval >= cval else _red)
        label = ""
        if pct_sets_cinema is not None:
            d = uval - cval
            s = "+" if d >= 0 else "−"
            label = s + f"{abs(d):.1f}".replace(".", ",") + " p.p."

        df_chart_sets = pd.DataFrame([
            {"Kto": sel_user, "Wartość": uval, "kolor": ucol, "diff_label": label, "label_color": ucol},
            {"Kto": "Średnia kina", "Wartość": cval, "kolor": _gray, "diff_label": "", "label_color": _gray},
        ])
        base_sets = alt.Chart(df_chart_sets)
        bars_sets = base_sets.mark_bar(size=28).encode(
            x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
            y=alt.Y("Wartość:Q", title="%"),
            color=alt.Color("kolor:N", legend=None, scale=None),
            tooltip=[alt.Tooltip("Kto:N"), alt.Tooltip("Wartość:Q", format=".1f")]
        )
        labels_sets = base_sets.mark_text(dy=-6, size=18).encode(
            x=alt.X("Kto:N", sort=[sel_user, "Średnia kina"], title=""),
            y=alt.Y("Wartość:Q"),
            text=alt.Text("diff_label:N"),
            color=alt.Color("label_color:N", legend=None, scale=None)
        )
        rule_sets = base_sets.transform_filter(alt.datum.Kto == "Średnia kina").mark_rule(
            strokeDash=[6,4], color="#6b7280", opacity=0.8
        ).encode(y="Wartość:Q")
        chart_sets = (bars_sets + labels_sets + rule_sets).properties(width=360, height=480)
        st.altair_chart(chart_sets, use_container_width=False)

    # --- Struktura sprzedaży — zestawy (osoba) ---
    st.markdown("### 🧩 Struktura sprzedaży — zestawy (osoba)")
    try:
        dff_u_sets = dff[dff["UserFullName"] == sel_user].copy()
        if dff_u_sets.empty:
            st.info("Brak danych o zestawach dla wybranego zleceniobiorcy w wybranym okresie.")
        else:
            SETS_LIST_LOCAL = ["XLOffer+","Sredni+","Duzy+","Family1+1","Duet+","MAXI+","Szkolny+","DuetShare+"]
            if "__pnorm" not in dff_u_sets.columns:
                dff_u_sets["__pnorm"] = dff_u_sets["ProductName"].map(_norm_key)
            pn  = dff_u_sets["__pnorm"]
            qty = pd.to_numeric(dff_u_sets.get("Quantity"), errors="coerce").fillna(0)
            rows = []
            total_sets = 0.0
            for name in SETS_LIST_LOCAL:
                key = _norm_key(name)
                cnt = float(qty[pn == key].sum())
                rows.append({"Zestaw": name, "Sztuki": cnt})
                total_sets += cnt
            for r in rows:
                r["Udział (%)"] = (None if total_sets == 0 else round(r["Sztuki"] / total_sets * 100, 1))
            df_sets_user = pd.DataFrame(rows).sort_values("Udział (%)", ascending=False, na_position="last")

            def _fmt_int_sets(v):
                try:
                    return f"{int(v):,}".replace(",", " ")
                except Exception:
                    return ""
            def _fmt_pct_sets(v):
                return "" if pd.isna(v) else f"{v:.1f} %"

            styled_sets_user = df_sets_user.style.format({"Sztuki": _fmt_int_sets, "Udział (%)": _fmt_pct_sets})
            st.dataframe(styled_sets_user, use_container_width=True, hide_index=True)
            st.caption(f"Razem zestawów (osoba): {int(total_sets):,}".replace(",", " "))

            if total_sets > 0:
                try:
                    df_pie_u = df_sets_user.dropna(subset=["Udział (%)"]).copy()
                    if not df_pie_u.empty:
                        chart_pie_u = (
                            alt.Chart(df_pie_u)
                            .mark_arc()
                            .encode(
                                theta=alt.Theta(field="Sztuki", type="quantitative"),
                                color=alt.Color(
                                    field="Zestaw", type="nominal",
                                    legend=alt.Legend(title="Zestaw", labelFontSize=16, titleFontSize=18, symbolSize=200)
                                ),
                                tooltip=[
                                    alt.Tooltip("Zestaw:N"),
                                    alt.Tooltip("Sztuki:Q", format=",.0f"),
                                    alt.Tooltip("Udział (%):Q", format=".1f"),
                                ],
                            )
                            .properties(width=380, height=360)
                        )
                        st.altair_chart(chart_pie_u, use_container_width=True)
                except Exception:
                    st.caption("Nie udało się wyrenderować wykresu kołowego (zestawy — osoba).")
    except Exception as ex:
        st.warning(f"Nie udało się przygotować 'Struktura sprzedaży — zestawy (osoba)': {ex}")


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
        dff = _exclude_caf_vip(df.loc[mask].copy())
    else:
        dff = _exclude_caf_vip(df.copy())

    required = {"UserFullName", "ProductName", "Quantity"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    m = compute_bar_metrics(dff)
    pu = m["per_user"]
    cin = m["cinema"]
    tx_bar_count_by_user = pu["tx_count"] if not pu.empty else pd.Series(dtype="Int64")
    empty_series = pd.Series(dtype="Float64")

    def style_over_avg(df_in: pd.DataFrame, avg_val, is_pct: bool):
        def _fmt_pct(x): return "" if pd.isna(x) else f"{x:.1f} %"
        def _fmt_pln(x):
            if pd.isna(x): return ""
            s = f"{x:,.2f}".replace(",", " ").replace(".", ","); return s + " zł"
        def _color(v):
            try:
                ok = (not pd.isna(v)) and (avg_val is not None) and (not pd.isna(avg_val)) and (v >= avg_val)
                return "background-color: #dcfce7; font-weight: 600" if ok else ""
            except Exception:
                return ""
        sty = df_in.style.map(_color, subset=["Wartość"])
        return sty.format({"Wartość": _fmt_pct if is_pct else _fmt_pln})

    def _rank_table(value_series):
        t = pd.DataFrame({"Wartość": value_series, "Liczba transakcji bar": tx_bar_count_by_user}).sort_values("Wartość", ascending=False, na_position="last")
        return t.rename_axis("Zleceniobiorca").reset_index()[["Zleceniobiorca","Liczba transakcji bar","Wartość"]]

    # % Extra Sos
    st.markdown("#### % Extra Sos")
    df_extra = _rank_table(pu["pct_extra"] if not pu.empty else empty_series)
    avg_extra = cin.get("pct_extra")
    if avg_extra is not None: st.caption(f"Średnia kina: **{avg_extra:.1f} %**")
    st.dataframe(style_over_avg(df_extra, avg_extra, is_pct=True), use_container_width=True, hide_index=True)

    # % Popcorny smakowe
    st.markdown("#### % Popcorny smakowe")
    df_pop = _rank_table(pu["pct_popcorny"] if not pu.empty else empty_series)
    avg_pop = cin.get("pct_popcorny")
    if avg_pop is not None: st.caption(f"Średnia kina: **{avg_pop:.1f} %**")
    st.dataframe(style_over_avg(df_pop, avg_pop, is_pct=True), use_container_width=True, hide_index=True)

    # % ShareCorn
    st.markdown("#### % ShareCorn")
    df_share = _rank_table(pu["pct_sharecorn"] if not pu.empty else empty_series)
    avg_share = cin.get("pct_sharecorn")
    if avg_share is not None: st.caption(f"Średnia kina: **{avg_share:.1f} %**")
    st.dataframe(style_over_avg(df_share, avg_share, is_pct=True), use_container_width=True, hide_index=True)

    # % Nachos Serowe
    st.markdown("#### % Nachos Serowe")
    df_serowe = _rank_table(pu["pct_nachos_serowe"] if not pu.empty else empty_series)
    avg_serowe = cin.get("pct_nachos_serowe")
    if avg_serowe is not None: st.caption(f"Średnia kina: **{avg_serowe:.1f} %**")
    st.dataframe(style_over_avg(df_serowe, avg_serowe, is_pct=True), use_container_width=True, hide_index=True)

    # % Chipsy
    st.markdown("#### % Chipsy")
    df_chipsy = _rank_table(pu["pct_chipsy"] if not pu.empty else empty_series)
    avg_chipsy = cin.get("pct_chipsy")
    if avg_chipsy is not None: st.caption(f"Średnia kina: **{avg_chipsy:.1f} %**")
    st.dataframe(style_over_avg(df_chipsy, avg_chipsy, is_pct=True), use_container_width=True, hide_index=True)

    # Średnia wartość transakcji
    st.markdown("#### Średnia wartość transakcji")
    if {"TransactionId", "NetAmount"}.issubset(dff.columns):
        df_avg = _rank_table(pu["avg_tx"] if not pu.empty else empty_series)
        avg_global = cin.get("avg_tx")
        if avg_global is not None:
            st.caption(f"Średnia kina: **{avg_global:,.2f} zł**".replace(",", " ").replace(".", ","))
        st.dataframe(style_over_avg(df_avg, avg_global, is_pct=False), use_container_width=True, hide_index=True)
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
        
    st.markdown(
            """
            **Jak działa kreator?**
            
            W "Produkty i Punktacja" wybierz produkty które chcesz uwzględniać w swoim konkursie oraz za ile punktów mają być punktowane.
            W "Mianownik współczynnika" wybierz przez co chcesz dzielić twój wynik, jeśli nie planujesz go przez nic dzielić po prostu wybierz "Stała 1".
            Jeśli twój konkurs zakłada minimum transakcji wpisz je, program wyświetli dwie tabele, jedną z osobami które spełniły minimum i drugą z tymi, którzy się nie zakwalifikowali :D
            """
        )

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

# ---------- Zakładka: Proporcje sprzedaży ----------
with tab_props:
    st.subheader("Proporcje sprzedaży")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    # Zakres dat
    if "__date" in df.columns and df["__date"].notna().any():
        min_d, max_d = df["__date"].dropna().min(), df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="props_date")
        d_from, d_to = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_d, max_d)
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
    else:
        dff = df.copy()

    with st.expander("Zestawy", expanded=False):
        if dff.empty:
            st.info("Brak danych w wybranym zakresie dat.")
        else:
            # Lista zestawów (wraz z DuetShare+)
            SETS_LIST_LOCAL = ["XLOffer+","Sredni+","Duzy+","Family1+1","Duet+","MAXI+","Szkolny+","DuetShare+"]

            # Użyj znormalizowanej nazwy produktu jeśli dostępna
            if "__pnorm" in dff.columns:
                pn = dff["__pnorm"]
                if "SETS_NORM" in globals():
                    set_keys = list(SETS_NORM) if isinstance(SETS_NORM, (list, tuple, set)) else [str(SETS_NORM)]
                else:
                    set_keys = [str(x).upper().strip() for x in SETS_LIST_LOCAL]
            else:
                pn = dff["ProductName"].astype(str).str.upper().str.strip()
                set_keys = [str(x).upper().strip() for x in SETS_LIST_LOCAL]

            qty = pd.to_numeric(dff.get("Quantity"), errors="coerce").fillna(0)
            total_sets = float(qty[pn.isin(set_keys)].sum())

            rows = []
            for name in SETS_LIST_LOCAL:
                key = str(name).upper().strip()
                cnt = float(qty[pn == key].sum())
                share = (cnt/total_sets*100) if total_sets else None
                rows.append({"Zestaw": name, "Sztuki": cnt, "Udział (%)": (None if share is None else round(share, 1))})

            df_sets = pd.DataFrame(rows).sort_values("Udział (%)", ascending=False, na_position="last")

            def _fmt_int(v):
                try:
                    return f"{int(v):,}".replace(",", " ")
                except Exception:
                    return ""

            def _fmt_pct(v):
                import pandas as pd
                return "" if pd.isna(v) else f"{v:.1f} %"

            styled = df_sets.style.format({"Sztuki": _fmt_int, "Udział (%)": _fmt_pct})
            st.dataframe(styled, use_container_width=True, hide_index=True)
            st.caption(f"Razem zestawów w okresie: {int(total_sets):,}".replace(",", " "))
                # --- Wykres kołowy: udziały zestawów ---
        try:
            import altair as alt  # lokalny import, nie wymaga zmian w nagłówku pliku
            df_pie = df_sets.dropna(subset=["Udział (%)"]).copy()
            if not df_pie.empty:
                chart_pie = (
                    alt.Chart(df_pie)
                    .mark_arc()
                    .encode(
                        theta=alt.Theta(field="Sztuki", type="quantitative"),
                        color=alt.Color(field="Zestaw", type="nominal", legend=alt.Legend(title="Zestaw")),
                        tooltip=[
                            alt.Tooltip("Zestaw:N"),
                            alt.Tooltip("Sztuki:Q", format=",.0f"),
                            alt.Tooltip("Udział (%):Q", format=".1f"),
                        ],
                    )
                    .properties(width=380, height=360)
                    .configure_legend(
                        labelFontSize=30,
                        titleFontSize=35,
                        symbolSize=400,   # (opcjonalnie)
                    )
                )
                st.altair_chart(chart_pie, use_container_width=True)
        except Exception:
            st.caption("Nie udało się wyrenderować wykresu kołowego.")
            
    # --- Expander: Nachos BBQ vs serowe ---
    with st.expander("Nachos BBQ vs serowe", expanded=False):
        if dff.empty:
            st.info("Brak danych w wybranym zakresie dat.")
        else:
            # Klucze produktów: tacka średnia/duża jako baza nachos
            BASE_NACHOS_KEYS_NORM = {"tackanachossrednia", "tackanachosduza"}
            CHEESE_KEY_NORM = "nachosserowe"
    
            # Wybór kolumny z nazwą produktu (znormalizowana jeśli dostępna)
            if "__pnorm" in dff.columns:
                pn = dff["__pnorm"].astype(str)
            else:
                pn = dff["ProductName"].astype(str).str.upper().str.strip()
                # lokalna normalizacja: alfanum + lower
                pn = pn.apply(lambda s: "".join(ch for ch in s if ch.isalnum()).lower())
    
            qty = pd.to_numeric(dff.get("Quantity"), errors="coerce").fillna(0)
    
            total_nachos = float(qty[pn.isin(BASE_NACHOS_KEYS_NORM)].sum())
            serowe_cnt   = float(qty[pn == CHEESE_KEY_NORM].sum())
            # BBQ = całość nachos - serowe
            bbq_cnt = max(0.0, total_nachos - serowe_cnt)
    
            if total_nachos > 0:
                serowe_pct = round(serowe_cnt / total_nachos * 100, 1)
                bbq_pct    = round(100.0 - serowe_pct, 1)
            else:
                serowe_pct = None
                bbq_pct    = None
    
            rows = [
                {"Kategoria": "Nachos SEROWE", "Sztuki": serowe_cnt, "Udział (%)": serowe_pct},
                {"Kategoria": "Nachos BBQ",    "Sztuki": bbq_cnt,    "Udział (%)": bbq_pct},
            ]
            df_nachos = pd.DataFrame(rows)
    
            def _fmt_int(v):
                try:
                    return f"{int(round(v)):,}".replace(",", " ")
                except Exception:
                    return ""
            def _fmt_pct(v):
                import pandas as pd
                return "" if pd.isna(v) else f"{float(v):.1f} %"
    
            styled_nachos = df_nachos.style.format({"Sztuki": _fmt_int, "Udział (%)": _fmt_pct})
            st.dataframe(styled_nachos, use_container_width=True, hide_index=True)
            st.caption(f"Łącznie tacki nachos (średnia + duża): {int(round(total_nachos)):,}".replace(",", " "))
    
            # Wykres kołowy: serowe vs BBQ
            try:
                import altair as alt
                df_pie_n = df_nachos.dropna(subset=["Udział (%)"]).copy()
                if not df_pie_n.empty:
                    chart_pie_n = (
                        alt.Chart(df_pie_n)
                        .mark_arc()
                        .encode(
                            theta=alt.Theta(field="Sztuki", type="quantitative"),
                            color=alt.Color(field="Kategoria", type="nominal",
                                            legend=alt.Legend(title="Kategoria", labelFontSize=16, titleFontSize=18, symbolSize=200)),
                            tooltip=[
                                alt.Tooltip("Kategoria:N"),
                                alt.Tooltip("Sztuki:Q", format=",.0f"),
                                alt.Tooltip("Udział (%):Q", format=".1f"),
                            ],
                        )
                        .properties(width=380, height=360)
                        .configure_legend(
                            labelFontSize=30,
                            titleFontSize=35,
                            symbolSize=400,   # (opcjonalnie)
                        )   
                    )
                    st.altair_chart(chart_pie_n, use_container_width=True)
            except Exception:
                st.caption("Nie udało się wyrenderować wykresu kołowego (nachos).")
    # --- Expander: Bulk ---
    with st.expander("Bulk", expanded=False):
        if dff.empty:
            st.info("Brak danych w wybranym zakresie dat.")
        else:
            # Zapewnij kolumnę __pnorm spójną z resztą aplikacji
            if "__pnorm" not in dff.columns:
                dff["__pnorm"] = dff["ProductName"].map(_norm_key)

            pn  = dff["__pnorm"].astype(str)
            qty = pd.to_numeric(dff.get("Quantity"), errors="coerce").fillna(0)

            # Zlicz sztuki dla Doti/Haribo/Wawel po znormalizowanych kluczach
            rows = []
            total_bulk = 0.0
            for label, orig in BULK_LABELS.items():
                key = _norm_key(orig)
                cnt = float(qty[pn == key].sum())
                rows.append({"Produkt": label, "Gramy": cnt})
                total_bulk += cnt

            # Udziały % (suma trzech pozycji = 100%)
            for r in rows:
                r["Udział (%)"] = (None if total_bulk == 0 else round(r["Gramy"] / total_bulk * 100, 1))

            df_bulk = pd.DataFrame(rows).sort_values("Udział (%)", ascending=False, na_position="last")

            # Formatowanie tabeli
            def _fmt_int(v):
                try:
                    return f"{int(round(v)):,}".replace(",", " ")
                except Exception:
                    return ""
            def _fmt_pct(v):
                import pandas as pd
                return "" if pd.isna(v) else f"{float(v):.1f} %"

            styled_bulk = df_bulk.style.format({"Gramy": _fmt_int, "Udział (%)": _fmt_pct})
            st.dataframe(styled_bulk, use_container_width=True, hide_index=True)
            st.caption(f"Razem (Bulk): {int(round(total_bulk)):,}".replace(",", " "))

            # Wykres kołowy
            try:
                import altair as alt
                df_pie_b = df_bulk.dropna(subset=["Udział (%)"]).copy()
                if not df_pie_b.empty:
                    chart_pie_b = (
                        alt.Chart(df_pie_b)
                        .mark_arc()
                        .encode(
                            theta=alt.Theta(field="Gramy", type="quantitative"),
                            color=alt.Color(field="Produkt", type="nominal",
                                            legend=alt.Legend(title="Produkt", labelFontSize=16, titleFontSize=18, symbolSize=200)),
                            tooltip=[
                                alt.Tooltip("Produkt:N"),
                                alt.Tooltip("Gramy:Q", format=",.0f"),
                                alt.Tooltip("Udział (%):Q", format=".1f"),
                            ],
                        )
                        .properties(width=380, height=360)
                    )
                    st.altair_chart(chart_pie_b, use_container_width=True)
            except Exception:
                st.caption("Nie udało się wyrenderować wykresu kołowego (Bulk).")
