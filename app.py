import os
import io
from pathlib import Path
import pandas as pd
import streamlit as st

# ========================= USTAWIENIA STRONY =========================
st.set_page_config(page_title="CineStats — sprzedaż i KPI", layout="wide")
st.title("🎬 CineStats — sprzedaż i KPI (multi-POS)")

# ========================= POMOCNICZE =========================
def read_any_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".csv", ".txt"):
        try:
            return pd.read_csv(path, sep=None, engine="python")
        except Exception:
            return pd.read_csv(path)
    if ext == ".xlsx":
        # wymaga 'openpyxl' — mamy w requirements
        return pd.read_excel(path, engine="openpyxl")
    raise ValueError(f"Nieobsługiwany format pliku: {ext}")

def add__date_column(df: pd.DataFrame) -> pd.DataFrame:
    """Próbuje zbudować kolumnę __date na podstawie kilku możliwych nazw kolumn."""
    if "__date" in df.columns:
        return df
    df = df.copy()
    candidates = [
        "OrderDate", "CreatedAt", "CreationDate", "BusinessDate",
        "TransactionDate", "Date", "ReceiptDate"
    ]
    col = None
    for c in candidates:
        if c in df.columns:
            col = c
            break
    if col is None:
        # bez dat — trudno, ale pozwólmy działać
        df["__date"] = pd.NaT
        return df
    df["__date"] = pd.to_datetime(df[col], errors="coerce", dayfirst=True, utc=False).dt.date
    return df

def ensure_data_or_stop() -> pd.DataFrame:
    df = st.session_state.get("df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych — wczytaj pliki w zakładce **Dane**.")
        st.stop()
    return df

def _norm_key(x) -> str:
    s = "" if x is None else str(x)
    s = s.lower()
    return "".join(ch for ch in s if ch.isalnum())

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

# ---------- Formatery ----------
def fmt_pln(x):
    return "" if pd.isna(x) else f"{x:,.2f}".replace(",", " ").replace(".", ",") + " zł"

def fmt_pct(x):
    return "" if pd.isna(x) else f"{x:.1f} %"

# ---------- Zestawy produktów do KPI ----------
FLAVORED_NORM = { _norm_key(x) for x in [
    "BEKON-SER","BEKON-SER/SOL","CHEDDAR/SOL","KARMEL.","KARMEL/BEKON.","KARMEL/CHEDDAR.","KARMEL/SOL.","SER-CHEDDAR"
]}
BASE_POP_NORM  = { _norm_key(x) for x in [
    "KubekPopcorn1,5l","KubekPopcorn2,3l","KubekPopcorn4,2l","KubekPopcorn5,2l","KubekPopcorn6,5l"
]}
SHARE_NUM_NORM = { _norm_key("KubekPopcorn6,5l") }
SHARE_DEN_NORM = BASE_POP_NORM - SHARE_NUM_NORM

# ========================= ZAKŁADKI =========================
tab_dane, tab_pivot, tab_indy, tab_best, tab_comp, tab_cafe, tab_vip = st.tabs(
    ["🗂️ Dane", "📈 Tabela przestawna", "👤 Wyniki indywidualne", "🏆 Najlepsi", "🧮 Kreator Konkursów", "☕ Cafe Stats", "VIP stats"]
)

# ---------- Zakładka: Dane ----------
with tab_dane:
    st.subheader("🗂️ Wczytaj dane")
    uploaded = st.file_uploader("Wrzuć pliki (.xlsx/.csv/.txt)", type=["xlsx","csv","txt"], accept_multiple_files=True)
    if st.button("🔄 Wczytaj/odśwież"):
        if not uploaded:
            st.warning("Dodaj pliki.")
        else:
            tmpdir = Path(st.session_state.get("__tmpdir", "."))
            frames = []
            fails = []
            for uf in uploaded:
                p = Path(f"./__uploaded__{uf.name}")
                p.write_bytes(uf.read())
                try:
                    df = read_any_table(p)
                    df["__source_file"] = uf.name
                    frames.append(df)
                except Exception as e:
                    fails.append((uf.name, str(e)))
            if frames:
                df = pd.concat(frames, ignore_index=True)
                # sanity columns
                for c in ["Quantity","NetAmount"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                st.session_state["df"] = df
                st.success(f"Wczytano {len(df):,} wierszy.".replace(",", " "))
                st.dataframe(df.head(300), use_container_width=True)
            if fails:
                st.error("Niektóre pliki nie zostały wczytane:")
                for n, e in fails:
                    st.write(f"- {n}: {e}")

# ---------- Wspólne: funkcja wyboru zakresu dat ----------
def date_filtered(df: pd.DataFrame, key: str):
    df = add__date_column(df)
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key=key)
        if isinstance(picked, (list, tuple)) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        return df.loc[mask_d].copy()
    return df.copy()

# ---------- Zakładka: Tabela przestawna ----------
with tab_pivot:
    st.subheader("📈 Tabela przestawna (bez POS: CAF/VIP)")
    df = ensure_data_or_stop()
    dff = date_filtered(df, key="pivot_date")
    dff = _exclude_caf_vip(dff)

    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(dff.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()

    dff["Quantity"] = pd.to_numeric(dff["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in dff.columns:
        dff["NetAmount"] = pd.to_numeric(dff["NetAmount"], errors="coerce")
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    users = sorted(dff["UserFullName"].dropna().unique())

    # % Extra Sos
    mask_extra = dff["__pnorm"].eq("extranachossauce")
    mask_base  = dff["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})
    extra = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    base  = dff.loc[mask_base ].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_extra = (extra / base.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % Popcorny smakowe
    mask_flav = dff["__pnorm"].isin(FLAVORED_NORM)
    mask_basep= dff["__pnorm"].isin(BASE_POP_NORM)
    flav = dff.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    basep= dff.loc[mask_basep].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_pop = (flav / basep.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % ShareCorn
    mask_sn = dff["__pnorm"].isin(SHARE_NUM_NORM)
    mask_sd = dff["__pnorm"].isin(SHARE_DEN_NORM)
    num = dff.loc[mask_sn].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    den = dff.loc[mask_sd].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_share = (num / den.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # Transakcje i średnia wartość transakcji
    if {"TransactionId","NetAmount"}.issubset(dff.columns):
        grp = dff.groupby(["UserFullName","TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx = f.where(nun <= 1, s)
        revenue = per_tx.groupby("UserFullName").sum(min_count=1).reindex(users)
        tx_count = dff.groupby("UserFullName")["TransactionId"].nunique().reindex(users).astype("Int64")
        avg = (revenue / tx_count.replace(0, pd.NA)).astype("Float64").round(2)
    else:
        tx_count = pd.Series([pd.NA]*len(users), index=users, dtype="Int64")
        avg = pd.Series([pd.NA]*len(users), index=users, dtype="Float64")

    table = pd.DataFrame(index=users)
    table["Liczba transakcji"] = tx_count
    table["Średnia wartość transakcji"] = avg
    table["% Extra Sos"] = pct_extra
    table["% Popcorny smakowe"] = pct_pop
    table["% ShareCorn"] = pct_share

    # Średnia kina (bez CAF/VIP) — dla % liczona globalnie
    def _glob_pct(num_mask, den_mask):
        n = float(dff.loc[num_mask, "Quantity"].sum())
        d = float(dff.loc[den_mask, "Quantity"].sum())
        return None if d == 0 else round(n/d*100, 1)
    if "TransactionId" in dff.columns and "NetAmount" in dff.columns:
        g = dff.groupby("TransactionId")["NetAmount"]
        per_tx_all = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        glob_avg = None if dff["TransactionId"].nunique()==0 else round(float(per_tx_all.sum(min_count=1))/int(dff["TransactionId"].nunique()), 2)
    else:
        glob_avg = None

    summary = pd.DataFrame({
        "Liczba transakcji": [int(dff["TransactionId"].nunique()) if "TransactionId" in dff.columns else None],
        "Średnia wartość transakcji": [glob_avg],
        "% Extra Sos": [_glob_pct(mask_extra, mask_base)],
        "% Popcorny smakowe": [_glob_pct(mask_flav, mask_basep)],
        "% ShareCorn": [_glob_pct(mask_sn, mask_sd)]
    }, index=["Średnia kina"])

    out = pd.concat([summary, table], axis=0)
    out = out.sort_values(by="Średnia wartość transakcji", ascending=False, na_position="last")

    def _row_style(row):
        if row.name == "Średnia kina":
            return ['font-weight:700; background-color:#f3f4f6' for _ in row]
        return ['' for _ in row]

    styled = (out.style
              .format({"Średnia wartość transakcji": fmt_pln,
                       "% Extra Sos": fmt_pct,
                       "% Popcorny smakowe": fmt_pct,
                       "% ShareCorn": fmt_pct})
              .apply(_row_style, axis=1))
    st.dataframe(styled, use_container_width=True)

    # Eksport
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            out.to_excel(w, index=True, sheet_name="Wskaźniki")
            wb = w.book; ws = w.sheets["Wskaźniki"]
            fmt_bold = wb.add_format({"bold": True})
            fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
            fmt_int = wb.add_format({"num_format": "0"})
            ws.set_row(0, None, fmt_bold)
            ws.set_column("A:A", 28)
            ws.set_column("B:B", 16, fmt_int)
            ws.set_column("C:C", 22, fmt_pln)
    except Exception as ex:
        st.warning(f"Nie udało się przygotować XLSX: {ex}")
    else:
        st.download_button("⬇️ Pobierz XLSX (Tabela przestawna)", data=buf.getvalue(),
                           file_name="TabelaPrzestawna.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- Zakładka: Wyniki indywidualne ----------
with tab_indy:
    st.subheader("👤 Wyniki indywidualne (bez POS: CAF/VIP)")
    df = ensure_data_or_stop()
    dff = date_filtered(df, key="indy_date")
    dff = _exclude_caf_vip(dff)

    req = {"UserFullName","ProductName","Quantity"}
    if not req.issubset(dff.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()

    users_all = sorted(dff["UserFullName"].dropna().unique())
    user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None)

    dff["Quantity"] = pd.to_numeric(dff["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in dff.columns: dff["NetAmount"] = pd.to_numeric(dff["NetAmount"], errors="coerce")
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)

    # Funkcje %
    def _pct_local(frame, num_mask, den_mask):
        n = float(frame.loc[num_mask, "Quantity"].sum())
        d = float(frame.loc[den_mask, "Quantity"].sum())
        return None if d == 0 else round(n/d*100, 1)

    # Kino (global)
    if {"TransactionId","NetAmount"}.issubset(dff.columns):
        g = dff.groupby("TransactionId")["NetAmount"]
        per_tx_all = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        avg_cinema = None if dff["TransactionId"].nunique()==0 else round(float(per_tx_all.sum(min_count=1))/int(dff["TransactionId"].nunique()), 2)
    else:
        avg_cinema = None
    row_cinema = {
        "Średnia wartość transakcji": avg_cinema,
        "% Extra Sos": _pct_local(dff, dff["__pnorm"].eq("extranachossauce"), dff["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})),
        "% Popcorny smakowe": _pct_local(dff, dff["__pnorm"].isin(FLAVORED_NORM), dff["__pnorm"].isin(BASE_POP_NORM)),
        "% ShareCorn": _pct_local(dff, dff["__pnorm"].isin(SHARE_NUM_NORM), dff["__pnorm"].isin(SHARE_DEN_NORM)),
    }

    # Osoba
    du = dff[dff["UserFullName"] == user].copy()
    if {"TransactionId","NetAmount"}.issubset(du.columns):
        g = du.groupby("TransactionId")["NetAmount"]
        per_tx = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        txc = int(du["TransactionId"].nunique())
        avg_user = None if txc==0 else round(float(per_tx.sum(min_count=1))/txc, 2)
    else:
        avg_user, txc = None, 0
    st.metric("Liczba transakcji (osoba)", f"{txc:,}".replace(",", " "))

    row_user = {
        "Średnia wartość transakcji": avg_user,
        "% Extra Sos": _pct_local(du, du["__pnorm"].eq("extranachossauce"), du["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})),
        "% Popcorny smakowe": _pct_local(du, du["__pnorm"].isin(FLAVORED_NORM), du["__pnorm"].isin(BASE_POP_NORM)),
        "% ShareCorn": _pct_local(du, du["__pnorm"].isin(SHARE_NUM_NORM), du["__pnorm"].isin(SHARE_DEN_NORM)),
    }

    disp = pd.DataFrame({
        "Wskaźnik": ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"],
        user: [row_user[k] for k in ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"]],
        "Średnia kina": [row_cinema[k] for k in ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"]],
    })
    styled = (disp.style
              .format({user: lambda v: fmt_pln(v) if isinstance(v,(int,float)) and disp.loc[0,"Wskaźnik"]=="Średnia wartość transakcji" else v,
                       "Średnia kina": lambda v: fmt_pln(v) if isinstance(v,(int,float)) and disp.loc[0,"Wskaźnik"]=="Średnia wartość transakcji" else v})
             )
    st.dataframe(styled, use_container_width=True, hide_index=True)

# ---------- Zakładka: Najlepsi ----------
with tab_best:
    st.subheader("🏆 Najlepsi (bez POS: CAF/VIP)")
    df = ensure_data_or_stop()
    dff = date_filtered(df, key="best_date")
    dff = _exclude_caf_vip(dff)

    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(dff.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()

    dff["Quantity"] = pd.to_numeric(dff["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in dff.columns:
        dff["NetAmount"] = pd.to_numeric(dff["NetAmount"], errors="coerce")
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    users = sorted(dff["UserFullName"].dropna().unique())

    def _rank_series(series, desc=True):
        dfv = pd.DataFrame({"Wartość": series})
        dfv = dfv.sort_values("Wartość", ascending=not desc, na_position="last")
        return dfv

    # Średnie kina do progów (kolorowanie)
    def _glob_pct(num_mask, den_mask):
        n = float(dff.loc[num_mask, "Quantity"].sum())
        d = float(dff.loc[den_mask, "Quantity"].sum())
        return None if d == 0 else round(n/d*100, 1)

    # % Extra Sos
    mask_extra = dff["__pnorm"].eq("extranachossauce")
    mask_base  = dff["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})
    extra = dff.loc[mask_extra].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    base  = dff.loc[mask_base ].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_extra = (extra / base.replace(0, pd.NA) * 100).astype("Float64")
    avg_extra = _glob_pct(mask_extra, mask_base)
    st.markdown(f"#### % Extra Sos  |  Średnia kina: **{'' if avg_extra is None else f'{avg_extra:.1f} %'}**")
    st.dataframe(_rank_series(pct_extra), use_container_width=True)

    # % Popcorny smakowe
    mask_flav = dff["__pnorm"].isin(FLAVORED_NORM)
    mask_basep= dff["__pnorm"].isin(BASE_POP_NORM)
    flav = dff.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    basep= dff.loc[mask_basep].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_pop = (flav / basep.replace(0, pd.NA) * 100).astype("Float64")
    avg_pop = _glob_pct(mask_flav, mask_basep)
    st.markdown(f"#### % Popcorny smakowe  |  Średnia kina: **{'' if avg_pop is None else f'{avg_pop:.1f} %'}**")
    st.dataframe(_rank_series(pct_pop), use_container_width=True)

    # % ShareCorn
    mask_sn = dff["__pnorm"].isin(SHARE_NUM_NORM)
    mask_sd = dff["__pnorm"].isin(SHARE_DEN_NORM)
    num = dff.loc[mask_sn].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    den = dff.loc[mask_sd].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_share = (num / den.replace(0, pd.NA) * 100).astype("Float64")
    avg_share = _glob_pct(mask_sn, mask_sd)
    st.markdown(f"#### % ShareCorn  |  Średnia kina: **{'' if avg_share is None else f'{avg_share:.1f} %'}**")
    st.dataframe(_rank_series(pct_share), use_container_width=True)

    # Średnia wartość transakcji
    st.markdown("#### Średnia wartość transakcji")
    if {"TransactionId","NetAmount"}.issubset(dff.columns):
        grp = dff.groupby(["UserFullName","TransactionId"])["NetAmount"]
        per_tx = grp.first().where(grp.nunique(dropna=True) <= 1, grp.sum(min_count=1))
        revenue = per_tx.groupby("UserFullName").sum(min_count=1).reindex(users)
        txc = dff.groupby("UserFullName")["TransactionId"].nunique().reindex(users)
        avg_by_user = (revenue / txc.replace(0, pd.NA)).astype("Float64")
        st.dataframe(_rank_series(avg_by_user), use_container_width=True)
    else:
        st.info("Brak TransactionId/NetAmount — nie policzę średniej transakcji.")

# ---------- Zakładka: Kreator Konkursów ----------
with tab_comp:
    st.subheader("🧮 Kreator Konkursów (bez POS: CAF/VIP)")
    df = ensure_data_or_stop()
    dff = date_filtered(df, key="comp_date")
    dff = _exclude_caf_vip(dff)

    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(dff.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()

    dff["Quantity"] = pd.to_numeric(dff["Quantity"], errors="coerce").fillna(0)
    products_all = sorted(dff["ProductName"].dropna().unique())
    users = sorted(dff["UserFullName"].dropna().unique())

    # grupa "Popcorny Smakowe"
    dff["__pnorm"] = dff["ProductName"].map(_norm_key)
    mask_flav = dff["__pnorm"].isin(FLAVORED_NORM)
    GROUP_FLAVORED = "Popcorny Smakowe"
    products_plus = [GROUP_FLAVORED] + products_all

    # konfigurator
    if "contest_rows" not in st.session_state:
        st.session_state["contest_rows"] = [ (None, 1.0) ]

    cols = st.columns([1,1,6])
    with cols[0]:
        if st.button("➕ Dodaj pozycję"):
            st.session_state["contest_rows"].append((None, 1.0))
    with cols[1]:
        if st.button("➖ Usuń ostatnią", disabled=len(st.session_state["contest_rows"])<=1):
            st.session_state["contest_rows"].pop()

    pairs = []
    for i, (p0, pts0) in enumerate(st.session_state["contest_rows"]):
        c1, c2 = st.columns([3,1])
        with c1:
            idx = products_plus.index(p0) if p0 in products_plus else 0
            prod = st.selectbox(f"Produkt #{i+1}", options=products_plus, index=idx, key=f"contest_prod_{i}")
        with c2:
            pts = st.number_input(f"Punkty #{i+1}", value=float(pts0), step=0.5, key=f"contest_pts_{i}")
        pairs.append((prod, float(pts)))

    den_mode = st.selectbox("Mianownik", ["Liczba transakcji", "Wybrany produkt", "Stała 1"])
    den_prod = None
    if den_mode == "Wybrany produkt":
        den_prod = st.selectbox("Produkt (mianownik)", options=products_plus)

    if st.button("🧮 Oblicz ranking", type="primary"):
        import numpy as np

        # licznik
        num = pd.Series(0.0, index=users)
        for prod, pts in pairs:
            if not prod: continue
            if prod == GROUP_FLAVORED:
                s = dff.loc[mask_flav].groupby("UserFullName")["Quantity"].sum()
            else:
                s = dff.loc[dff["ProductName"] == prod].groupby("UserFullName")["Quantity"].sum()
            num = num.add(s.reindex(users, fill_value=0).astype(float) * float(pts), fill_value=0.0)

        # mianownik
        if den_mode == "Liczba transakcji":
            if "TransactionId" not in dff.columns:
                st.error("Brak TransactionId → wybierz inny mianownik."); st.stop()
            den = dff.groupby("UserFullName")["TransactionId"].nunique().reindex(users, fill_value=0).astype(float)
        elif den_mode == "Wybrany produkt":
            if not den_prod:
                st.error("Wybierz produkt dla mianownika."); st.stop()
            if den_prod == GROUP_FLAVORED:
                den = dff.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0).astype(float)
            else:
                den = dff.loc[dff["ProductName"] == den_prod].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0).astype(float)
        else:
            den = pd.Series(1.0, index=users)

        score = (num / den.replace(0, np.nan)).astype("float")
        out = pd.DataFrame({
            "Wynik (%)": (score*100),
            "Licznik (pkt)": num,
            "Mianownik": den,
        }).sort_values("Wynik (%)", ascending=False, na_position="last")
        out.insert(0, "Miejsce", range(1, len(out)+1))
        # medale
        def medal(place:int):
            return "🥇" if place==1 else ("🥈" if place==2 else ("🥉" if place==3 else ""))
        out["Miejsce"] = out["Miejsce"].astype(int).map(lambda x: f"{x}. {medal(x)}")

        disp = out.reset_index(names="Zleceniobiorca")[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik"]]
        st.dataframe(disp, use_container_width=True, hide_index=True)

        # eksport
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                disp.to_excel(w, index=False, sheet_name="Ranking")
            st.download_button("⬇️ Pobierz ranking (XLSX)", data=buf.getvalue(),
                               file_name="KreatorKonkursow.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować XLSX: {ex}")

# ---------- Zakładka: Cafe Stats ----------
with tab_cafe:
    st.subheader("☕ Cafe Stats — wszystkie kina (CAF)")
    df = ensure_data_or_stop()
    df = add__date_column(df)

    # Zakres dat
    if "__date" in df.columns and df["__date"].notna().any():
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="cafe_date")
        if isinstance(picked, (list, tuple)) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
    else:
        dff = df.copy()

    required = {"UserFullName", "TransactionId", "NetAmount", "PosName"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn do obliczeń CAF: UserFullName, TransactionId, NetAmount, PosName.")
        st.stop()

    # tylko POS z 'CAF'
    tx_df = _keep_caf(dff)

    if tx_df.empty:
        st.info("Brak danych dla POS 'CAF' w wybranym zakresie dat.")
    else:
        users_sorted = sorted(tx_df["UserFullName"].dropna().unique())
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx_total = f.where(nun <= 1, s)

        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted)
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64").round(2)

        # Średnia (CAF — wszystkie kina)
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

        def _row_style(row):
            if row.name == "Średnia (CAF — wszystkie kina)":
                return ['font-weight:700; background-color:#f3f4f6' for _ in row]
            diff = row.get("Różnica")
            if pd.isna(diff): return ['' for _ in row]
            if diff > 0:  return ['background-color:#dcfce7; font-weight:600' for _ in row]
            if diff < 0:  return ['background-color:#fee2e2; font-weight:600' for _ in row]
            return ['' for _ in row]

        styled = (final_df.style
                  .format({"Średnia wartość transakcji (CAF)": fmt_pln, "Różnica": fmt_pln})
                  .apply(_row_style, axis=1))
        st.dataframe(styled, use_container_width=True)

        # Eksport XLSX
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                final_df.to_excel(w, index=True, sheet_name="CafeStats")
                wb = w.book; ws = w.sheets["CafeStats"]
                fmt_bold = wb.add_format({"bold": True})
                fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
                fmt_int = wb.add_format({"num_format": "0"})
                ws.set_row(0, None, fmt_bold)
                ws.set_column("A:A", 28)
                ws.set_column("B:B", 20, fmt_int)
                ws.set_column("C:C", 28, fmt_pln)
                ws.set_column("D:D", 20, fmt_pln)
            st.download_button("⬇️ Pobierz XLSX (Cafe Stats)", data=buf.getvalue(),
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
        min_d = df["__date"].dropna().min()
        max_d = df["__date"].dropna().max()
        picked = st.date_input("Zakres dat (włącznie)", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="vip_date")
        if isinstance(picked, (list, tuple)) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from, d_to = min_d, max_d
        mask_d = (df["__date"] >= d_from) & (df["__date"] <= d_to)
        dff = df.loc[mask_d].copy()
    else:
        dff = df.copy()

    required = {"UserFullName", "TransactionId", "NetAmount", "PosName"}
    if not required.issubset(dff.columns):
        st.error("Brak wymaganych kolumn do obliczeń VIP: UserFullName, TransactionId, NetAmount, PosName.")
        st.stop()

    # tylko POS z 'VIP'
    tx_df = _keep_vip(dff)

    if tx_df.empty:
        st.info("Brak danych dla POS 'VIP' w wybranym zakresie dat.")
    else:
        users_sorted = sorted(tx_df["UserFullName"].dropna().unique())
        grp = tx_df.groupby(["UserFullName", "TransactionId"])["NetAmount"]
        nun = grp.nunique(dropna=True); s = grp.sum(min_count=1); f = grp.first()
        per_tx_total = f.where(nun <= 1, s)

        revenue_by_user = per_tx_total.groupby("UserFullName").sum(min_count=1).reindex(users_sorted)
        tx_count_by_user = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users_sorted)
        avg_by_user = (revenue_by_user / tx_count_by_user.replace(0, pd.NA)).astype("Float64").round(2)

        # Średnia (VIP — wszystkie kina)
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

        def _row_style(row):
            if row.name == "Średnia (VIP — wszystkie kina)":
                return ['font-weight:700; background-color:#f3f4f6' for _ in row]
            diff = row.get("Różnica")
            if pd.isna(diff): return ['' for _ in row]
            if diff > 0:  return ['background-color:#dcfce7; font-weight:600' for _ in row]
            if diff < 0:  return ['background-color:#fee2e2; font-weight:600' for _ in row]
            return ['' for _ in row]

        styled = (final_df.style
                  .format({"Średnia wartość transakcji (VIP)": fmt_pln, "Różnica": fmt_pln})
                  .apply(_row_style, axis=1))
        st.dataframe(styled, use_container_width=True)

        # Eksport XLSX
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                final_df.to_excel(w, index=True, sheet_name="VIPStats")
                wb = w.book; ws = w.sheets["VIPStats"]
                fmt_bold = wb.add_format({"bold": True})
                fmt_pln = wb.add_format({'num_format': '#,##0.00 "zł"'})
                fmt_int = wb.add_format({"num_format": "0"})
                ws.set_row(0, None, fmt_bold)
                ws.set_column("A:A", 28)
                ws.set_column("B:B", 20, fmt_int)
                ws.set_column("C:C", 28, fmt_pln)
                ws.set_column("D:D", 20, fmt_pln)
            st.download_button("⬇️ Pobierz XLSX (VIP stats)", data=buf.getvalue(),
                               file_name="VIPStats.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować XLSX: {ex}")
