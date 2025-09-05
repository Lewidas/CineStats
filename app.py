import os, io, re, json, tempfile
from pathlib import Path
import pandas as pd
import streamlit as st
import boto3

# =================== USTAWIENIA STRONY ===================
st.set_page_config(page_title="CineStats — sprzedaż i wskaźniki", layout="wide")
st.title("🎬 CineStats — sprzedaż i wskaźniki")

# ---------- Prosta ochrona hasłem (opcjonalna) ----------
if "PASSWORD" in st.secrets:
    if "AUTHED" not in st.session_state:
        pw = st.text_input("Hasło", type="password")
        if st.button("Zaloguj"):
            if pw == st.secrets["PASSWORD"]:
                st.session_state["AUTHED"] = True
                st.rerun()
            else:
                st.error("Nieprawidłowe hasło.")
        st.stop()

# ---------- Pomocnicze ----------
def _read_csv_any(path: Path) -> pd.DataFrame:
    try:    return pd.read_csv(path, sep=None, engine="python")
    except: return pd.read_csv(path)

def read_any_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".csv",".txt"):  return _read_csv_any(path)
    if ext == ".xlsx":          return pd.read_excel(path, engine="openpyxl")
    raise ValueError(f"Nieobsługiwane rozszerzenie: {ext}")

def _load_from_paths(paths: list[Path]) -> pd.DataFrame:
    frames, failures = [], []
    for p in paths:
        try:
            df = read_any_table(p)
            df["__source_file"] = p.name
            frames.append(df)
        except Exception as e:
            failures.append((p.name, str(e)))
    if failures:
        with st.expander("❗ Pliki z błędami"):
            for n, err in failures: st.error(f"{n} → {err}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _norm_key(x):
    s = "" if x is None else str(x)
    s = s.lower()
    # uproszczona normalizacja (bez znaków diakrytycznych)
    return "".join(ch for ch in s if ch.isalnum())

# ---------- Integracja S3/R2 ----------
def _s3_connect():
    bucket   = st.secrets.get("S3_BUCKET", "cine-stats")
    region   = st.secrets.get("S3_REGION", "eu-central-1")
    endpoint = st.secrets.get("S3_ENDPOINT_URL") or None
    ak = st.secrets.get("AWS_ACCESS_KEY_ID")
    sk = st.secrets.get("AWS_SECRET_ACCESS_KEY")
    if not ak or not sk:
        raise RuntimeError("Brak AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY w Secrets.")
    cli = boto3.client("s3", region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk, endpoint_url=endpoint)
    base_prefix = st.secrets.get("S3_PREFIX", "Raporty/")
    return cli, bucket, base_prefix

def s3_list_datasets(prefix_extra=""):
    s3, bucket, base = _s3_connect()
    pref = (base + (prefix_extra or "")).lstrip("/")
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": pref}
        if token: kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for it in resp.get("Contents", []):
            k = it["Key"].lower()
            if k.endswith((".xlsx",".csv",".txt")): keys.append(it["Key"])
        if resp.get("IsTruncated"): token = resp.get("NextContinuationToken")
        else: break
    return sorted(keys)

def s3_read_df(key: str) -> pd.DataFrame:
    s3, bucket, _ = _s3_connect()
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    ext = os.path.splitext(key)[1].lower()
    if ext in (".csv",".txt"):
        try:    return pd.read_csv(io.BytesIO(data), sep=None, engine="python")
        except: return pd.read_csv(io.BytesIO(data))
    if ext == ".xlsx":
        tmp = Path(tempfile.mkdtemp()) / os.path.basename(key)
        tmp.write_bytes(data)
        return pd.read_excel(tmp, engine="openpyxl")
    raise ValueError(f"Nieobsługiwane rozszerzenie: {ext}")

def load_from_s3(prefix_extra=""):
    frames, fails = [], []
    for k in s3_list_datasets(prefix_extra):
        try:
            df = s3_read_df(k)
            df["__source_file"] = k.split("/")[-1]
            frames.append(df)
        except Exception as e:
            fails.append((k, str(e)))
    if fails:
        with st.expander("❗ Pliki z błędami w S3"):
            for k, err in fails: st.error(f"{k} → {err}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# =============== TABS (podstrony) ===============
tab_dane, tab_pivot, tab_indy, tab_best, tab_comp = st.tabs(
    ["🗂️ Dane", "📈 Tabela wskaźników", "👤 Wyniki indywidualne", "🏆 Najlepsi", "🧮 Kreator Konkursów"]
)

# ---------- Zakładka: Dane ----------
with tab_dane:
    st.subheader("🗂️ Ustawienia źródła danych")
    mode = st.radio("Źródło danych", ["Chmura (S3/R2)", "Wgrywanie plików"], horizontal=True)

    if mode == "Chmura (S3/R2)":
        pref = st.text_input("Prefiks w buckecie (opcjonalnie)", value=st.secrets.get("S3_PREFIX", "Raporty/"))
        if st.button("🔄 Wczytaj z S3/R2", type="primary"):
            try:
                with st.spinner("Łączenie i pobieranie..."):
                    st.session_state["df"] = load_from_s3(prefix_extra=pref)
                st.success("Dane zaczytane z S3.")
            except Exception as ex:
                st.error(f"Nie udało się połączyć z S3/R2: {ex}")
        st.info("Raporty trzymaj w buckecie pod zadanym prefiksem (np. Raporty/2025-09/…).")

    else:
        uploaded = st.file_uploader("Wrzuć pliki (.xlsx/.csv/.txt)", type=["xlsx","csv","txt"], accept_multiple_files=True)
        if st.button("🔄 Wczytaj/odśwież", type="primary"):
            if not uploaded:
                st.warning("Dodaj pliki.")
            else:
                tmpdir = Path(tempfile.mkdtemp(prefix="uploads_"))
                paths = []
                for uf in uploaded:
                    p = tmpdir / uf.name
                    p.write_bytes(uf.read())
                    paths.append(p)
                with st.spinner("Wczytywanie..."):
                    st.session_state["df"] = _load_from_paths(paths)

    df = st.session_state.get("df", pd.DataFrame())
    if df.empty:
        st.info("Brak danych w pamięci. Wybierz tryb i wczytaj pliki.")
    else:
        st.success(f"Wczytano {len(df):,} wierszy.".replace(",", " "))
        st.dataframe(df.head(300), use_container_width=True)

def ensure_data_or_stop() -> pd.DataFrame:
    df = st.session_state.get("df", pd.DataFrame())
    if df.empty:
        st.warning("Brak danych. Najpierw wczytaj pliki w zakładce **Dane**.")
        st.stop()
    return df

# ---------- Wspólne definicje ----------
FLAVORED_NORM = { _norm_key(x) for x in ["BEKON-SER","BEKON-SER/SOL","CHEDDAR/SOL","KARMEL.","KARMEL/BEKON.","KARMEL/CHEDDAR.","KARMEL/SOL.","SER-CHEDDAR"] }
BASE_POP_NORM  = { _norm_key(x) for x in ["KubekPopcorn1,5l","KubekPopcorn2,3l","KubekPopcorn4,2l","KubekPopcorn5,2l","KubekPopcorn6,5l"] }
SHARE_NUM_NORM = { _norm_key("KubekPopcorn6,5l") }
SHARE_DEN_NORM = BASE_POP_NORM - SHARE_NUM_NORM

# ---------- Zakładka: Tabela wskaźników ----------
with tab_pivot:
    st.subheader("📈 Tabela wskaźników")
    df = ensure_data_or_stop()
    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(df.columns):
        st.error("Brak wymaganych kolumn: UserFullName, ProductName, Quantity.")
        st.stop()

    df = df.copy()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in df.columns:
        df["NetAmount"] = pd.to_numeric(df["NetAmount"], errors="coerce")
    df["__pnorm"] = df["ProductName"].map(_norm_key)

    users = sorted(df["UserFullName"].dropna().unique())

    # % Extra Sos (licznik: Extra Nachos Sauce; mianownik: Tacka Nachos Średnia/Duża)
    mask_extra = df["__pnorm"].eq("extranachossauce")
    mask_base  = df["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})
    extra_by_u = df.loc[mask_extra].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    base_by_u  = df.loc[mask_base ].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_extra  = (extra_by_u / base_by_u.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % Popcorny smakowe
    mask_flav = df["__pnorm"].isin(FLAVORED_NORM)
    mask_basep= df["__pnorm"].isin(BASE_POP_NORM)
    flav_by_u = df.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    basep_by_u= df.loc[mask_basep].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_pop   = (flav_by_u / basep_by_u.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # % ShareCorn
    share_num = df["__pnorm"].isin(SHARE_NUM_NORM)
    share_den = df["__pnorm"].isin(SHARE_DEN_NORM)
    num_by_u  = df.loc[share_num].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    den_by_u  = df.loc[share_den].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct_share = (num_by_u / den_by_u.replace(0, pd.NA) * 100).astype("Float64").round(1)

    # Transakcje + średnia wartość transakcji (z wykluczeniem POS Bonarka CAF1/VIP1)
    tx_df = df.copy()
    if "PosName" in tx_df.columns:
        m_ex = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~m_ex].copy()

    if "TransactionId" in tx_df.columns:
        tx_count = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users, fill_value=0).astype("Int64")
    else:
        tx_count = pd.Series([pd.NA]*len(users), index=users, dtype="Int64")

    if {"TransactionId","NetAmount"}.issubset(tx_df.columns):
        grp = tx_df.groupby(["UserFullName","TransactionId"])["NetAmount"]
        nun, s, f = grp.nunique(dropna=True), grp.sum(min_count=1), grp.first()
        per_tx   = f.where(nun <= 1, s)
        revenue  = per_tx.groupby("UserFullName").sum(min_count=1).reindex(users).astype("Float64")
        avg_val  = (revenue / tx_count.astype("Float64").replace(0, pd.NA)).astype("Float64").round(2)
    else:
        avg_val = pd.Series([pd.NA]*len(users), index=users, dtype="Float64")

    table = pd.DataFrame(index=users)
    table["Liczba transakcji"] = tx_count
    table["Średnia wartość transakcji"] = avg_val
    table["% Extra Sos"] = pct_extra
    table["% Popcorny smakowe"] = pct_pop
    table["% ShareCorn"] = pct_share
    table = table.sort_values(by="Średnia wartość transakcji", ascending=False, na_position="last")

    st.dataframe(table, use_container_width=True)

    # Eksport XLSX
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            table.to_excel(w, sheet_name="Wskaźniki")
        st.download_button("⬇️ Pobierz XLSX (Wskaźniki)", data=buf.getvalue(),
                           file_name="Wskazniki.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as ex:
        st.warning(f"Nie udało się przygotować XLSX: {ex}")

# ---------- Zakładka: Wyniki indywidualne ----------
with tab_indy:
    st.subheader("👤 Wyniki indywidualne")
    df = ensure_data_or_stop().copy()
    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(df.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in df.columns:
        df["NetAmount"] = pd.to_numeric(df["NetAmount"], errors="coerce")
    df["__pnorm"] = df["ProductName"].map(_norm_key)

    users_all = sorted(df["UserFullName"].dropna().unique())
    user = st.selectbox("Zleceniobiorca", options=users_all, index=0 if users_all else None)

    # maski jak wyżej
    mask_extra = df["__pnorm"].eq("extranachossauce")
    mask_base  = df["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})
    mask_flav  = df["__pnorm"].isin(FLAVORED_NORM)
    mask_basep = df["__pnorm"].isin(BASE_POP_NORM)
    mask_sn    = df["__pnorm"].isin(SHARE_NUM_NORM)
    mask_sd    = df["__pnorm"].isin(SHARE_DEN_NORM)

    # kino
    def _pct(num_mask, den_mask, frame):
        num = float(frame.loc[num_mask, "Quantity"].sum())
        den = float(frame.loc[den_mask, "Quantity"].sum())
        return None if den == 0 else round(num/den*100, 1)

    df_tx = df.copy()
    if "PosName" in df_tx.columns:
        ex = df_tx["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        df_tx = df_tx.loc[~ex].copy()

    if {"TransactionId","NetAmount"}.issubset(df_tx.columns):
        g = df_tx.groupby("TransactionId")["NetAmount"]
        per_tx = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        avg_cinema = None if df_tx["TransactionId"].nunique()==0 else round(float(per_tx.sum(min_count=1))/int(df_tx["TransactionId"].nunique()), 2)
    else:
        avg_cinema = None

    row_cinema = {
        "Średnia wartość transakcji": avg_cinema,
        "% Extra Sos": _pct(mask_extra, mask_base, df),
        "% Popcorny smakowe": _pct(mask_flav, mask_basep, df),
        "% ShareCorn": _pct(mask_sn, mask_sd, df),
    }

    # osoba
    d_u = df[df["UserFullName"] == user]
    dfu_tx = d_u.copy()
    if "PosName" in dfu_tx.columns:
        exu = dfu_tx["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        dfu_tx = dfu_tx.loc[~exu].copy()

    if {"TransactionId","NetAmount"}.issubset(dfu_tx.columns):
        g = dfu_tx.groupby("TransactionId")["NetAmount"]
        per_tx = g.first().where(g.nunique(dropna=True) <= 1, g.sum(min_count=1))
        txc = int(dfu_tx["TransactionId"].nunique())
        avg_user = None if txc==0 else round(float(per_tx.sum(min_count=1))/txc, 2)
    else:
        avg_user, txc = None, None

    row_user = {
        "Średnia wartość transakcji": avg_user,
        "% Extra Sos": _pct(d_u["__pnorm"].eq("extranachossauce"), d_u["__pnorm"].isin({"tackanachossrednia","tackanachosduza"}), d_u),
        "% Popcorny smakowe": _pct(d_u["__pnorm"].isin(FLAVORED_NORM), d_u["__pnorm"].isin(BASE_POP_NORM), d_u),
        "% ShareCorn": _pct(d_u["__pnorm"].isin(SHARE_NUM_NORM), d_u["__pnorm"].isin(SHARE_DEN_NORM), d_u),
    }

    disp = pd.DataFrame({
        "Wskaźnik": ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"],
        user: [row_user[k] for k in ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"]],
        "Średnia kina": [row_cinema[k] for k in ["Średnia wartość transakcji","% Extra Sos","% Popcorny smakowe","% ShareCorn"]],
    })
    st.metric("Liczba transakcji (osoba)", "-" if txc is None else f"{txc:,}".replace(",", " "))
    st.dataframe(disp, use_container_width=True, hide_index=True)

# ---------- Zakładka: Najlepsi ----------
with tab_best:
    st.subheader("🏆 Najlepsi — ranking")
    df = ensure_data_or_stop().copy()
    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(df.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    if "NetAmount" in df.columns:
        df["NetAmount"] = pd.to_numeric(df["NetAmount"], errors="coerce")
    df["__pnorm"] = df["ProductName"].map(_norm_key)
    users = sorted(df["UserFullName"].dropna().unique())

    # % Extra Sos
    mask_extra = df["__pnorm"].eq("extranachossauce")
    mask_base  = df["__pnorm"].isin({"tackanachossrednia","tackanachosduza"})
    extra = df.loc[mask_extra].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    base  = df.loc[mask_base ].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct   = (extra / base.replace(0, pd.NA) * 100).astype("Float64")
    avg_c = None if float(base.sum())==0 else round(float(extra.sum())/float(base.sum())*100,1)
    st.markdown("#### % Extra Sos" + ("" if avg_c is None else f"  |  Średnia kina: **{avg_c:.1f} %**"))
    st.dataframe(pd.DataFrame({"Wartość": pct}).sort_values("Wartość", ascending=False, na_position="last"),
                 use_container_width=True)

    # % Popcorny smakowe
    mask_flav = df["__pnorm"].isin(FLAVORED_NORM)
    mask_basep= df["__pnorm"].isin(BASE_POP_NORM)
    flav = df.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    basp = df.loc[mask_basep].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct2 = (flav / basp.replace(0, pd.NA) * 100).astype("Float64")
    avg2 = None if float(basp.sum())==0 else round(float(flav.sum())/float(basp.sum())*100,1)
    st.markdown("#### % Popcorny smakowe" + ("" if avg2 is None else f"  |  Średnia kina: **{avg2:.1f} %**"))
    st.dataframe(pd.DataFrame({"Wartość": pct2}).sort_values("Wartość", ascending=False, na_position="last"),
                 use_container_width=True)

    # % ShareCorn
    sn = df["__pnorm"].isin(SHARE_NUM_NORM)
    sd = df["__pnorm"].isin(SHARE_DEN_NORM)
    num = df.loc[sn].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    den = df.loc[sd].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0)
    pct3= (num / den.replace(0, pd.NA) * 100).astype("Float64")
    avg3= None if float(den.sum())==0 else round(float(num.sum())/float(den.sum())*100,1)
    st.markdown("#### % ShareCorn" + ("" if avg3 is None else f"  |  Średnia kina: **{avg3:.1f} %**"))
    st.dataframe(pd.DataFrame({"Wartość": pct3}).sort_values("Wartość", ascending=False, na_position="last"),
                 use_container_width=True)

    # Średnia wartość transakcji
    tx_df = df.copy()
    if "PosName" in tx_df.columns:
        mask_excl = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
        tx_df = tx_df.loc[~mask_excl].copy()
    st.markdown("#### Średnia wartość transakcji")
    if {"TransactionId","NetAmount"}.issubset(tx_df.columns):
        grp = tx_df.groupby(["UserFullName","TransactionId"])["NetAmount"]
        per_tx = grp.first().where(grp.nunique(dropna=True) <= 1, grp.sum(min_count=1))
        rev_by_user = per_tx.groupby("UserFullName").sum(min_count=1).reindex(users)
        tx_by_user  = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users)
        avg_by_user = (rev_by_user / tx_by_user.replace(0, pd.NA)).astype("Float64")
        st.dataframe(pd.DataFrame({"Wartość": avg_by_user}).sort_values("Wartość", ascending=False, na_position="last"),
                     use_container_width=True)
    else:
        st.info("Brak TransactionId/NetAmount — nie policzę średniej transakcji.")

# ---------- Zakładka: Kreator Konkursów ----------
with tab_comp:
    st.subheader("🧮 Kreator Konkursów")
    df = ensure_data_or_stop().copy()
    need = {"UserFullName","ProductName","Quantity"}
    if not need.issubset(df.columns):
        st.error("Wymagane kolumny: UserFullName, ProductName, Quantity"); st.stop()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)

    products_all = sorted(df["ProductName"].dropna().unique())
    users = sorted(df["UserFullName"].dropna().unique())

    # grupa "Popcorny Smakowe"
    df["__pnorm"] = df["ProductName"].map(_norm_key)
    mask_flav = df["__pnorm"].isin(FLAVORED_NORM)
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
            prod = st.selectbox(f"Produkt #{i+1}", options=products_plus,
                                index=(products_plus.index(p0) if p0 in products_plus else 0),
                                key=f"contest_prod_{i}")
        with c2:
            pts = st.number_input(f"Punkty #{i+1}", value=float(pts0), step=0.5, key=f"contest_pts_{i}")
        pairs.append((prod, float(pts)))

    st.divider()
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
                s = df.loc[mask_flav].groupby("UserFullName")["Quantity"].sum()
            else:
                s = df.loc[df["ProductName"] == prod].groupby("UserFullName")["Quantity"].sum()
            num = num.add(s.reindex(users, fill_value=0).astype(float) * float(pts), fill_value=0.0)
        # mianownik
        if den_mode == "Liczba transakcji":
            if "TransactionId" not in df.columns:
                st.error("Brak TransactionId → wybierz inny mianownik."); st.stop()
            tx_df = df.copy()
            if "PosName" in tx_df.columns:
                ex = tx_df["PosName"].astype(str).str.contains("Bonarka CAF1|Bonarka VIP1", case=False, regex=True, na=False)
                tx_df = tx_df.loc[~ex].copy()
            den = tx_df.groupby("UserFullName")["TransactionId"].nunique().reindex(users, fill_value=0).astype(float)
        elif den_mode == "Wybrany produkt":
            if not den_prod: st.error("Wybierz produkt dla mianownika."); st.stop()
            if den_prod == GROUP_FLAVORED:
                den = df.loc[mask_flav].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0).astype(float)
            else:
                den = df.loc[df["ProductName"] == den_prod].groupby("UserFullName")["Quantity"].sum().reindex(users, fill_value=0).astype(float)
        else:
            den = pd.Series(1.0, index=users)

        score = (num / den.replace(0, np.nan)).astype("float")
        out = pd.DataFrame({
            "Wynik": score,
            "Wynik (%)": (score*100),
            "Licznik (pkt)": num,
            "Mianownik": den,
        }).sort_values("Wynik", ascending=False, na_position="last")
        out.insert(0, "Miejsce", range(1, len(out)+1))

        disp = out.reset_index(names="Zleceniobiorca")[["Miejsce","Zleceniobiorca","Wynik (%)","Licznik (pkt)","Mianownik"]]
        st.dataframe(disp, use_container_width=True, hide_index=True)

        # eksport
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                disp.to_excel(w, index=False, sheet_name="Ranking")
            st.download_button("⬇️ Pobierz ranking (XLSX)", data=buf.getvalue(),
                               file_name="Konkurs.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as ex:
            st.warning(f"Nie udało się przygotować XLSX: {ex}")

