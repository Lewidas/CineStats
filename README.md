# CineStats (Program Sprzedaż) — Streamlit Cloud

## Deploy
1. Umieść w repo: `app.py`, `requirements.txt`, `runtime.txt`.
2. Streamlit Community Cloud → **New app** → wybierz repo/branch i `app.py`.
3. (Opcjonalnie) w **Settings → Secrets** dodaj:
   ```
   PASSWORD="TwojeSuperHaslo"
   ```
4. Po starcie: zakładka **Dane** → **Wgrywanie plików** → wgraj raporty.

## Uwaga
- W chmurze brak LibreOffice — używamy `openpyxl` i naprawy `.xlsx`.
- **.xls** nieobsługiwane w tej wersji online — zapisz do `.xlsx` lub `.csv`.
