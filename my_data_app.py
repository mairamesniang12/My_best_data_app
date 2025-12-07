# app.py
import streamlit as st
import pandas as pd
import sqlite3
import time
from datetime import datetime
from requests import get
from bs4 import BeautifulSoup as bs
import io
import altair as alt

# ---------------------------
# CONFIG
# ---------------------------
DB_PATH = "scraper_animals.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible)"}

CATEGORIES = {
    "chiens": "https://sn.coinafrique.com/categorie/chiens",
    "moutons": "https://sn.coinafrique.com/categorie/moutons",
    "poules-lapins-et-pigeons": "https://sn.coinafrique.com/categorie/poules-lapins-et-pigeons",
    "autres-animaux": "https://sn.coinafrique.com/categorie/autres-animaux",
}

# ---------------------------
# DATABASE INITIALIZATION
# ---------------------------
def init_db(path=DB_PATH):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    # raw_data stores V1..V4 as strings (uncleaned)
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            source_url TEXT,
            v1 TEXT,
            v2 TEXT,
            v3 TEXT,
            v4 TEXT,
            scraped_at TEXT
        )
    ''')
    # cleaned data (normalized)
    c.execute('''
        CREATE TABLE IF NOT EXISTS cleaned_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            name TEXT,
            price REAL,
            address TEXT,
            image TEXT,
            cleaned_at TEXT
        )
    ''')
    # evaluations form
    c.execute('''
        CREATE TABLE IF NOT EXISTS evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            features TEXT,
            rating INTEGER,
            comments TEXT,
            created_at TEXT
        )
    ''')
    conn.commit()
    return conn, c

conn, c = init_db()

# ---------------------------
# SCRAPING (BeautifulSoup)
# ---------------------------
def fetch_soup(url):
    res = get(url, timeout=12, headers=HEADERS)
    res.raise_for_status()
    return bs(res.content, "html.parser")

def detect_last_page(soup):
    # heuristic: find links with page= and pick max number
    links = soup.find_all("a", href=True)
    max_page = 1
    for a in links:
        href = a["href"]
        if "page=" in href:
            try:
                part = href.split("page=")[-1].split("&")[0]
                num = int("".join(ch for ch in part if ch.isdigit()))
                if num > max_page:
                    max_page = num
            except:
                pass
    return max_page

def parse_container(container):
    # returns v1..v4 as strings or None
    try:
        p_desc = container.find("p", class_="ad__card-description")
        v1 = p_desc.a.text.strip() if p_desc and p_desc.a else (p_desc.text.strip() if p_desc else None)
    except:
        v1 = None
    try:
        p_price = container.find("p", class_="ad__card-price")
        v2 = p_price.text.strip() if p_price else None
    except:
        v2 = None
    try:
        p_loc = container.find("p", class_="ad__card-location")
        v3 = p_loc.text.strip() if p_loc else None
    except:
        v3 = None
    try:
        img = container.find("img")
        v4 = img["src"] if img and "src" in img.attrs else None
    except:
        v4 = None
    return v1, v2, v3, v4

def scrape_category(category_key, max_pages=None, save_raw=True, polite_delay=0.3):
    base = CATEGORIES[category_key]
    rows = []
    # detect pages
    try:
        first_soup = fetch_soup(base)
        last = detect_last_page(first_soup)
    except Exception:
        last = 1
    if max_pages and max_pages > 0:
        last = min(last, max_pages)
    # loop pages
    for page in range(1, last + 1):
        url = f"{base}?page={page}"
        try:
            soup = fetch_soup(url)
        except Exception as e:
            st.warning(f"Erreur chargement {url}: {e}")
            continue
        containers = soup.select("div.col.s6.m4.l3")
        for cont in containers:
            v1, v2, v3, v4 = parse_container(cont)
            rows.append([category_key, url, v1, v2, v3, v4, datetime.utcnow().isoformat()])
            if save_raw:
                # avoid inserting exact duplicates (simple check)
                c.execute('''
                    SELECT 1 FROM raw_data WHERE category=? AND v1=? AND v2=? AND v3=? AND v4=?
                ''', (category_key, v1, v2, v3, v4))
                exists = c.fetchone()
                if not exists:
                    c.execute('''
                        INSERT INTO raw_data (category, source_url, v1, v2, v3, v4, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (category_key, url, v1, v2, v3, v4, datetime.utcnow().isoformat()))
        conn.commit()
        time.sleep(polite_delay)
    df = pd.DataFrame(rows, columns=["category","source_url","v1","v2","v3","v4","scraped_at"])
    return df

def scrape_multiple(categories, max_pages_per_cat=None, save_raw=True):
    dfs = []
    for cat in categories:
        df = scrape_category(cat, max_pages=max_pages_per_cat, save_raw=save_raw)
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame(columns=["category","source_url","v1","v2","v3","v4","scraped_at"])

# ---------------------------
# CLEANING
# ---------------------------
def clean_price(raw_price):
    if raw_price is None:
        return None
    s = str(raw_price)
    for tok in ["CFA","cfa","XOF","\u00a0","\xa0"," "]:
        s = s.replace(tok, "")
    s = s.replace(",", "")
    digits = "".join(ch for ch in s if (ch.isdigit() or ch == "."))
    try:
        return float(digits) if digits else None
    except:
        return None

def clean_address(raw_addr):
    if raw_addr is None:
        return None
    s = str(raw_addr).replace("location_on","").strip()
    s = " ".join(s.split())
    return s if s else None

def clean_raw_dataframe(df_raw):
    # df_raw expected columns: category, v1, v2, v3, v4
    rows = []
    for _, r in df_raw.iterrows():
        cat = r.get("category")
        v1 = r.get("v1")
        v2 = r.get("v2")
        v3 = r.get("v3")
        v4 = r.get("v4")
        name = v1.strip() if isinstance(v1, str) else None
        price = clean_price(v2)
        address = clean_address(v3)
        image = v4
        rows.append([cat, name, price, address, image])
        # avoid inserting duplicates in cleaned_data: check name+price+address
        c.execute('''
            SELECT 1 FROM cleaned_data WHERE category=? AND name=? AND price=? AND address=?
        ''', (cat, name, price, address))
        exists = c.fetchone()
        if not exists:
            c.execute('''
                INSERT INTO cleaned_data (category, name, price, address, image, cleaned_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (cat, name, price, address, image, datetime.utcnow().isoformat()))
    conn.commit()
    df_clean = pd.DataFrame(rows, columns=["category","name","price","address","image"])
    df_clean = df_clean.drop_duplicates(subset=["name","price","address"])
    return df_clean

# ---------------------------
# UPLOAD WebScraper export (save raw)
# ---------------------------
def save_uploaded_to_raw(df_uploaded):
    saved = 0
    for _, row in df_uploaded.iterrows():
        # best-effort mapping
        v1 = row.get("name") or row.get("Name") or row.get("title") or row.get("details")
        v2 = row.get("price") or row.get("price_raw") or row.get("prix")
        v3 = row.get("address") or row.get("location") or row.get("address_raw")
        v4 = row.get("image") or row.get("image_src") or row.get("image_link")
        # check duplicate
        c.execute('SELECT 1 FROM raw_data WHERE v1=? AND v2=? AND v3=? AND v4=?', (str(v1), str(v2), str(v3), str(v4)))
        if not c.fetchone():
            c.execute('''
                INSERT INTO raw_data (category, source_url, v1, v2, v3, v4, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', ("uploaded", "uploaded_file", str(v1), str(v2), str(v3), str(v4), datetime.utcnow().isoformat()))
            saved += 1
    conn.commit()
    return saved

# ---------------------------
# STREAMLIT UI
# ---------------------------
st.set_page_config(page_title="Coopérative: Data Science — Scraper", layout="wide")
st.title("Coopérative: Data Science — Scraper CoinAfrique (animaux)")

menu = st.sidebar.selectbox("Navigation", [
    "Scrape pages (clean+raw)",
    "Scrape RAW (Web Scraper style)",
    "Upload Web Scraper export (uncleaned)",
    "Dashboard (cleaned)",
    "Evaluation form",
    "DB viewer"
])

# ---- Scrape pages (clean+raw) ----
if menu == "Scrape pages (clean+raw)":
    st.header("Scraper multi-pages — BeautifulSoup")
    st.write("Sélectionne les catégories à scraper. Le contenu brut sera stocké dans `raw_data`. Tu peux ensuite nettoyer et insérer dans `cleaned_data`.")
    cats = st.multiselect("Choisir catégories", list(CATEGORIES.keys()), default=list(CATEGORIES.keys()))
    max_pages = st.number_input("Max pages par catégorie (0 = auto detect)", min_value=0, value=0)
    if st.button("Lancer scraping (BeautifulSoup)"):
        mp = None if max_pages == 0 else int(max_pages)
        with st.spinner("Scraping en cours..."):
            df_raw = scrape_multiple(cats, max_pages_per_cat=mp, save_raw=True)
        st.success(f"Scrap terminé — {len(df_raw)} items (raw).")
        st.dataframe(df_raw.head(200))
        if st.button("Nettoyer et stocker dans cleaned_data"):
            with st.spinner("Nettoyage..."):
                df_clean = clean_raw_dataframe(df_raw)
            st.success(f"Nettoyé et inséré {len(df_clean)} lignes (cleaned_data).")
            st.dataframe(df_clean.head(200))
            st.download_button("Télécharger cleaned CSV", data=df_clean.to_csv(index=False).encode("utf-8"), file_name="cleaned_data.csv", mime="text/csv")

# ---- Scrape RAW (Web Scraper style) ----
elif menu == "Scrape RAW (Web Scraper style)":
    st.header("Scrape RAW — enregistre V1..V4 tels quels dans raw_data")
    cats = st.multiselect("Choisir catégories (raw)", list(CATEGORIES.keys()), default=list(CATEGORIES.keys()))
    max_pages = st.number_input("Max pages par catégorie (0 = auto detect)", min_value=0, value=0, key="raw_max")
    if st.button("Lancer scraping RAW"):
        mp = None if max_pages == 0 else int(max_pages)
        with st.spinner("Scraping RAW en cours..."):
            df_raw = scrape_multiple(cats, max_pages_per_cat=mp, save_raw=True)
        st.success(f"Scrap RAW terminé — {len(df_raw)} lignes (insérées dans raw_data).")
        st.dataframe(df_raw.head(200))
        st.download_button("Télécharger raw CSV", data=df_raw.to_csv(index=False).encode("utf-8"), file_name="raw_data.csv", mime="text/csv")

# ---- Upload Web Scraper export ----
elif menu == "Upload Web Scraper export (uncleaned)":
    st.header("Importer un export Web Scraper (CSV ou JSON) — sauvegarde dans raw_data (uncleaned)")
    uploaded = st.file_uploader("Choisir un fichier CSV ou JSON", type=["csv","json"])
    if uploaded is not None:
        try:
            if uploaded.name.endswith(".json"):
                df_ws = pd.read_json(uploaded)
            else:
                df_ws = pd.read_csv(uploaded)
            st.subheader("Aperçu du fichier importé")
            st.dataframe(df_ws.head(200))
            if st.button("Sauvegarder l'import dans raw_data"):
                saved = save_uploaded_to_raw(df_ws)
                st.success(f"{saved} lignes sauvegardées dans raw_data.")
        except Exception as e:
            st.error(f"Impossible de lire le fichier: {e}")

# ---- Dashboard (cleaned) ----
elif menu == "Dashboard (cleaned)":
    st.header("Dashboard — Données nettoyées")
    df_clean = pd.read_sql_query("SELECT * FROM cleaned_data", conn)
    if df_clean.empty:
        st.info("Aucune donnée nettoyée trouvée. Nettoie des données après scraping (menu Scrape pages).")
    else:
        st.subheader("Aperçu (cleaned)")
        st.dataframe(df_clean.head(300))
        st.write("---")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total items (cleaned)", len(df_clean))
        col2.metric("Median price", f"{int(df_clean['price'].median()) if not df_clean['price'].isnull().all() else 'N/A'}")
        col3.metric("Unique addresses", df_clean['address'].nunique())

        # Price distribution
        if df_clean['price'].notnull().sum() > 0:
            chart = alt.Chart(df_clean).mark_bar().encode(
                alt.X("price:Q", bin=alt.Bin(maxbins=40), title="Price"),
                y="count()"
            ).properties(height=300, title="Distribution des prix (cleaned)")
            st.altair_chart(chart, use_container_width=True)

        # Top addresses
        top_loc = df_clean['address'].value_counts().reset_index().rename(columns={'index':'address','address':'count'}).head(10)
        if not top_loc.empty:
            chart2 = alt.Chart(top_loc).mark_bar().encode(
                x="count:Q",
                y=alt.Y("address:N", sort='-x')
            ).properties(height=300, title="Top adresses")
            st.altair_chart(chart2, use_container_width=True)

        # Per-category counts
        cat_counts = df_clean['category'].value_counts().reset_index().rename(columns={'index':'category','category':'count'})
        if not cat_counts.empty:
            chart3 = alt.Chart(cat_counts).mark_bar().encode(
                x="category:N",
                y="count:Q"
            ).properties(height=250, title="Nombre d'annonces par catégorie")
            st.altair_chart(chart3, use_container_width=True)

        # Download cleaned CSV
        st.download_button("Télécharger cleaned_data.csv", data=df_clean.to_csv(index=False).encode("utf-8"), file_name="cleaned_data.csv", mime="text/csv")

# ---- Evaluation form ----
elif menu == "Evaluation form":
    st.header("Formulaire d'évaluation de l'application")
    with st.form("eval_form"):
        features = st.multiselect("Quelles fonctionnalités avez-vous testées ?", ['Scraping', 'Upload', 'Dashboard', 'Download', 'Evaluation form'])
        likert1 = st.slider("Les fonctionnalités répondent à mes besoins (1-5)", 1, 5, 4)
        likert2 = st.slider("L'application est facile à utiliser (1-5)", 1, 5, 4)
        comments = st.text_area("Commentaires / suggestions")
        submitted = st.form_submit_button("Envoyer l'évaluation")
    if submitted:
        # store combined rating (average) or store likerts separately; here we store as overall mean
        rating = int(round((likert1 + likert2) / 2))
        c.execute('INSERT INTO evaluations (features, rating, comments, created_at) VALUES (?, ?, ?, ?)',
                  (", ".join(features), rating, comments, datetime.utcnow().isoformat()))
        conn.commit()
        st.success("Merci — votre évaluation a été enregistrée !")

# ---- DB viewer ----
elif menu == "DB viewer":
    st.header("Visualiseur de la base SQLite")
    table = st.selectbox("Choisir une table à charger", ["raw_data", "cleaned_data", "evaluations"])
    if st.button("Charger"):
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        st.dataframe(df)
        st.download_button("Télécharger (CSV)", data=df.to_csv(index=False).encode("utf-8"), file_name=f"{table}.csv", mime="text/csv")

st.caption("Built for Coopérative: Data Science — Respecte les règles du site scrappé (robots.txt) et scrape de façon responsable.")
