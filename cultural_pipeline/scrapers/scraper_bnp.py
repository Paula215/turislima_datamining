"""
Scraper para BNP (Biblioteca Nacional del Perú) - eventos culturales
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

BASE_URL = "https://eventos.bnp.gob.pe"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_event_links() -> list[str]:
    url = f"{BASE_URL}/externo/inicio#gpInicio"
    response = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "lxml")
    links = []
    for a in soup.select("div.portfolio a"):
        href = a.get("href")
        if href and "/agenda-cultural/" in href:
            links.append(BASE_URL + href)
    return list(set(links))


def scrape_event(url: str) -> dict:
    response = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "lxml")
    data = {"url": url}

    titulo = soup.find("h1") or soup.find("h2")
    data["titulo"] = titulo.text.strip() if titulo else None

    tipo = soup.find("span", style=lambda x: x and "background-color" in x)
    data["tipo"] = tipo.text.strip() if tipo else None

    etiquetas = {}
    for li in soup.select("ul li"):
        spans = li.find_all("span")
        if len(spans) >= 2:
            key = spans[0].text.strip().split("\n")[0]
            value = spans[-1].text.strip()
            etiquetas[key] = value
    data["etiquetas"] = etiquetas

    img = soup.find("img", class_="img-event-detail")
    data["imagen"] = img["src"] if img else None

    desc = soup.find("p", class_="descripcion")
    data["descripcion"] = desc.text.strip() if desc else None

    info_adicional = None
    for p in soup.find_all("p"):
        texto = p.get_text(" ", strip=True)
        if "AM" in texto or "PM" in texto:
            info_adicional = texto
            break
    data["info_adicional"] = info_adicional

    estado = soup.find("strong")
    data["estado"] = estado.text.strip() if estado else None

    return data


def run() -> pd.DataFrame:
    print("🔍 BNP scraper iniciado...")
    links = get_event_links()
    print(f"  {len(links)} eventos encontrados")
    all_data = []
    for i, link in enumerate(links):
        try:
            data = scrape_event(link)
            all_data.append(data)
            time.sleep(1)
        except Exception as e:
            print(f"  ❌ Error en {link}: {e}")

    df = pd.json_normalize(all_data)
    df["_source"] = "bnp"
    df["_scraped_at"] = datetime.utcnow().isoformat()
    print(f"✅ BNP: {len(df)} eventos")
    return df


if __name__ == "__main__":
    df = run()
    df.to_csv("bnp_raw.csv", index=False, encoding="utf-8-sig")
