"""
Scraper para MALI (Museo de Arte de Lima) - actividades culturales
Usa Selenium por el JS rendering
"""
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

BASE_URL = "https://mali.pe/es/activity/"


def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver


def scroll_page(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def get_events(driver) -> list[dict]:
    soup = BeautifulSoup(driver.page_source, "lxml")
    events = []
    for card in soup.select("a.thumblink"):
        url = card.get("href")
        title = card.select_one("h4")
        date_text = card.select_one("p")
        if url:
            events.append({
                "url": url,
                "titulo_lista": title.text.strip() if title else None,
                "fecha_raw": date_text.text.strip() if date_text else None
            })
    return events


def scrape_event(driver, url: str) -> dict:
    driver.get(url)
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "lxml")
    data = {"url": url}

    title = soup.find("h1", class_="entry-title")
    data["titulo"] = title.text.strip() if title else None

    category = soup.find("span", class_="category_item")
    data["tipo"] = category.text.strip() if category else None

    container = soup.select_one("div.card-body")
    info = {}
    if container:
        for li in container.select("ul li"):
            strong = li.find("strong")
            if strong:
                key = strong.text.replace(":", "").strip()
                strong.extract()
                value = li.get_text(" ", strip=True)
                info[key] = value

    data["fecha"] = info.get("Fecha")
    data["hora"] = info.get("Hora")
    data["lugar"] = info.get("Lugar")

    descripcion = []
    if container:
        for p in container.find_all("p"):
            texto = p.get_text(" ", strip=True)
            if texto:
                descripcion.append(texto)
    data["descripcion"] = " ".join(descripcion) if descripcion else None

    img = soup.select_one("div.post-thumbnail img")
    data["imagen"] = img["src"] if img else None

    return data


def run() -> pd.DataFrame:
    if not SELENIUM_AVAILABLE:
        print("⚠️ Selenium no disponible — saltando MALI")
        return pd.DataFrame()

    print("🔍 MALI scraper iniciado...")
    driver = init_driver()
    try:
        driver.get(BASE_URL)
        time.sleep(5)
        scroll_page(driver)
        events = get_events(driver)
        print(f"  {len(events)} actividades encontradas")

        all_data = []
        for i, e in enumerate(events):
            try:
                data = scrape_event(driver, e["url"])
                data["fecha_lista"] = e["fecha_raw"]
                all_data.append(data)
            except Exception as err:
                print(f"  ❌ Error: {err}")

        df = pd.json_normalize(all_data)
        df["_source"] = "mali"
        df["_scraped_at"] = datetime.utcnow().isoformat()
        print(f"✅ MALI: {len(df)} eventos")
        return df
    finally:
        driver.quit()


if __name__ == "__main__":
    df = run()
    df.to_csv("mali_raw.csv", index=False, encoding="utf-8-sig")
