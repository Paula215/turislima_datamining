"""
Scraper para MALI (Museo de Arte de Lima) - actividades culturales
Usa Selenium por el JS rendering
"""
import time
import pandas as pd
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from _bronze import ScrapeResult

SCRAPER_VERSION = "mali/1.0.0"

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
    import os
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # In containerized environments we ship `chromium-driver` from apt and
    # let Selenium Manager (>=4.6) auto-discover it, avoiding the runtime
    # download from chromedriver.storage.googleapis.com.
    if os.getenv("USE_SYSTEM_CHROMEDRIVER", "").strip().lower() in ("1", "true", "yes"):
        return webdriver.Chrome(options=options)
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


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


def run_with_payload() -> ScrapeResult:
    """Ejecuta el scraper MALI y devuelve df + raw_records para Bronze."""
    ingest_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not SELENIUM_AVAILABLE:
        print("⚠️ Selenium no disponible — saltando MALI")
        return ScrapeResult(
            df=pd.DataFrame(),
            raw_records=[],
            metadata={
                "scraper_version": SCRAPER_VERSION,
                "ingest_ts": ingest_ts,
                "notes": "selenium_unavailable",
            },
        )

    print("🔍 MALI scraper iniciado...")
    driver = init_driver()
    errors: list[dict] = []
    all_data: list[dict] = []
    events_found = 0
    try:
        driver.get(BASE_URL)
        time.sleep(5)
        scroll_page(driver)
        events = get_events(driver)
        events_found = len(events)
        print(f"  {events_found} actividades encontradas")

        for e in events:
            try:
                data = scrape_event(driver, e["url"])
                data["fecha_lista"] = e["fecha_raw"]
                all_data.append(data)
            except Exception as err:
                print(f"  ❌ Error: {err}")
                errors.append({"url": e.get("url"), "error": str(err)})

        df = pd.json_normalize(all_data)
        df["_source"] = "mali"
        df["_scraped_at"] = datetime.utcnow().isoformat()
        print(f"✅ MALI: {len(df)} eventos")
    finally:
        driver.quit()

    return ScrapeResult(
        df=df,
        raw_records=all_data,
        metadata={
            "scraper_version": SCRAPER_VERSION,
            "ingest_ts": ingest_ts,
            "url": BASE_URL,
            "events_found": events_found,
            "errors": errors,
        },
    )


def run() -> pd.DataFrame:
    return run_with_payload().df


if __name__ == "__main__":
    df = run()
    df.to_csv("mali_raw.csv", index=False, encoding="utf-8-sig")
