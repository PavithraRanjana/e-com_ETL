import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from airflow.decorators import dag, task

RAW_DIR = Path("/opt/airflow/data/raw")
BASE_URL = "https://www.simplytek.lk/collections/all"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}


@dag(
    dag_id="simplytek_bronze_extract",
    description="Daily extraction of SimplyTek product catalog into raw JSON",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={
        "owner": "pavithra",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["simplytek", "bronze", "extract"],
)
def simplytek_extract_pipeline():

    @task()
    def scrape_catalog(ds: str = None) -> str:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        products = []

        # Start with the first 3 pages while testing
        for page in range(1, 4):
            url = f"{BASE_URL}?page={page}"
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            grid = soup.select_one("#product-grid") or soup
            cards = grid.select("product-card.product-card")

            for card in cards:
                title_el = card.select_one("a.product-card-title")
                title_text = title_el.get_text(" ", strip=True) if title_el else ""
                if not title_text:
                    continue

                compare_el = card.select_one("input.product-card-compare--checkbox")
                current_price_el = card.select_one("span.price ins .amount")
                original_price_el = card.select_one("span.price del .amount")
                badge_el = card.select_one(".badge.onsale")
                rating_el = card.select_one(".star-rating .body-font")
                stock_el = card.select_one(".product-inventory-notice--text")
                swatch_els = card.select(".product-card-swatch .visually-hidden")

                # Prefer canonical /products/... URL from data attribute if present
                href = (
                    compare_el.get("data-product-url")
                    if compare_el and compare_el.get("data-product-url")
                    else title_el.get("href", "")
                )
                product_url = (
                    href if href.startswith("http") else f"https://www.simplytek.lk{href}"
                )

                products.append(
                    {
                        "source": "simplytek",
                        "scraped_date": ds,
                        "scraped_at_utc": datetime.utcnow().isoformat(),
                        "page": page,
                        "product_id": compare_el.get("data-product-id") if compare_el else None,
                        "product_handle": compare_el.get("data-product-handle") if compare_el else None,
                        "title_raw": title_el.get_text(" ", strip=True),
                        "price_current_raw": (
                            current_price_el.get_text(" ", strip=True)
                            if current_price_el
                            else None
                        ),
                        "price_original_raw": (
                            original_price_el.get_text(" ", strip=True)
                            if original_price_el
                            else None
                        ),
                        "discount_badge_raw": (
                            badge_el.get_text(" ", strip=True) if badge_el else None
                        ),
                        "rating_raw": (
                            rating_el.get_text(" ", strip=True) if rating_el else None
                        ),
                        "stock_status_raw": (
                            stock_el.get_text(" ", strip=True) if stock_el else None
                        ),
                        "colors_raw": [
                            s.get_text(" ", strip=True)
                            for s in swatch_els
                            if s.get_text(strip=True)
                        ],
                        "product_url": product_url,
                    }
                )

            time.sleep(1)

        output_path = RAW_DIR / f"simplytek_raw_{ds}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(products, f, indent=2, ensure_ascii=False)

        print(f"Scraped {len(products)} raw product records -> {output_path}")
        return str(output_path)

    @task()
    def validate_raw_output(file_path: str) -> str:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Expected output file missing: {file_path}")

        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)

        if not records:
            raise ValueError("0 products extracted.")

        empty_titles = [r for r in records if not (r.get("title_raw") or "").strip()]
        empty_prices = [r for r in records if not (r.get("price_current_raw") or "").strip()]
        unique_ids = {r.get("product_id") for r in records if r.get("product_id")}

        if empty_titles or empty_prices:
            raise ValueError(
                f"Data quality check failed: {len(empty_titles)} empty titles, "
                f"{len(empty_prices)} empty prices."
            )

        print(
            f"Validation passed: {len(records)} records "
            f"({len(unique_ids)} unique product IDs)."
        )
        return file_path

    raw_file = scrape_catalog()
    validate_raw_output(raw_file)


simplytek_extract_pipeline()