# error_handler.py

from lxml import html
from pymongo import errors

def has_zero_results(html_src: str) -> bool:
    """
    Detect the “0 results total” banner:
    <h4 class="text-dark-a">0 results total</h4>
    """
    tree = html.fromstring(html_src)
    # finds any <h4 class="text-dark-a"> that contains “0” and “results total”
    return bool(
        tree.xpath(
            '//h4[contains(@class,"text-dark-a") and contains(.,"0") and contains(.,"results total")]'
        )
    )

def is_404_page(html_src: str) -> bool:
    """
    Detect the “Page not found (404)” error block.
    """
    return "Page not found (404)" in html_src

def handle_html_errors(
    html_src: str,
    page_url: str,
    entry: dict,
    all_entries: list,
    save_entries_fn,
    collection,
) -> bool:
    """
    Check for zero-results or 404.  
    If an error is found:
      • mark entry["value"]=True  
      • call save_entries_fn(all_entries) to persist urls.json  
      • for 404 only: insert the page_url into MongoDB  
    Returns True if we should skip further scraping of this entry.
    """
    # 1) No job posts
    if has_zero_results(html_src):
        print(f"⚠️ Zero results at {page_url}. Marking this base-URL done.")
        entry["value"] = True
        save_entries_fn(all_entries)
        return True

    # 2) 404 Page
    if is_404_page(html_src):
        print(f"⚠️ 404 at {page_url}. Saving page_url and marking done.")
        try:
            collection.update_one(
                {"url": page_url},
                {"$setOnInsert": {"url": page_url, "processed": False}},
                upsert=True,
            )
        except errors.PyMongoError as e:
            print("   ⚠️ Mongo error saving 404 URL:", e)
        entry["value"] = True
        save_entries_fn(all_entries)
        return True

    # no error
    return False
