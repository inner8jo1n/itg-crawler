import logging
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> dict:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            logger.warning(
                "Failed to parse HTML for URL: %s", url, exc_info=True
            )
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
            }

        try:
            metadata = self.extract_metadata(soup)
        except Exception:
            logger.warning(
                "Failed to extract metadata for URL: %s", url, exc_info=True
            )
            metadata = {}

        try:
            text = self.extract_text(soup)
        except Exception:
            logger.warning(
                "Failed to extract text for URL: %s", url, exc_info=True
            )
            text = ""

        try:
            links = self.extract_links(soup, url)
        except Exception:
            logger.warning(
                "Failed to extract links for URL: %s", url, exc_info=True
            )
            links = []

        try:
            images = self.extract_images(soup, url)
        except Exception:
            logger.warning(
                "Failed to extract images for URL: %s", url, exc_info=True
            )
            images = []

        return {
            "url": url,
            "title": metadata.get("title", ""),
            "text": text,
            "links": links,
            "metadata": metadata,
            "images": images,
        }

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        same_domain_only: bool = False,
    ) -> list[str]:
        links = soup.find_all("a", href=True)
        base_domain = urlparse(base_url).netloc

        absolute_links = []
        for link in links:
            href = str(link.get("href"))
            absolute_url = urljoin(base_url, href)

            parsed = urlparse(absolute_url)
            is_valid = parsed.scheme in ("http", "https") and parsed.netloc
            if not is_valid:
                continue
            if same_domain_only and parsed.netloc != base_domain:
                continue

            absolute_links.append(absolute_url)

        return absolute_links

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = soup.find_all("img", src=True)

        result = []
        for image in images:
            src = image.get("src")
            alt = image.get("alt", "")

            if src:
                src = urljoin(base_url, str(src))

            result.append({"src": src, "alt": alt})

        return result

    def extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        headings = {}
        for level in ("h1", "h2", "h3"):
            tags = soup.find_all(level)
            headings[level] = [tag.get_text(strip=True) for tag in tags]
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = soup.find_all("table")

        result = []
        for table in tables:
            rows = table.find_all("tr")
            table_data = []
            for row in rows:
                cells = row.find_all(["td", "th"])
                row_data = [cell.get_text(strip=True) for cell in cells]
                table_data.append(row_data)
            result.append(table_data)
        return result

    def extract_lists(self, soup: BeautifulSoup) -> list[list[str]]:
        lists = soup.find_all(["ul", "ol"])

        result = []
        for list_tag in lists:
            items = list_tag.find_all("li")
            item_texts = [item.get_text(strip=True) for item in items]
            result.append(item_texts)
        return result

    def extract_text(
        self, soup: BeautifulSoup, selector: str | None = None
    ) -> str:
        if selector is None:
            return soup.get_text(separator=" ", strip=True)

        element = soup.select_one(selector)
        if element is None:
            return ""

        return element.get_text(separator=" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        title = soup.title.get_text(strip=True) if soup.title else ""
        description_tag = soup.find("meta", attrs={"name": "description"})
        description_content = (
            description_tag.get("content", "") if description_tag else ""
        )
        description = (
            description_content.strip()
            if isinstance(description_content, str)
            else ""
        )
        keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        keywords_content = (
            keywords_tag.get("content", "") if keywords_tag else ""
        )
        keywords = (
            keywords_content.strip()
            if isinstance(keywords_content, str)
            else ""
        )

        return {
            "title": title,
            "description": description,
            "keywords": keywords,
        }
