from bs4 import BeautifulSoup

from itg_crawler.parser import HTMLParser


async def test_parse_html_extracts_title_and_links():
    html = """
    <html>
        <head><title>Test Page</title></head>
        <body>
            <a href="/about">About</a>
            <a href="https://other.com/page">External</a>
        </body>
    </html>
    """

    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com")

    assert result["title"] == "Test Page"
    assert "https://example.com/about" in result["links"]
    assert "https://other.com/page" in result["links"]


async def test_parse_html_handles_invalid_html():
    html = "<html><body><p>Unclosed paragraph<div>Broken</html>"

    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com")

    assert result["url"] == "https://example.com"
    assert isinstance(result["title"], str)
    assert isinstance(result["links"], list)


async def test_extract_links_converts_relative_to_absolute():
    html = """
    <html><body>
        <a href="/page1">Page 1</a>
        <a href="page2">Page 2</a>
        <a href="../page3">Page 3</a>
    </body></html>
    """

    soup = BeautifulSoup(html, "lxml")

    parser = HTMLParser()
    links = parser.extract_links(soup, "https://example.com/blog/current")

    assert "https://example.com/page1" in links
    assert "https://example.com/blog/page2" in links
    assert "https://example.com/page3" in links


def test_extract_metadata():
    html = """
    <html>
        <head>
            <title>My Page</title>
            <meta name="description" content="A test page description">
            <meta name="keywords" content="test, html, parser">
        </head>
        <body></body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")

    parser = HTMLParser()
    metadata = parser.extract_metadata(soup)

    assert metadata["title"] == "My Page"
    assert metadata["description"] == "A test page description"
    assert metadata["keywords"] == "test, html, parser"
