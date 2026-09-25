from itg_crawler.errors import (
    ForbiddenError,
    NotFoundError,
    PermanentError,
    RateLimitedError,
    ServerError,
    ServiceUnavailableError,
    TransientError,
    UnauthorizedError,
    classify_http_status,
)


def test_classify_retryable_statuses():
    assert isinstance(classify_http_status(429), RateLimitedError)
    assert isinstance(classify_http_status(503), ServiceUnavailableError)
    assert isinstance(classify_http_status(500), ServerError)
    assert isinstance(classify_http_status(500), TransientError)


def test_classify_permanent_statuses():
    assert isinstance(classify_http_status(404), NotFoundError)
    assert isinstance(classify_http_status(403), ForbiddenError)
    assert isinstance(classify_http_status(401), UnauthorizedError)
    assert isinstance(classify_http_status(404), PermanentError)


def test_classify_unmapped_client_error_is_permanent():
    error = classify_http_status(418)

    assert isinstance(error, PermanentError)
    assert not isinstance(error, TransientError)


def test_classify_unmapped_server_error_is_transient():
    error = classify_http_status(502)

    assert isinstance(error, TransientError)


def test_error_carries_url_and_status():
    error = classify_http_status(404, url="https://example.com/missing")

    assert error.url == "https://example.com/missing"
    assert error.status_code == 404
