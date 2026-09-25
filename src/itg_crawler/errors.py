class CrawlerError(Exception):
    def __init__(
        self,
        message: str,
        url: str | None = None,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.retry_after = retry_after


class TransientError(CrawlerError):
    pass


class PermanentError(CrawlerError):
    pass


class NetworkError(CrawlerError):
    pass


class ParseError(CrawlerError):
    pass


class RateLimitedError(TransientError):
    pass


class ServiceUnavailableError(TransientError):
    pass


class ServerError(TransientError):
    pass


class NotFoundError(PermanentError):
    pass


class ForbiddenError(PermanentError):
    pass


class UnauthorizedError(PermanentError):
    pass


_STATUS_ERRORS: dict[int, type[CrawlerError]] = {
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    429: RateLimitedError,
    503: ServiceUnavailableError,
}


def classify_http_status(
    status_code: int, url: str | None = None
) -> CrawlerError:
    error_cls = _STATUS_ERRORS.get(status_code)
    if error_cls is not None:
        return error_cls(
            f"HTTP {status_code} for {url}",
            url=url,
            status_code=status_code,
        )

    if 500 <= status_code < 600:
        return ServerError(
            f"HTTP {status_code} for {url}",
            url=url,
            status_code=status_code,
        )

    if 400 <= status_code < 500:
        return PermanentError(
            f"HTTP {status_code} for {url}",
            url=url,
            status_code=status_code,
        )

    return TransientError(
        f"HTTP {status_code} for {url}",
        url=url,
        status_code=status_code,
    )
