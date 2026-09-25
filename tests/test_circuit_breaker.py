import time

from itg_crawler.circuit_breaker import CircuitBreaker


def test_allows_requests_below_failure_threshold():
    breaker = CircuitBreaker(failure_threshold=3)

    breaker.record_failure("example.com")
    breaker.record_failure("example.com")

    assert breaker.allow_request("example.com")
    assert breaker.get_state("example.com") == "closed"


def test_opens_after_failure_threshold_is_reached():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=10.0)

    breaker.record_failure("example.com")
    breaker.record_failure("example.com")

    assert not breaker.allow_request("example.com")
    assert breaker.get_state("example.com") == "open"


def test_recovers_after_timeout_elapses():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

    breaker.record_failure("example.com")
    assert not breaker.allow_request("example.com")

    time.sleep(0.06)

    assert breaker.allow_request("example.com")
    assert breaker.get_state("example.com") == "closed"


def test_success_resets_failure_count():
    breaker = CircuitBreaker(failure_threshold=2)

    breaker.record_failure("example.com")
    breaker.record_success("example.com")
    breaker.record_failure("example.com")

    assert breaker.allow_request("example.com")


def test_domains_are_tracked_independently():
    breaker = CircuitBreaker(failure_threshold=1)

    breaker.record_failure("a.com")

    assert not breaker.allow_request("a.com")
    assert breaker.allow_request("b.com")
