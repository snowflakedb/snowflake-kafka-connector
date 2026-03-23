import pytest


@pytest.fixture(params=["v4", "v3"])
def connector_version(request):
    """The Snowflake Kafka Connector version under test.

    Every test that (transitively) depends on this fixture is automatically run twice:
    once for v4 and once for v3.
    """
    return request.param


@pytest.fixture
def name_salt(session_name_salt, connector_version):  # noqa: F811
    """Diversify names between test runs and connector versions."""
    if connector_version == "v3":
        return f"{session_name_salt}_V3"
    return session_name_salt
