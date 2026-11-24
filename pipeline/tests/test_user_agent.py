from frt.scraping.user_agent import REPO_URL, user_agent


def test_user_agent_includes_contact_from_environment(monkeypatch):
    monkeypatch.setenv("SCRAPER_CONTACT", "bot@example.org")
    assert user_agent() == f"FarRightTrackerBot/1.0 ({REPO_URL}; bot@example.org)"


def test_user_agent_without_contact_only_names_the_repo(monkeypatch):
    monkeypatch.delenv("SCRAPER_CONTACT", raising=False)
    assert user_agent() == f"FarRightTrackerBot/1.0 ({REPO_URL})"
