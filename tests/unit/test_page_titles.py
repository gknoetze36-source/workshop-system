"""Browser tab titles must be plain text (login once leaked a <p> into its <title>)."""
import re

import pytest


@pytest.mark.parametrize("path", ["/login", "/register"])
def test_public_page_titles_are_plain_text_with_icon(path):
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    html = phanta_app.app.test_client().get(path).get_data(as_text=True)
    title = re.search(r"<title>(.*?)</title>", html, re.S).group(1)
    assert "<" not in title and title.strip(), title
    assert 'rel="icon"' in html


def test_login_title():
    import phanta_app
    html = phanta_app.app.test_client().get("/login").get_data(as_text=True)
    assert "<title>Login - PHANTA</title>" in html
