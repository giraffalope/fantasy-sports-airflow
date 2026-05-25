"""Small BeautifulSoup helpers for typed HTML scraping."""

from __future__ import annotations

from typing import Any

from bs4 import NavigableString, Tag


def as_str(value: object | None) -> str:
    """Normalize tag attribute values to a plain string."""
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(part) for part in value)
    return str(value)


def require_tag(tag: Tag | NavigableString | None, *, context: str) -> Tag:
    resolved = as_tag(tag)
    if resolved is None:
        raise ValueError(context)
    return resolved


def tag_text(tag: Tag | NavigableString | None, *, default: str = "") -> str:
    resolved = as_tag(tag)
    if resolved is None:
        return default
    return resolved.get_text(strip=True)


def class_list(tag: Tag) -> list[str]:
    raw = tag.get("class")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(part) for part in raw]
    return [str(raw)]


def as_tag(node: Tag | NavigableString | int | None) -> Tag | None:
    if isinstance(node, Tag):
        return node
    return None


def require_tag_from(node: Tag | NavigableString | int | None, *, context: str) -> Tag:
    tag = as_tag(node)
    return require_tag(tag, context=context)


def child_tags(parent: Tag | NavigableString, *args: Any, **kwargs: Any) -> list[Tag]:
    if not isinstance(parent, Tag):
        return []
    return [el for el in parent.find_all(*args, **kwargs) if isinstance(el, Tag)]
