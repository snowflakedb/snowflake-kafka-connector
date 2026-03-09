"""Lightweight matcher objects for use with pytest's ``assert ==``.

These matchers implement ``__eq__`` so they can be dropped into dicts or
lists and compared with ``==``.  When a comparison fails, pytest's assertion
rewriting shows the matcher's ``__repr__`` in the diff, making it clear what
was expected.

Usage::

    from lib.matchers import ANY_INT, RegexMatch

    assert metadata == {
        "offset": 0,
        "CreateTime": ANY_INT,
        "topic": RegexMatch(r"my_topic_\\w+"),
    }
"""

import re


class AnyInstance:
    """Matches any value that is an instance of the given type(s)."""

    def __init__(self, *expected_types):
        self._types = expected_types

    def __eq__(self, other):
        return isinstance(other, self._types)

    def __repr__(self):
        names = ", ".join(t.__name__ for t in self._types)
        return f"<any {names}>"


class RegexMatch:
    """Matches any string that fully matches the given pattern."""

    def __init__(self, pattern):
        self._pattern = pattern

    def __eq__(self, other):
        return isinstance(other, str) and re.fullmatch(self._pattern, other) is not None

    def __repr__(self):
        return f"<regex {self._pattern!r}>"


ANY_INT = AnyInstance(int)
ANY_STR = AnyInstance(str)
