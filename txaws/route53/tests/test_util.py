# Licenced under the txaws licence available at /LICENSE in the txaws source.

"""
Tests for L{txaws.route53._util}.
"""

from twisted.trial.unittest import TestCase

from txaws.route53._util import bytes_to_str, to_xml, tags


class MaybeBytesToUnicodeTestCase(TestCase):
    """
    Tests for L{bytes_to_str}.
    """
    def test_bytes(self):
        """
        When called with an instance of L{bytes}, L{bytes_to_str}
        decodes its input using I{ascii} and returns the resulting str
        string as an instance of L{str}.
        """
        self.assertRaises(
            UnicodeDecodeError,
            lambda: bytes_to_str(u"\N{SNOWMAN}".encode("utf-8")),
        )
        decoded = bytes_to_str(b"hello world")
        self.assertIsInstance(decoded, str)
        self.assertEqual(decoded, u"hello world")

    def test_str(self):
        """
        When called with an instance of L{str},
        L{bytes_to_str} returns its input unmodified.
        """
        self.assertEqual(
            u"\N{SNOWMAN}",
            bytes_to_str(u"\N{SNOWMAN}"),
        )


class ToXMLTestCase(TestCase):
    """
    Tests for L{to_xml}.
    """
    def test_none(self):
        """
        When called with L{None}, L{to_xml} returns a L{Deferred} that
        fires with C{b""}.
        """
        self.assertEqual(b"", self.successResultOf(to_xml(None)))


    def test_something(self):
        """
        When called with an instance of L{txaws.route53._util.Tag},
        L{to_xml} returns a L{Defered} giving the result of flattening
        it as an instance of L{bytes} with an xml doctype prepended.
        """
        self.assertEqual(
            """<?xml version="1.0" encoding="UTF-8"?>\n<Foo>bar</Foo>""",
            self.successResultOf(to_xml(tags.Foo(u"bar"))),
        )
