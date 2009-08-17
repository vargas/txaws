# Copyright (c) 2009 Canonical Ltd <duncan.mcgreggor@canonical.com>
# Licenced under the txaws licence available at /LICENSE in the txaws source.

try:
    from xml.etree import ElementTree
except ImportError:
    from elementtree import ElementTree

from twisted.trial.unittest import TestCase

from txaws.ec2.exception import EC2Error


REQUEST_ID = "a9e514a7-bee4-4e56-9dad-0e86d8175aa4"


ERROR_EXAMPLE_SINGLE = """
<?xml version="1.0"?>
<Response>
    <Errors>
        <Error>
            <Code>Error.Code</Code>
            <Message>Message for Error.Code</Message>
        </Error>
    </Errors>
    <RequestID>%s</RequestID>
</Response>
""" % REQUEST_ID


ERROR_EXAMPLE_MULTIPLE = """
<?xml version="1.0"?>
<Response>
    <Errors>
        <Error>
            <Code>Error.Code1</Code>
            <Message>Message for Error.Code1</Message>
        </Error>
        <Error>
            <Code>Error.Code2</Code>
            <Message>Message for Error.Code2</Message>
        </Error>
    </Errors>
    <RequestID>%s</RequestID>
</Response>
""" % REQUEST_ID


class EC2ErrorTestCase(TestCase):

    def test_node_to_dict(self):
        xml = "<parent><child1>text1</child1><child2>text2</child2></parent>"
        error = EC2Error("<dummy />")
        data = error._node_to_dict(ElementTree.fromstring(xml))
        self.assertEquals(data, {"child1": "text1", "child2": "text2"})

    def test_set_request_id(self):
        xml = "<a><b /><RequestID>%s</RequestID></a>" % REQUEST_ID
        error = EC2Error("<dummy />")
        error._set_request_id(ElementTree.fromstring(xml))
        self.assertEquals(error.requestID, REQUEST_ID)

    def test_set_errors(self):
        errorsXML = "<Error><Code>1</Code><Message>2</Message></Error>"
        xml = "<a><Errors>%s</Errors><b /></a>" % errorsXML
        error = EC2Error("<dummy />")
        error._set_errors(ElementTree.fromstring(xml))
        self.assertEquals(error.errors[0]["Code"], "1")
        self.assertEquals(error.errors[0]["Message"], "2")

    def test_set_empty_errors(self):
        xml = "<a><Errors /><b /></a>"
        error = EC2Error("<dummy />")
        error._set_errors(ElementTree.fromstring(xml))
        self.assertEquals(error.errors, [])

    def test_set_empty_error(self):
        xml = "<a><Errors><Error /><Error /></Errors><b /></a>"
        error = EC2Error("<dummy />")
        error._set_errors(ElementTree.fromstring(xml))
        self.assertEquals(error.errors, [])

    def test_parse_without_xml(self):
        xml = "<dummy />"
        error = EC2Error(xml)
        error.parse()
        self.assertEquals(error.original, xml)

    def test_parse_with_xml(self):
        xml1 = "<dummy1 />"
        xml2 = "<dummy2 />"
        error = EC2Error(xml2)
        error.parse(xml2)
        self.assertEquals(error.original, xml2)

    def test_has_error(self):
        errorsXML = "<Error><Code>Code1</Code><Message>2</Message></Error>"
        xml = "<a><Errors>%s</Errors><b /></a>" % errorsXML
        error = EC2Error(xml)
        self.assertTrue(error.has_error("Code1"))

    def test_single_error(self):
        error = EC2Error(ERROR_EXAMPLE_SINGLE)
        self.assertEquals(len(error.errors), 1)

    def test_multiple_errors(self):
        error = EC2Error(ERROR_EXAMPLE_MULTIPLE)
        self.assertEquals(len(error.errors), 2)

    def test_empty_xml(self):
        self.assertRaises(ValueError, EC2Error, "")

    def test_no_request_id(self):
        errors = "<Errors><Error><Code /><Message /></Error></Errors>"
        xml = "<Response>%s<RequestID /></Response>" % errors
        error = EC2Error(xml)
        self.assertEquals(error.requestID, "")

    def test_no_request_id_node(self):
        errors = "<Errors><Error><Code /><Message /></Error></Errors>"
        xml = "<Response>%s</Response>" % errors
        error = EC2Error(xml)
        self.assertEquals(error.requestID, "")

    def test_no_errors_node(self):
        xml = "<Response><RequestID /></Response>"
        error = EC2Error(xml)
        self.assertEquals(error.errors, [])

    def test_no_error_node(self):
        xml = "<Response><Errors /><RequestID /></Response>"
        error = EC2Error(xml)
        self.assertEquals(error.errors, [])

    def test_no_error_code_node(self):
        errors = "<Errors><Error><Message /></Error></Errors>"
        xml = "<Response>%s<RequestID /></Response>" % errors
        error = EC2Error(xml)
        self.assertEquals(error.errors, [])

    def test_no_error_message_node(self):
        errors = "<Errors><Error><Code /></Error></Errors>"
        xml = "<Response>%s<RequestID /></Response>" % errors
        error = EC2Error(xml)
        self.assertEquals(error.errors, [])

    def test_single_get_error_codes(self):
        error = EC2Error(ERROR_EXAMPLE_SINGLE)
        self.assertEquals(error.get_error_codes(), "Error.Code")

    def test_multiple_get_error_codes(self):
        error = EC2Error(ERROR_EXAMPLE_MULTIPLE)
        self.assertEquals(error.get_error_codes(), 2)

    def test_zero_get_error_codes(self):
        xml = "<Response><RequestID /></Response>"
        error = EC2Error(xml)
        self.assertEquals(error.get_error_codes(), None)

    def test_single_get_error_messages(self):
        error = EC2Error(ERROR_EXAMPLE_SINGLE)
        self.assertEquals(error.get_error_messages(), "Message for Error.Code")

    def test_multiple_get_error_messages(self):
        error = EC2Error(ERROR_EXAMPLE_MULTIPLE)
        self.assertEquals(error.get_error_messages(), "Multiple EC2 Errors")

    def test_zero_get_error_messages(self):
        xml = "<Response><RequestID /></Response>"
        error = EC2Error(xml)
        self.assertEquals(error.get_error_messages(), "Empty error list")

    def test_single_error_str(self):
        error = EC2Error(ERROR_EXAMPLE_SINGLE)
        self.assertEquals(str(error), "Error Message: Message for Error.Code")

    def test_multiple_errors_str(self):
        error = EC2Error(ERROR_EXAMPLE_MULTIPLE)
        self.assertEquals(str(error), "Multiple EC2 Errors.")

    def test_single_error_repr(self):
        error = EC2Error(ERROR_EXAMPLE_SINGLE)
        self.assertEquals(
            repr(error),
            "<EC2Error object with Error code: Error.Code>")

    def test_multiple_errors_repr(self):
        error = EC2Error(ERROR_EXAMPLE_MULTIPLE)
        self.assertEquals(repr(error), "<EC2Error object with Error count: 2>")
