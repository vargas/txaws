try:
    from xml.etree import ElementTree
except ImportError:
    from elementtree import ElementTree

from txaws.exception import AWSError


class EC2Error(AWSError):
    """
    A error class providing custom methods on EC2 errors.
    """
    def __init__(self, xml):
        if not xml:
            raise ValueError("XML cannot be empty.")
        self.original = xml
        self.errors = []
        self.requestID = ""
        self.parse()

    def __str__(self):
        return self._get_error_message_string()

    def __repr__(self):
        return "<%s object with %s>" % (
            self.__class__.__name__, self._get_error_code_string())

    def _set_request_id(self, tree):
        requestIDNode = tree.find(".//RequestID")
        if hasattr(requestIDNode, "text"):
            text = requestIDNode.text
            if text:
                self.requestID = text

    def _set_errors(self, tree):
        errorsNode = tree.find(".//Errors")
        if errorsNode:
            for error in errorsNode:
                data = self._node_to_dict(error)
                if data:
                    self.errors.append(data)

    def _get_error_code_string(self):
        count = len(self.errors)
        error_code = self.get_error_codes()
        if count > 1:
            return "Error count: %s" % error_code
        else:
            return "Error code: %s" % error_code

    def _get_error_message_string(self):
        count = len(self.errors)
        error_message = self.get_error_messages()
        if count > 1:
            return "%s." % error_message
        else:
            return "Error Message: %s" % error_message

    def _node_to_dict(self, node):
        data = {}
        for child in node:
            if child.tag and child.text:
                data[child.tag] = child.text
        return data

    def parse(self, xml=""):
        if not xml:
            xml = self.original
        tree = ElementTree.fromstring(xml.strip())
        self._set_request_id(tree)
        self._set_errors(tree)

    def has_error(self, errorString):
        for error in self.errors:
            if errorString in error.values():
                return True
        return False

    def get_error_codes(self):
        count = len(self.errors)
        if count > 1:
            return count
        elif count == 0:
            return
        else:
            return self.errors[0]["Code"]

    def get_error_messages(self):
        count = len(self.errors)
        if count > 1:
            return "Multiple EC2 Errors"
        elif count == 0:
            return "Empty error list"
        else:
            return self.errors[0]["Message"]
