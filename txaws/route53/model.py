# Licenced under the txaws licence available at /LICENSE in the txaws source.

"""
Simple model objects related to Route53 interactions.
"""

__all__ = [
    "Name", "SOA", "NS", "A", "CNAME",
    "HostedZone",
]

from ipaddress import IPv4Address, IPv6Address

from zope.interface import implementer, provider

import attr
from attr import validators

from constantly import Names, NamedConstant

from txaws.route53._util import bytes_to_str
from txaws.route53.interface import IResourceRecordLoader, IBasicResourceRecord, IRRSetChange
from txaws.client._validators import set_of

@attr.s(frozen=True)
class Name:
    text = attr.ib(
        convert=lambda v: v + "." if not v.endswith(".") else v,
        validator=validators.instance_of(str),
    )

    def __str__(self):
        return self.text.encode("idna")


@attr.s(frozen=True)
class RRSetKey:
    label = attr.ib()
    type = attr.ib()



class RRSetType(Names):
    RESOURCE = NamedConstant()
    ALIAS = NamedConstant()



@attr.s(frozen=True)
class RRSet:
    """
    https://tools.ietf.org/html/rfc2181#section-5
    http://docs.aws.amazon.com/Route53/latest/APIReference/API_ResourceRecord.html

    @ivar name: The label (name) of the resource record set involved in the change.
    @type name: L{Name}

    @ivar type: The type of the resource record set. For example, NS, SOA, AAAA, etc.
    @type type: L{str}

    @ivar ttl: The time-to-live for this resource record set.
    @type ttl: L{int}

    @ivar records: The resource records involved in the change.
    @type records: L{list} of L{IResourceRecord} providers
    """
    type = RRSetType.RESOURCE

    label = attr.ib(validator=validators.instance_of(Name))
    type = attr.ib(validator=validators.instance_of(str))
    ttl = attr.ib(validator=validators.instance_of(int))
    records = attr.ib(validator=set_of(validators.provides(IBasicResourceRecord)))



@attr.s(frozen=True)
class AliasRRSet:
    """
    http://docs.aws.amazon.com/Route53/latest/APIReference/API_AliasTarget.html

    @ivar dns_name: The target of the alias (of variable meaning; see AWS
        docs).
    @type dns_name: L{Name}

    @ivar evaluate_target_health: Inherit health of the target.
    @type evaluate_target_health: L{bool}

    @ivar hosted_zone_id: Scope information for interpreting C{dns_name} (of
        variable meaning; see AWS docs).
    @type hosted_zone_id: L{str}
    """
    type = type = RRSetType.ALIAS

    label = attr.ib(validator=validators.instance_of(Name))
    type = attr.ib(validator=validators.instance_of(str))
    dns_name = attr.ib(validator=validators.instance_of(Name))
    evaluate_target_health = attr.ib(validator=validators.instance_of(bool))
    hosted_zone_id = attr.ib(validator=validators.instance_of(str))



@implementer(IRRSetChange)
@attr.s(frozen=True)
class _ChangeRRSet:
    action = attr.ib()
    rrset = attr.ib(validator=validators.instance_of(RRSet))



def create_rrset(rrset):
    return _ChangeRRSet("CREATE", rrset)


def delete_rrset(rrset):
    return _ChangeRRSet("DELETE", rrset)


def upsert_rrset(rrset):
    return _ChangeRRSet("UPSERT", rrset)


@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class NS:
    nameserver = attr.ib(validator=validators.instance_of(Name))

    @classmethod
    def basic_from_element(cls, e):
        return cls(Name(bytes_to_str(e.find("Value").text)))


    def to_text(self):
        return str(self.nameserver)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class A:
    address = attr.ib(validator=validators.instance_of(IPv4Address))

    @classmethod
    def basic_from_element(cls, e):
        return cls(IPv4Address(bytes_to_str(e.find("Value").text)))


    def to_text(self):
        return str(self.address)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class AAAA:
    address = attr.ib(validator=validators.instance_of(IPv6Address))

    @classmethod
    def basic_from_element(cls, e):
        return cls(IPv6Address(bytes_to_str(e.find("Value").text)))


    def to_text(self):
        return str(self.address)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class MX:
    name = attr.ib(validator=validators.instance_of(Name))
    preference = attr.ib(validator=validators.instance_of(int))

    @classmethod
    def basic_from_element(cls, e):
        parts = bytes_to_str(e.find("Value").text).split()
        preference = int(parts[0])
        name = parts[1]
        return cls(Name(name), preference)


    def to_text(self):
        return "{} {}".format(self.preference, self.name)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class CNAME:
    canonical_name = attr.ib(validator=validators.instance_of(Name))

    @classmethod
    def basic_from_element(cls, e):
        return cls(Name(bytes_to_str(e.find("Value").text)))

    def to_text(self):
        return str(self.canonical_name)



def _split_quoted(text):
    """
    Split a str string on *SPACE* characters.

    Splitting is not done at *SPACE* characters occurring within matched
    *QUOTATION MARK*s.  *REVERSE SOLIDUS* can be used to remove all
    interpretation from the following character.

    :param str text: The string to split.

    :return: A two-tuple of str giving the two split pieces.
    """
    quoted = False
    escaped = False
    result = []
    for i, ch in enumerate(text):
        if escaped:
            escaped = False
            result.append(ch)
        elif ch == '\\':
            escaped = True
        elif ch == '"':
            quoted = not quoted
        elif not quoted and ch == ' ':
            return "".join(result), text[i:].lstrip()
        else:
            result.append(ch)
    return "".join(result), ""



def _quote(text):
    """
    Quote the given string so ``_split_quoted`` will not split it up.

    :param str text: The string to quote:

    :return: A str string representing ``text`` as protected from
        splitting.
    """
    return (
        '"' +
        text.replace("\\", "\\\\").replace('"', '\\"') +
        '"'
    )


@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class NAPTR:
    """
    Represent a Name Authority Pointer record.

    See AWS API documentation for this type for restrictions on field values.
    This object does not enforce these restrictions beyond simple type
    constraints but attempting to send out-of-bounds values to the AWS Route53
    API may provoke an error.
    """
    order = attr.ib(validator=validators.instance_of(int))
    preference = attr.ib(validator=validators.instance_of(int))
    flag = attr.ib(validator=validators.instance_of(str))
    service = attr.ib(validator=validators.instance_of(str))
    regexp = attr.ib(validator=validators.instance_of(str))
    replacement = attr.ib(validator=validators.instance_of(Name))

    @classmethod
    def basic_from_element(cls, e):
        value = bytes_to_str(e.find("Value").text)
        order, preference, rest = value.split(None, 2)
        flag, rest = _split_quoted(rest)
        service, rest = _split_quoted(rest)
        regexp, replacement = _split_quoted(rest)
        return cls(
            int(order),
            int(preference),
            flag, service, regexp, Name(replacement),
        )


    def to_text(self):
        replacement = self.replacement
        if replacement == Name("."):
            replacement = "."

        return "{} {} {} {} {} {}".format(
            self.order, self.preference,
            _quote(self.flag),
            _quote(self.service),
            _quote(self.regexp),
            replacement,
        )



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class PTR:
    name = attr.ib(validator=validators.instance_of(Name))

    @classmethod
    def basic_from_element(cls, e):
        return cls(Name(bytes_to_str(e.find("Value").text)))


    def to_text(self):
        return str(self.name)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class SPF:
    value = attr.ib(validator=validators.instance_of(str))

    @classmethod
    def basic_from_element(cls, e):
        return cls(
            _split_quoted(
                bytes_to_str(
                    e.find("Value").text
                )
            )[0]
        )


    def to_text(self):
        return _quote(self.value)



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class SRV:
    priority = attr.ib(validator=validators.instance_of(int))
    weight = attr.ib(validator=validators.instance_of(int))
    port = attr.ib(validator=validators.instance_of(int))
    name = attr.ib(validator=validators.instance_of(Name))


    @classmethod
    def basic_from_element(cls, e):
        priority, weight, port, name = bytes_to_str(
            e.find("Value").text
        ).split()
        return cls(int(priority), int(weight), int(port), Name(name))


    def to_text(self):
        return "{} {} {} {}".format(
            self.priority, self.weight, self.port, self.name,
        )



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class TXT:
    texts = attr.ib(
        convert=tuple,
        validator=validators.instance_of(tuple),
    )

    @classmethod
    def basic_from_element(cls, e):
        pieces = []
        value = bytes_to_str(e.find("Value").text)
        while value:
            piece, value = _split_quoted(value)
            pieces.append(piece)
        return cls(pieces)


    def to_text(self):
        return " ".join(
            _quote(value)
            for value
            in self.texts
        )



@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class SOA:
    mname = attr.ib(validator=validators.instance_of(Name))
    rname = attr.ib(validator=validators.instance_of(Name))
    serial = attr.ib(validator=validators.instance_of(int))
    refresh = attr.ib(validator=validators.instance_of(int))
    retry = attr.ib(validator=validators.instance_of(int))
    expire = attr.ib(validator=validators.instance_of(int))
    minimum = attr.ib(validator=validators.instance_of(int))

    @classmethod
    def basic_from_element(cls, e):
        text = bytes_to_str(e.find("Value").text)
        parts = dict(zip(_SOA_FIELDS, text.split()))
        return cls(
            Name(parts["mname"]),
            Name(parts["rname"]),
            int(parts["serial"]),
            int(parts["refresh"]),
            int(parts["retry"]),
            int(parts["expire"]),
            int(parts["minimum"]),
        )

    def to_text(self):
        return "{mname} {rname} {serial} {refresh} {retry} {expire} {minimum}".format(
            **attr.asdict(self, recurse=False)
        )

_SOA_FIELDS = list(field.name for field in attr.fields(SOA))


@provider(IResourceRecordLoader)
@implementer(IBasicResourceRecord)
@attr.s(frozen=True)
class UnknownRecordType:
    value = attr.ib(validator=validators.instance_of(str))

    @classmethod
    def basic_from_element(cls, e):
        return cls(bytes_to_str(e.find("Value").text))


    def to_text(self):
        return str(self.value)



@attr.s
class HostedZone:
    """
    http://docs.aws.amazon.com/Route53/latest/APIReference/API_HostedZone.html
    """
    name = attr.ib(validator=validators.instance_of(str))
    identifier = attr.ib(validator=validators.instance_of(str))
    rrset_count = attr.ib(validator=validators.instance_of(int))
    reference = attr.ib(validator=validators.instance_of(str))
