import hmac
import base64
import warnings
import mimetypes
from hashlib import sha256
from urllib.parse import quote, quote_plus
from datetime import datetime

from twisted.web.http import datetimeToString

from txaws import _auth_v4
from txaws.client.base import BaseClient, BaseQuery
from txaws.service import AWSServiceEndpoint, REGION_US_EAST_1, SQS_ENDPOINT
from txaws.sqs.connection import SQSConnection
from txaws.sqs.errors import RequestParamError
from txaws.sqs.parser import (empty_check,
                              parse_send_message_batch,
                              parse_change_message_visibility_batch,
                              parse_delete_message_batch,
                              parse_receive_message,
                              parse_get_queue_url,
                              parse_list_queues,
                              parse_create_queue,
                              parse_queue_attributes)

class Query(BaseQuery):
    """A query for submission to the S3 service."""

    def __init__(self, bucket=None, object_name=None, data="",
                 content_type=None, metadata={}, amz_headers={},
                 body_producer=None, *args, **kwargs):
        super(Query, self).__init__(*args, **kwargs)

        # data might be None or "", alas.
        if data and body_producer is not None:
            raise ValueError("data and body_producer are mutually exclusive.")

        self.bucket = bucket
        self.object_name = object_name
        self.data = data
        self.body_producer = body_producer
        self.content_type = content_type
        self.metadata = metadata
        self.amz_headers = amz_headers
        self._date = datetimeToString()
        if not self.endpoint or not self.endpoint.host:
            self.endpoint = AWSServiceEndpoint(S3_ENDPOINT)
        self.endpoint.set_method(self.action)

    @property
    def date(self):
        """
        Return the date and emit a deprecation warning.
        """
        warnings.warn("txaws.s3.client.Query.date is a deprecated attribute",
                      DeprecationWarning,
                      stacklevel=2)
        return self._date

    @date.setter
    def date(self, value):
        """
        Set the date.

        @param value: The new date for this L{Query}.
        @type value: L{str}
        """
        self._date = value

    def set_content_type(self):
        """
        Set the content type based on the file extension used in the object
        name.
        """
        if self.object_name and not self.content_type:
            # XXX nothing is currently done with the encoding... we may
            # need to in the future
            self.content_type, encoding = mimetypes.guess_type(
                self.object_name, strict=False)

    def get_headers(self, instant):
        """
        Build the list of headers needed in order to perform S3 operations.
        """
        headers = {'x-amz-date': _auth_v4.makeAMZDate(instant)}
        if self.body_producer is None:
            data = self.data
            if data is None:
                data = b""
            headers["x-amz-content-sha256"] = sha256(data).hexdigest()
        else:
            data = None
            headers["x-amz-content-sha256"] = b"UNSIGNED-PAYLOAD"
        for key, value in self.metadata.items():
            headers["x-amz-meta-" + key] = value
        for key, value in self.amz_headers.items():
            headers["x-amz-" + key] = value

        # Before we check if the content type is set, let's see if we can set
        # it by guessing the the mimetype.
        self.set_content_type()
        if self.content_type is not None:
            headers["Content-Type"] = self.content_type
        if self.creds is not None:
            headers["Authorization"] = self.sign(
                headers,
                data,
                sqs_url_context(self.endpoint),
                instant,
                method=self.action)
        return headers

    def sign(self, headers, data, url_context, instant, method,
             region=REGION_US_EAST_1):
        """Sign this query using its built in credentials."""
        headers["host"] = url_context.get_encoded_host()

        if data is None:
            request = _auth_v4._CanonicalRequest.from_request_components(
                method=method,
                url=url_context.get_encoded_path(),
                headers=headers,
                headers_to_sign=('host', 'x-amz-date'),
                payload_hash=None,
            )
        else:
            request = _auth_v4._CanonicalRequest.from_request_components_and_payload(
                method=method,
                url=url_context.get_encoded_path(),
                headers=headers,
                headers_to_sign=('host', 'x-amz-date'),
                payload=data,
            )

        return _auth_v4._make_authorization_header(
            region=region,
            service="sqs",
            canonical_request=request,
            credentials=self.creds,
            instant=instant)

    def submit(self, url_context=None, utcnow=datetime.datetime.utcnow):
        """Submit this query.

        @return: A deferred from get_page
        """
        if not url_context:
            url_context = sqs_url_context(
                self.endpoint, self.bucket, self.object_name)
        d = self.get_page(
            url_context.get_encoded_url(),
            method=self.action,
            postdata=self.data or b"",
            headers=self.get_headers(utcnow()),
        )

        return d.addErrback(s3_error_wrapper)

def sqs_url_context(service_endpoint):
    """
    Create a URL based on the given service endpoint and suitable for
    the given bucket or object.

    @param service_endpoint: The service endpoint on which to base the
        resulting URL.
    @type service_endpoint: L{AWSServiceEndpoint}

    @param bucket: If given, the name of a bucket to reference.
    @type bucket: L{str}

    @param object_name: If given, the name of an object or object
        subresource to reference.
    @type object_name: L{str}
    """

    # Define our own query parser which can handle the consequences of
    # `?acl` and such (subresources).  At its best, parse_qsl doesn't
    # let us differentiate between these and empty values (such as
    # `?acl=`).
    def p(s):
        results = []
        args = s.split("&")
        for a in args:
            pieces = a.split("=")
            if len(pieces) == 1:
                results.append((unquote(pieces[0]),))
            elif len(pieces) == 2:
                results.append(tuple(map(unquote, pieces)))
            else:
                raise Exception("oh no")
        return results

    query = []
    path = []
    if bucket is None:
        path.append("")
    else:
        if isinstance(bucket, bytes):
            bucket = bucket.decode("utf-8")
        path.append(bucket)
        if object_name is None:
            path.append("")
        else:
            if isinstance(object_name, bytes):
                object_name = object_name.decode("utf-8")
            if "?" in object_name:
                object_name, query = object_name.split("?", 1)
                query = p(query)
            object_name_components = object_name.split("/")
            if object_name_components[0] == "":
                object_name_components.pop(0)
            if object_name_components:
                path.extend(object_name_components)
            else:
                path.append("")
    return _SQSURLContext(
        scheme=service_endpoint.scheme.decode("utf-8"),
        host=service_endpoint.get_host().decode("utf-8"),
        port=service_endpoint.port,
        path=path,
        query=query,
    )


class _SQSURLContext(_URLContext):
    # Backwards compatibility layer.  For deprecation.  s3_url_context
    # should just return an _URLContext and application code should
    # interact with that interface.
    def get_host(self):
        return self.get_encoded_host()

    def get_path(self):
        return self.get_encoded_path()

    def get_url(self):
        return self.get_encoded_url()

class QuerysSignatureV4(BaseQuery):

    version = '2012-11-05'

    def __init__(self, creds, endpoint, agent=None):
        super(QuerysSignatureV4, self).__init__(endpoint.get_host(), agent)
        self.creds = creds
        self.endpoint = endpoint
        self.region = endpoint.get_host().split('.')[1]

    def _get_amz(self, dt):
        return '/'.join([
            self.creds.access_key,
            dt.strftime('%Y%m%d'),
            self.region,
            'sqs',
            'aws4_request',
        ])

    def _canonical_headers(self, headers):
        l = sorted(['%s:%s' % (name.lower().strip(),
                    ' '.join(value.strip().split()))
                    for name, value in headers])
        return '\n'.join(l) + '\n'

    def _hashed_canonical_request(self, q_str, params, canonical_headers):
        d = [
           self.endpoint.method.upper(),
           quote(self.endpoint.path),
           q_str,
           self._canonical_headers(canonical_headers),
           params['X-Amz-SignedHeaders'],
           sha256('').hexdigest(), # GET has no body
        ]
        return sha256('\n'.join(d)).hexdigest()

    def _signature(self, params, hsh, dt):
        str_to_sign = '%s\n%s\n%s\n%s' % (
            params['X-Amz-Algorithm'], params['X-Amz-Date'],
            '/'.join(params['X-Amz-Credential'].split('/')[1:]), hsh
        )
        HMAC = lambda x, y: hmac.new(x, y.encode('utf-8'), sha256).digest()
        d = HMAC(HMAC(HMAC(HMAC("AWS4" + self.creds.secret_key,
                                dt.strftime('%Y%m%d')),
                           self.region),
                      "sqs"),
                "aws4_request")
        signature = hmac.new(d, str_to_sign.encode('utf-8'), sha256).hexdigest()
        return signature

    def _generate_request_url(self, action, query_params, dt, canonical_headers):
        query_params.extend([
            ('Action', action),
            ('Version', self.version),
            ('X-Amz-Algorithm', 'AWS4-HMAC-SHA256'),
            ('X-Amz-Credential', self._get_amz(dt)),
            ('X-Amz-Date', dt.strftime('%Y%m%dT%H%M%SZ')),
            ('X-Amz-SignedHeaders', 'host;x-amz-date'),
        ])
        query_params.sort(key=lambda x: x[0])
        params = dict(query_params)
        query_string = quote(query_params)
        hsh = self._hashed_canonical_request(query_string,
                                             params,
                                             canonical_headers)
        query_string += '&' + quote([('X-Amz-Signature',
                                               self._signature(params,
                                                               hsh,
                                                               dt))])
        return '%s?%s' % (self.endpoint.get_uri(), query_string)

    def submit(self, action, **params):
        dt = datetime.utcnow()
        canonical_headers = [('host', self.endpoint.host),
                             ('X-Amz-Date', dt.strftime('%Y%m%dT%H%M%SZ'))]
        url = self._generate_request_url(action,
                                         params.items(),
                                         dt,
                                         canonical_headers)
        return self.get_page(
            url, method='GET', body_producer=self.body_producer,
            receiver_factory=self.receiver_factory,
            headers=dict(canonical_headers),
        )


class QuerySignatureV2(BaseQuery):

    version = '2012-11-05'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, creds, endpoint, agent=None):
        super(QuerySignatureV2, self).__init__(endpoint.get_host(), agent)
        self.creds = creds
        self.endpoint = endpoint

    def _calculate_signature(self, query_params_list):
        query_string = quote(query_params_list)
        string_to_sign = '%s\n%s\n%s\n%s' % (
            self.endpoint.method, self.endpoint.host,
            self.endpoint.path, query_string
        )
        return hmac_sha256(self.creds.secret_key, string_to_sign)

    def _generate_request_url(self, action, query_params):
        query_params.extend([
            ('Action', action),
            ('AWSAccessKeyId', self.creds.access_key),
            ('Version', self.version),
            ('SignatureVersion', '2'),
            ('SignatureMethod', 'HmacSHA256'),
            ('Timestamp', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')),
        ])
        query_params.sort()
        query_params.append(('Signature', self._calculate_signature(query_params)))
        query_string = quote(query_params)
        return '%s?%s' % (self.endpoint.get_uri(), query_string)

    def submit(self, action, **params):
        url = self._generate_request_url(action, params.items())
        return self.get_page(
            url, method='GET', body_producer=self.body_producer,
            receiver_factory=self.receiver_factory
        )


class SQSClient(BaseClient):
    """
        API functions for working with queues in general (not a specific queue):
            - CreateQueue;
            - GetQueueUrl;
            - ListQueues.
    """

    def __init__(self, creds=None, endpoint=None, query_factory=None):
        query_factory = QuerysSignatureV4(creds, endpoint)
        super(SQSClient, self).__init__(creds, endpoint, query_factory)

    def get_queue(self, owner_id, queue):
        """
            @param owner_id: required, C{str}.
            @param queue: required, C{str}:
            If owner_id and queue name is known, there is no need to do
            request for queue url. You should call this method to get queue
            and make operations on it.
        """
        endpoint = AWSServiceEndpoint(uri=self.endpoint.get_uri())
        endpoint.set_path('/{}/{}/'.format(owner_id, queue))
        query_factory = QuerysSignatureV4(self.creds, endpoint,
                                          self.query_factory.agent)
        return Queue(self.creds, endpoint, query_factory)

    def create_queue(self, name, attrs=None):
        """
            @param name: required, C{str}.
            @param attrs: optional, C{dict}:
                {'DelaySeconds': C{int} from 0 to 900, default - 0,
                 'MaximumMessageSize': C{int} from 1024 bytes (1 KiB)
                                       up to 65536 bytes (64 KiB),
                                       default - 65536,
                 'MessageRetentionPeriod': C{int} (seconds) from
                                        60 (1 minute) to 1209600 (14 days),
                                        default - 345600 (4 days),
                 'Policy': valid form-url-encoded policy,
                 'ReceiveMessageWaitTimeSeconds': C{int} from 0 to 20
                                                  (seconds), default - 0,
                 'VisibilityTimeout': C{int} from 0 to 43200 (12 hours),
                                      default - 30,
                }
            DelaySeconds - The time in seconds that the delivery of all messages
                           in the queue will be delayed.
            MaximumMessageSize - The limit of how many bytes a message can
                                 contain before Amazon SQS rejects it.
            MessageRetentionPeriod - The number of seconds Amazon SQS retains
                                     a message.
            Policy - The formal description of the permissions for a resource.
            ReceiveMessageWaitTimeSeconds - Long poll support.
            VisibilityTimeout - The length of time, in seconds, that a message
                                received from a queue will be invisible to other
                                receiving components when they ask to receive
                                messages.
        """
        params = {'QueueName': name}
        if attrs:
            attributes = ['DelaySeconds',
                          'MaximumMessageSize',
                          'MessageRetentionPeriod',
                          'Policy',
                          'ReceiveMessageWaitTimeSeconds',
                          'VisibilityTimeout',
            ]
            if not set(attrs.keys()).issubset(attributes):
                raise RequestParamError('Unknown queue attributes.')
            name_templ = 'Attribute.{}.Name'
            value_templ = 'Attribute.{}.Value'
            for i, item in enumerate(attrs.items(), start=1):
                attr, value = item
                params[name_templ.format(i)] = attr
                params[value_templ.format(i)] = value

        body = self.query_factory.submit('CreateQueue', **params)
        body.addCallback(parse_create_queue)

        return body

    def get_queue_url(self, queue, owner_id=None):
        """
            @param queue: required, C{str} maximum 80 characters;
                          alphanumeric characters, hyphens (-).
            @param owner_id: required if queue belongs to another AWS account,
                          C{str}, id of owner's AWS account.
        """
        params = {'QueueName': queue}
        if owner:
            params['QueueOwnerAWSAccountId'] = owner

        body = self.query_factory.submit('GetQueueUrl', **params)
        body.addCallback(parse_get_queue_url)

        return body

    def list_queues(self, prefix=None):
        """
            @param prefix: optional, C{str} maximum 80 characters;
                           alphanumeric characters, hyphens (-),
                           and underscores (_) are allowed.
        """
        params = {}
        if prefix:
            params['QueueNamePrefix'] = prefix

        body = self.query_factory.submit('ListQueues', **params)
        body.addCallback(parse_list_queues)

        return body


class Queue:
    """
        Requests are made with path set to "/owner_id/queue_name/?...".
        Share with SQSClient creds and agent with HTTPConnectionPool.
        API functions for a specific queue:
            - AddPermission;
            - ChangeMessageVisibility;
            - ChangeMessageVisibilityBatch;
            - DeleteMessage;
            - DeleteMessageBatch;
            - DeleteQueue;
            - GetQueueAttributes;
            - ReceiveMessage;
            - RemovePermission;
            - SendMessage;
            - SendMessageBatch;
            - SetQueueAttributes.
        Description of mostly used params:
            - receipt_handle (ReceiptHandle) -  special parameter to change
                            state of a message, received with receive_message.
            - timeout (VisibilityTimeout) - the length of time, in seconds, that
                            a message received from a queue will be invisible
                            to other receiving components when they ask to
                            receive messages.
            - delay_seconds (DelaySeconds) - the number of seconds to delay
                            a specific message.
    """

    def __init__(self, creds, endpoint, query_factory):
        self.creds = creds
        self.endpoint = endpoint
        self.query_factory = query_factory

    def add_permission(self, label, perms):
        """
            @param label: required, C{str}, max 80 characters;
                          alphanumeric characters, hyphens (-), and
                          underscores (_) are allowed.
                          The unique identification of the permission.
            @param perms: required, C{list} of C{tuple} (AWSAccountId, action).
            Actions: 'SendMessage', 'ReceiveMessage', ...
            Only owner can grant permissions.
        """
        params = {'Label': label}
        for i, item in enumerate(perms, start=1):
            account_id, action = item
            params['AWSAccountId.{}'.format(i)] = account_id
            params['ActionName.{}'.format(i)] = action

        body = self.query_factory.submit('AddPermission', **params)
        body.addCallback(empty_check)

        return body

    def change_message_visibility(self, receipt_handle, timeout):
        """
            @param receipt_handle: required, C{str}.
            @param timeout: optional, C{int}.
                            Seconds from 0 to 43200 (max 12 hours).
        """
        params = {'ReceiptHandle': receipt_handle,
                  'VisibilityTimeout': timeout}

        body = self.query_factory.submit('ChangeMessageVisibility', **params)
        body.addCallback(empty_check)

        return body

    def change_message_visibility_batch(self, receipt_handles, timeout):
        """
            @param receipt_handles: required, C{list} of receipt_handle;
            @param timeout: optional, C{list} of C{int} (accordingly to the
                        order of receipt_handle) or C{int} value if it is
                        common for all messages. From 0 to 43200 (max 12 hours).
        """
        if len(receipt_handles) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        prefix = 'ChangeMessageVisibilityBatchRequestEntry'
        if isinstance(timeout, int):
            timeout = [timeout for i in range(len(receipt_handles))]
        for i, param in enumerate(zip(receipt_handles, timeout), start=1):
            params['{}.{}.Id'.format(prefix, i)] = i
            params['{}.{}.ReceiptHandle'.format(prefix, i)] = param[0]
            params['{}.{}.VisibilityTimeout'.format(prefix, i)] = param[1]

        body = self.query_factory.submit('ChangeMessageVisibilityBatch', **params)
        body.addCallback(parse_change_message_visibility_batch)

        return body

    def delete_message(self, receipt_handle):
        """
            @param receipt_handle: required, C{str}.
        """
        params = {'ReceiptHandle': receipt_handle}

        body = self.query_factory.submit('DeleteMessage', **params)
        body.addCallback(empty_check)

        return body

    def delete_message_batch(self, receipt_handles):
        """
            @param receipt_handles: required, C{list} of receipt_handle C{str}.
        """
        if len(receipt_handles) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        prefix = 'DeleteMessageBatchRequestEntry'
        for i, receipt in enumerate(receipt_handles, start=1):
            params['{}.{}.Id'.format(prefix, i)] = i
            params['{}.{}.ReceiptHandle'.format(prefix, i)] = receipt

        body = self.query_factory.submit('DeleteMessageBatch', **params)
        body.addCallback(parse_delete_message_batch)

        return body

    def delete_queue(self):
        """
            The response is successful even if the specified queue does not exist.
        """
        body = self.query_factory.submit('DeleteQueue')
        body.addCallback(empty_check)

        return body

    def get_queue_attributes(self, attrs):
        """
            @param attrs: required, C{list} of C{str}, default C{None}.

            ApproximateNumberOfMessagesNotVisible — approximate
                number of messages that are not timed-out and not deleted.
            VisibilityTimeout — Seconds from 0 to 43200 (max 12 hours).
            CreatedTimestamp — epoch time in seconds.
            LastModifiedTimestamp — time when the queue was last changed
                (epoch time in seconds).
            Policy — A valid form-url-encoded policy.
            MaximumMessageSize — from 1024 to 65536 bytes (1-64 KiB).
            MessageRetentionPeriod (seconds) — 60-1209600 (1 minute - 14 days).
            QueueArn — queue's Amazon resource name (ARN).
            ReceiveMessageWaitTimeSeconds — integer (from 0 to 20),
                indicates whether short poll (0) or long poll (1-20) is used.
            DelaySeconds — 0-900.
        """
        valid = ['All',
                 'ApproximateNumberOfMessages',
                 'ApproximateNumberOfMessagesNotVisible',
                 'ApproximateNumberOfMessagesDelayed',
                 'VisibilityTimeout',
                 'CreatedTimestamp',
                 'LastModifiedTimestamp',
                 'Policy',
                 'MaximumMessageSize',
                 'MessageRetentionPeriod',
                 'QueueArn',
                 'ReceiveMessageWaitTimeSeconds',
                 'DelaySeconds',
        ]
        if not set(attrs).issubset(valid):
            raise RequestParamError('Unknown queue attributes.')
        params = {}
        for i, attr in enumerate(attrs, start=1):
            params['AttributeName.{}'.format(i)] = attr

        body = self.query_factory.submit('GetQueueAttributes', **params)
        body.addCallback(parse_queue_attributes)

        return body

    def receive_message(self, max_number_of_messages=None, timeout=None,
                        wait_time_seconds=None):
        """
            @param max_number_of_messages: optional, C{int} from 1 to 10,
                                           default 1.
            @param timeout: optional, C{int} from 0 to 43200 (maximum 12 hours),
                            default - visibility timeout for the queue.
            @param wait_time_seconds: optional, C{int} from 1 to 20, default -
                            'ReceiveMessageWaitTimeSeconds' of the queue.
                            Long poll support.
        """
        params = {}
        if max_number_of_messages:
            params['MaxNumberOfMessages'] = max_number_of_messages
        if timeout:
            params['VisibilityTimeout'] = timeout
        if wait_time_seconds:
            params['WaitTimeSeconds'] = wait_time_seconds

        body = self.query_factory.submit('ReceiveMessage', **params)
        body.addCallback(parse_receive_message)

        return body

    def remove_permission(self, label):
        """
            @param label: required, C{str}.
                          The identification of the permission.
        """
        params = {'Label': label}

        body = self.query_factory.submit('RemovePermission', **params)
        body.addCallback(empty_check)

        return body

    def send_message(self, message, delay_seconds=None):
        """
            @param message: required, C{str}.
            @param delay_seconds: optional, C{int} from 0 to 900 (15 minutes),
                                  default - value for the queue.
        """
        params = {'MessageBody': base64.b64encode(message)}
        if delay_seconds:
            params['DelaySeconds'] = delay_seconds

        body = self.query_factory.submit('SendMessage', **params)
        body.addCallback(empty_check)

        return body

    def send_message_batch(self, messages, delay_seconds=None):
        """
            @param messages: required, C{list} of C{str}.
            @param delay_seconds: optional, C{list} of C{int} or C{int}
                        from 0 to 900 (15 minutes) if it's common for all messages.
                        Default - value for the queue.
        """
        if len(messages) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        if isinstance(delay_seconds, int):
            delay_seconds = [delay_seconds for i in range(len(messages))]
        prefix = 'SendMessageBatchRequestEntry'

        for i, msg in enumerate(messages, start=1):
            params['{}.{}.Id'.format(prefix, i)] = i
            params['{}.{}.MessageBody'.format(prefix, i)] = base64.b64encode(msg)
            if delay_seconds:
                params['{}.{}.DelaySeconds'.format(prefix, i)] = delay_seconds[i - 1]

        body = self.query_factory.submit('SendMessageBatch', **params)
        body.addCallback(parse_send_message_batch)

        return body

    def set_queue_attributes(self, attr, value):
        """
            @param attr: required, C{str}.
            @param value: required, type depends on attr
                          (described in get_queue_attributes).
            Sets one attribute of a queue per request.
        """
        valid = ['DelaySeconds',
                 'MaximumMessageSize',
                 'MessageRetentionPeriod',
                 'Policy',
                 'ReceiveMessageWaitTimeSeconds',
                 'VisibilityTimeout',
        ]
        if attr not in valid:
            raise RequestParamError('Unknown queue attribute.')
        params = {
            'Attribute.Name': attr,
            'Attribute.Value': value
        }

        body = self.query_factory.submit('SetQueueAttributes', **params)
        body.addCallback(empty_check)

        return body