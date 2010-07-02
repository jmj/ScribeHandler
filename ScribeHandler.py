import logging, socket

from urlparse import urlparse

from scribe import scribe
from thrift.transport import TTransport, TSocket, THttpClient
from thrift.protocol import TBinaryProtocol

version='0.02'

class ScribeLogError(Exception): pass
class ScribeTransportError(Exception): pass

FRAMED = 1
UNFRAMED = 2
HTTP = 3


class ScribeHandler(logging.Handler):
    def __init__(self, host='127.0.0.1', port=1463,
        category=None, transport=FRAMED, uri=None):

        if category is None:
            self.category = '%(hostname)s-%(loggername)s'
        else:
            self.category = category

        if transport is None:
            self.transport = None
            self.client = None
            logging.Handler.__init__(self)
            return

        if transport == HTTP:
            if uri is None:
                raise ScribeLogError('http transport with no uri')
            self.transport = THttpClient.THttpClient(host, port, uri)
        else:
            socket = TSocket.TSocket(host=host, port=port)

            if transport == FRAMED:
                self.transport = TTransport.TFramedTransport(socket)
            elif transport == UNFRAMED:
                self.transport = TTransport.TBufferedTransport(socket)
            else:
                raise ScribeLogError('Unsupported transport type')

        #self._make_client()
        logging.Handler.__init__(self)

    def _make_client(self):

        protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport,
            strictRead=False, strictWrite=False)
        self.client = scribe.Client(protocol)

    def __setattr__(self, var, val):
        ## Filterer is an old style class through at least 3.1
        self.__dict__[var] = val

        if var == 'transport':
            self._make_client()


    def emit(self, record):

        if (self.client is None) or (self.transport is None):
            raise ScribeTransportError('No transport defined')

        category = self.category % {
            'module' : record.module,
            'levelname': record.levelname,
            'loggername' : record.name,
            'processName' : record.processName,
            'hostname' : socket.gethostname(),
        }

        log_entry = scribe.LogEntry(category=category,
            message=record.getMessage())

        try:
            self.transport.open()
            result = self.client.Log(messages=[log_entry])
            self.transport.close()

            if result != scribe.ResultCode.OK:
                raise ScribeLogError(result)
        except:
            self.raiseException(record)


