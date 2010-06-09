import logging, socket

from urlparse import urlparse

from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

class ScribeLogError(Exception):
    pass

class ScribeHandler(logging.Handler):
    def __init__(self, host='127.0.0.1', port=1463,
        category=None, framed=True):

        if category is None:
            self.category = '%(hostname)s-%(loggername)s'
        else:
            self.category = category

        socket = TSocket.TSocket(host=host, port=port)
        if framed:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)

        protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport,
            strictRead=False, strictWrite=False)
        self.client = scribe.Client(protocol)

        logging.Handler.__init__(self)


    def emit(self, record):

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


