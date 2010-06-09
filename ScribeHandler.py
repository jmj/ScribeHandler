import logging, socket

from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

class ScribeLogError(Exception):
    pass

class ScribeHandler(logging.Handler):
    def __init__(self, host='127.0.0.1', port=1463,
        category=None, **kw):

        if category is None:
            self.category = '%(hostname)s-%(loggername)s'

        socket = TSocket.TSocket(host=host, port=port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport, strictRead=False, strictWrite=False)
        self.client = scribe.Client(iprot=protocol, oprot=protocol)

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


