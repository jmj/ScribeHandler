import logging
from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

class ScribeHandler(logging.Handler):
    def __init__(self, *args, **kw):
        host = kw.get('host', '127.0.0.1')
        port = kw.get('port', 1463)
        self.category = kw.get('category', 'error')

        socket = TSocket.TSocket(host=host, port=port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport, strictRead=False, strictWrite=False)
        self.client = scribe.Client(iprot=protocol, oprot=protocol)

        logging.Handler.__init__(self)


    def emit(self, record):
        log_entry = scribe.LogEntry(category=self.category,
            message=record.getMessage())
        self.transport.open()
        result = self.client.Log(messages=[log_entry])
        self.transport.close()



