# ScribeHandler is a simple proxy layer that works with the python standard
# logging module (http://docs.python.org/library/logging.html).  ScribeHandler
# acts a a handler object that gets added to a logger in the standard way.
# Copyright (C) 2010 Jeremy Jones
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Contact the author:
# http://www.letifer.org
# http://github.com/ScribeHandler
# jeremyj (at) letifer (dot) org


import logging, socket, shelve

from urlparse import urlparse

from scribe import scribe

from thrift.transport import TTransport, TSocket, THttpClient
from thrift.transport.TTransport import TTransportException

from thrift.protocol import TBinaryProtocol

version='0.05'

class ScribeLogError(Exception): pass
class ScribeTransportError(Exception): pass
class ScribeHandlerBufferError(Exception): pass

FRAMED = 1
UNFRAMED = 2
HTTP = 3


class ScribeHandler(logging.Handler):
    def __init__(self, host='127.0.0.1', port=1463,
        category=None, transport=FRAMED, uri=None, file_buffer=None):

        self.__buffer = None
        self.file_buffer = file_buffer

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

    def _get_buffer(self):
        if self.file_buffer is None:
            raise ScribeHandlerBufferError('No buffer file defined')

        try:
            self.__buffer.keys()
        except AttributeError:
            self.__buffer = None
        except ValueError:
            self.__buffer = None

        if self.__buffer is None:
            self.__buffer = shelve.open(self.file_buffer)

        return self.__buffer


    def _make_client(self):

        protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport,
            strictRead=False, strictWrite=False)
        self.client = scribe.Client(protocol)

    def __setattr__(self, var, val):
        ## Filterer is an old style class through at least 3.1
        self.__dict__[var] = val

        if var == 'transport':
            self._make_client()

    def get_entries(self, new):
        if self.file_buffer is not None:
            self._get_buffer()
        else:
            yield (None,new)
            return

        self.add_entry(new)

        sortedkeys = self.__buffer.keys()
        sortedkeys.sort()

        for k in sortedkeys:
            yield (k,self.__buffer[k])

        self.__buffer.close()

    def pop_entry(self, key):
        if self.file_buffer is None:
            return
        ## buffer should already be open
        self.__buffer.pop(key)
        self.__buffer.sync()

    def add_entry(self, new):
        if self.file_buffer is None:
            return

        self._get_buffer()

        try:
            topkey = max(self.__buffer.keys())
        except ValueError:
            topkey = -1

        newkey = '%s' % (int(topkey) + 1)
        self.__buffer[newkey] = new
        self.__buffer.sync()


    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then logged to Scribe with a trailing newline.
        """
        # apply formatting to the record.
        msg = self.format(record)
        # for backwards-compatibility, do not add in a line break if it
        # is already being manually added into each record.
        if not msg.startswith('\n') or msg.endswith('\n'):
          msg += '\n'

        if (self.client is None) or (self.transport is None):
            raise ScribeTransportError('No transport defined')

        # It looks like pypy Does not have logging.logRecord.processName
        # This is a hackish workaround.
        if hasattr(record, 'processName'):
            pn = record.processName
        else:
            pn = 'Unknown'

        category = self.category % {
            'module' : record.module,
            'levelname': record.levelname,
            'loggername' : record.name,
            'processName' : pn,
            'hostname' : socket.gethostname(),
        }

        log_entry = scribe.LogEntry(category=category, message=msg)

        try:
            self.transport.open()

            for le in self.get_entries(log_entry):
                result = self.client.Log(messages=[le[1]])
                if result != scribe.ResultCode.OK:
                    raise ScribeLogError(result)
                self.pop_entry(le[0])

            self.transport.close()

        except TTransportException:
            if self.file_buffer is not None:
                self.add_entry(log_entry)
            self._do_error(record)
        except:
                self._do_error(record)

    def _do_error(self, record):
        if self.file_buffer is not None:
            self.__buffer.sync()
            self.__buffer.close()
        else:
            self.handleError(record)


