using System;
using Thrift.Protocol;
using Thrift.Transport;

namespace CassandraLog4NetAppenderLibrary.Logging
{
    public class DefaultCassandraClientFactory : ICassandraClientFactory
    {
        public Apache.Cassandra.Cassandra.Iface CreateConnection(String host, Int32 port)
        {
            TSocket socket = new TSocket(host, port);
            TTransport trans = new TFramedTransport(socket);
            trans.Open();
            return new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(trans));
        }
    }
}