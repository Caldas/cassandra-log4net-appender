using System;
using Thrift.Protocol;
using Thrift.Transport;

namespace CassandraLog4NetAppenderLibrary.Logging
{
    public class DefaultCassandraClientFactory : ICassandraClientFactory
    {
        public String Host { get; set; }
        public Int32 Port { get; set; }

        public DefaultCassandraClientFactory(String host, Int32 port)
        {
            Host = host;
            Port = port;
        }

        public Apache.Cassandra.Cassandra.Iface CreateConnection()
        {
            TSocket socket = new TSocket(Host, Port);
            TTransport trans = new TFramedTransport(socket);
            trans.Open();
            return new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(trans));
        }
    }
}