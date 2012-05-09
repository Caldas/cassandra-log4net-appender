using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraLog4NetAppenderLibrary.Logging
{
    public interface ICassandraClientFactory
    {
        Apache.Cassandra.Cassandra.Iface CreateConnection(String host, Int32 port);
    }
}