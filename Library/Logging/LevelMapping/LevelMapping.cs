using System;
using log4net.Util;

namespace CassandraLog4NetAppenderLibrary.Logging.LevelMapping
{
    public class CassandraLevelMapping : LevelMappingEntry
    {
        public String Keyspace { get; set; }
        public String ColumnFamily { get; set; }
        public String ThriftAddress { get; set; }
        public String ThriftPort { get; set; }
        public String KeyStrategy { get; set; }
        //TODO: Add ConsistencyLevel to Mapping

        public override void ActivateOptions()
        {
            base.ActivateOptions();
        }
    }
}