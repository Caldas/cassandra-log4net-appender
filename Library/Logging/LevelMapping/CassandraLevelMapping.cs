// -----------------------------------------------------------------------
// <copyright file="CassandraLevelMapping.cs" company="VTEX OnLine Applications">
//     Copyright (c) VTEX OnLine Applications. All rights reserved.
// </copyright>
// <author>Fábio Caldas</author>
// -----------------------------------------------------------------------
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
        //TODO: #3 Add ConsistencyLevel to Mapping

        public override void ActivateOptions()
        {
            base.ActivateOptions();
        }
    }
}