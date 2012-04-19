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
        public String Hosts { get; set; }
        public String Port { get; set; }
        public String AppName { get; set; }
        public String KeyspaceName { get; set; }
        public String ColumnFamily { get; set; }
        public String PlacementStrategy { get; set; }
        public String StrategyOptions { get; set; }
        public String ReplicationFactor { get; set; }
        public String ConsistencyLevelWrite { get; set; }
        public String MaxBufferedRows { get; set; }

        public override void ActivateOptions()
        {
            base.ActivateOptions();
        }
    }
}