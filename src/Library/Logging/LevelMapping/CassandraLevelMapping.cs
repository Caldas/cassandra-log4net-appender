// -----------------------------------------------------------------------
// <copyright file="CassandraLevelMapping.cs" company="VTEX OnLine Applications">
//     Copyright (c) VTEX OnLine Applications. All rights reserved.
// </copyright>
// <author>Fábio Caldas</author>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Apache.Cassandra;
using log4net.Util;

namespace CassandraLog4NetAppenderLibrary.Logging.LevelMapping
{
    public class CassandraLevelMapping : LevelMappingEntry
    {
        private const Int32 defaultPort = 9160;
        private const Int32 defaultMaxBufferedRows = 1;
        private const Int32 defaultTtl = 0;
        private const Apache.Cassandra.ConsistencyLevel defaultConsistencyLevel = Apache.Cassandra.ConsistencyLevel.ONE;

        public String Host { get; set; }
        public String Port { get; set; }
        public String AppName { get; set; }
        public String KeyspaceName { get; set; }
        public String ColumnFamily { get; set; }
        public String PlacementStrategy { get; set; }
        public String StrategyOptions { get; set; }
        public String ReplicationFactor { get; set; }
        public String ConsistencyLevel { get; set; }
        public String Ttl { get; set; }
        public String MaxBufferedRows { get; set; }
        public String CassandraClientFactory { get; set; }

        public CassandraLevelMapping()
        {
            KeyspaceName = "Logging";
            ColumnFamily = "log_entries";
            AppName = "default";
            PlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
            ReplicationFactor = "1";
            ConsistencyLevel = defaultConsistencyLevel.ToString();
            Ttl = String.Empty;
            MaxBufferedRows = defaultMaxBufferedRows.ToString();
            Host = "localhost";
            Port = defaultPort.ToString();
            CassandraClientFactory = String.Empty;
        }

        public override void ActivateOptions()
        {
            base.ActivateOptions();
        }

        public Int32 GetPort() 
        {
            Int32 port = defaultPort;
            Int32.TryParse(Port, out port);
            return port;
        }

        public Int32 GetMaxBufferedRows()
        {
            Int32 maxBufferedRows = defaultMaxBufferedRows;
            Int32.TryParse(MaxBufferedRows, out maxBufferedRows);
            return maxBufferedRows;
        }

        public Apache.Cassandra.ConsistencyLevel GetConsistencyLevel() 
        {
            Apache.Cassandra.ConsistencyLevel consistencyLevel = defaultConsistencyLevel;
            Enum.TryParse<ConsistencyLevel>(ConsistencyLevel, out consistencyLevel);
            return consistencyLevel;
        }

        public Dictionary<String, String> GetStrategyOptions()
        {
            Dictionary<String, String> strategyOptions = new Dictionary<String, String>();
            String[] optionParts;
            foreach (String option in StrategyOptions.Split(new Char[]{','}))
            {
                optionParts = option.Split(new Char[]{':'});
                if (optionParts.Length == 2)
                    strategyOptions.Add(optionParts[0], optionParts[1]);
            }
            return strategyOptions;
        }

        public Int32 GetTtl() 
        {
            Int32 ttl = defaultTtl;
            Int32.TryParse(Ttl, out ttl);
            return ttl;
        }
    }
}