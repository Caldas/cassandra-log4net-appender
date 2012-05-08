// -----------------------------------------------------------------------
// <copyright file="CassandraAppender.cs" company="VTEX OnLine Applications">
//     Copyright (c) VTEX OnLine Applications. All rights reserved.
// </copyright>
// <author>Fábio Caldas</author>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using Apache.Cassandra;
using CassandraLog4NetAppenderLibrary.Cassandra;
using CassandraLog4NetAppenderLibrary.Logging.LevelMapping;
using log4net.Appender;
using log4net.Core;

namespace CassandraLog4NetAppenderLibrary.Logging.Appender
{
    public class CassandraAppender : AppenderSkeleton
    {
        private Dictionary<Byte[], Dictionary<String, List<Mutation>>> rowBuffer;
        private Apache.Cassandra.Cassandra.Iface client;

        protected static String ip = GetIP();
        protected static String hostname = GetHostName();

        public CassandraLevelMapping LevelMapping { get; set; }

        public CassandraAppender() { }

        public new Boolean RequiresLayout()
        {
            return false;
        }

        public void AddMapping(CassandraLevelMapping mapping)
        {
            LevelMapping = mapping;
        }

        override public void ActivateOptions()
        {
            base.ActivateOptions();
            try
            {
                ICassandraClientFactory cassandraClientFactory = null;

                if (String.IsNullOrWhiteSpace(LevelMapping.CassandraClientFactory))
                    cassandraClientFactory = new DefaultCassandraClientFactory(LevelMapping.Hosts, LevelMapping.GetPort());
                else
                    cassandraClientFactory = (ICassandraClientFactory)Activator.CreateInstance(Type.GetType(LevelMapping.CassandraClientFactory));

                this.client = cassandraClientFactory.CreateConnection();
            }
            catch (Exception exception)
            {
                throw new Exception("Can't initialize cassandra connections", exception);
            }

            try
            {
                SetupSchema();
            }
            catch (Exception exception)
            {
                throw new Exception("Error setting up cassandra logging schema", exception);
            }

            try
            {
                this.client.set_keyspace(LevelMapping.KeyspaceName);
            }
            catch (Exception exception)
            {
                throw new Exception("Error setting keyspace: " + LevelMapping.KeyspaceName, exception);
            }

            Reset();
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            Byte[] rowId = TimeGenerator.GetTimeUUID().ToCassandraByte();
            Dictionary<String, List<Mutation>> mutMap = new Dictionary<String, List<Mutation>>();
            mutMap.Add(LevelMapping.ColumnFamily, CreateMutationList(loggingEvent));
            this.rowBuffer.Add(rowId, mutMap);
            FlushIfNecessary();
        }

        protected void FlushIfNecessary()
        {
            if (this.rowBuffer.Count >= LevelMapping.GetMaxBufferedRows())
                Flush();
        }

        protected void Flush()
        {
            if (this.rowBuffer.Count > 0)
            {
                try
                {
                    this.client.batch_mutate(this.rowBuffer, LevelMapping.GetConsistencyLevel());
                }
                catch (Exception exception)
                {
                    throw new Exception("Failed to persist in Cassandra", exception);
                }
                Reset();
            }
        }

        public new void Close()
        {
            Flush();
            base.Close();
        }

        protected void Reset()
        {
            this.rowBuffer = new Dictionary<Byte[], Dictionary<String, List<Mutation>>>();
        }

        public virtual List<Mutation> CreateMutationList(LoggingEvent loggingEvent)
        {
            List<Mutation> mutList = new List<Mutation>();

            Int64 colTs = TimeGenerator.GetUnixTime();

            CreateMutation(mutList, "app_name", LevelMapping.AppName, colTs);
            CreateMutation(mutList, "host_ip", ip, colTs);
            CreateMutation(mutList, "host_name", hostname, colTs);
            CreateMutation(mutList, "logger_name", loggingEvent.LoggerName, colTs);
            CreateMutation(mutList, "level", loggingEvent.Level.ToString(), colTs);
            LocationInfo locInfo = loggingEvent.LocationInformation;
            if (locInfo != null)
            {
                CreateMutation(mutList, "class_name", locInfo.ClassName, colTs);
                CreateMutation(mutList, "file_name", locInfo.FileName, colTs);
                CreateMutation(mutList, "line_number", locInfo.LineNumber, colTs);
                CreateMutation(mutList, "method_name", locInfo.MethodName, colTs);
            }
            CreateMutation(mutList, "message", loggingEvent.RenderedMessage, colTs);
            CreateMutation(mutList, "app_start_time", TimeGenerator.GetUnixTime(LoggingEvent.StartTime), colTs);
            CreateMutation(mutList, "thread_name", loggingEvent.ThreadName, colTs);
            CreateMutation(mutList, "log_timestamp", TimeGenerator.GetUnixTime(loggingEvent.TimeStamp), colTs);
            CreateMutation(mutList, "throwable_str_rep", loggingEvent.GetExceptionString(), colTs);

            return mutList;
        }

        public virtual CfDef CreateCfDef()
        {
            CfDef cfDef = new CfDef();
            cfDef.Keyspace = LevelMapping.KeyspaceName;
            cfDef.Name = LevelMapping.ColumnFamily;
            cfDef.Key_validation_class = "UUIDType";
            cfDef.Comparator_type = "UTF8Type";
            cfDef.Default_validation_class = "UTF8Type";

            AddColumn(cfDef, "app_name", "UTF8Type");
            AddColumn(cfDef, "host_ip", "UTF8Type");
            AddColumn(cfDef, "host_name", "UTF8Type");
            AddColumn(cfDef, "logger_name", "UTF8Type");
            AddColumn(cfDef, "level", "UTF8Type");
            AddColumn(cfDef, "class_name", "UTF8Type");
            AddColumn(cfDef, "file_name", "UTF8Type");
            AddColumn(cfDef, "line_number", "UTF8Type");
            AddColumn(cfDef, "method_name", "UTF8Type");
            AddColumn(cfDef, "message", "UTF8Type");
            AddColumn(cfDef, "app_start_time", "LongType");
            AddColumn(cfDef, "thread_name", "UTF8Type");
            AddColumn(cfDef, "throwable_str_rep", "UTF8Type");
            AddColumn(cfDef, "log_timestamp", "LongType");

            return cfDef;
        }

        protected void CreateMutation(List<Mutation> mutList, String column, Int64 value, Int64 timestamp)
        {
            CreateMutation(mutList, column, value.ToCassandraByte(), timestamp);
        }

        protected void CreateMutation(List<Mutation> mutList, String column, String value, Int64 timestamp)
        {
            if (!String.IsNullOrWhiteSpace(value))
                CreateMutation(mutList, column, value.ToCassandraByte(), timestamp);
        }

        protected void CreateMutation(List<Mutation> mutList, String column, Byte[] value, Int64 timestamp)
        {
            Mutation mutation = new Mutation();
            Column col = new Column();
            col.Name = column.ToCassandraByte();
            col.Value = value;
            col.Timestamp = timestamp;
            col.Ttl = LevelMapping.GetTtl();
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
            cosc.Column = col;
            mutation.Column_or_supercolumn = cosc;
            mutList.Add(mutation);
        }
        
        private void SetupSchema()
        {
            try
            {
                KsDef ksDef = VerifyKeyspace();
                if (ksDef == null)
                    CreateKeyspaceAndColumnFamily();
                else if (!CheckForColumnFamily(ksDef))
                    CreateColumnFamily();
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        private void CreateKeyspaceAndColumnFamily()
        {
            List<CfDef> cfDefList = new List<CfDef>();
            cfDefList.Add(CreateCfDef());
            try
            {
                KsDef ksDef = new KsDef();
                ksDef.Name = LevelMapping.KeyspaceName;
                ksDef.Strategy_class = LevelMapping.PlacementStrategy;
                ksDef.Cf_defs = cfDefList;
                var strategyOptions = LevelMapping.GetStrategyOptions();
                if (LevelMapping.PlacementStrategy.Equals("org.apache.cassandra.locator.SimpleStrategy") && !strategyOptions.ContainsKey("replication_factor"))
                    strategyOptions.Add("replication_factor", LevelMapping.ReplicationFactor.ToString());
                ksDef.Strategy_options = strategyOptions;
                this.client.system_add_keyspace(ksDef);
                int magnitude = this.client.describe_ring(LevelMapping.KeyspaceName).Count;
                Thread.Sleep(1000 * magnitude);
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        private void CreateColumnFamily()
        {
            CfDef cfDef = CreateCfDef();
            try
            {
                this.client.set_keyspace(LevelMapping.KeyspaceName);
                this.client.system_add_column_family(cfDef);
                int magnitude = this.client.describe_ring(LevelMapping.KeyspaceName).Count;
                Thread.Sleep(1000 * magnitude);
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private KsDef VerifyKeyspace()
        {
            KsDef ksDef = null;
            try
            {
                ksDef = this.client.describe_keyspace(LevelMapping.KeyspaceName);
            }
            catch (NotFoundException)
            {
            }
            catch (Exception exception)
            {
                throw new IOException("Exception caught while trying to verify keyspace existance.", exception);
            }
            return ksDef;
        }

        private Boolean CheckForColumnFamily(KsDef ksDef)
        {
            Boolean exists = false;
            foreach (CfDef cfDef in ksDef.Cf_defs)
            {
                if (cfDef.Name.Equals(LevelMapping.ColumnFamily))
                {
                    exists = true;
                    break;
                }
            }

            return exists;
        }

        protected CfDef AddColumn(CfDef cfDef, String columnName, String validator)
        {
            ColumnDef colDef = new ColumnDef();
            colDef.Name = columnName.ToCassandraByte();
            colDef.Validation_class = validator;
            cfDef.Column_metadata = new List<ColumnDef>(1) { colDef };
            return cfDef;
        }

        private static String GetHostName()
        {
            String hostname = "unknown";
            try
            {
                hostname = Dns.GetHostName();
            }
            catch (Exception exception)
            {
                throw exception;
            }
            return hostname;
        }

        private static String GetIP()
        {
            String ip = "unknown";
            try
            {
                IPAddress[] addr = Dns.GetHostEntry(GetHostName()).AddressList;
                ip = addr[addr.Length - 1].ToString();
            }
            catch (Exception exception)
            {
                throw exception;
            }
            return ip;
        }

        private static String Unescape(String b)
        {
            if ((b[0] == '"') && (b[b.Length - 1] == '"'))
                b = b.Substring(1, b.Length - 1);
            return b;
        }
    }
}