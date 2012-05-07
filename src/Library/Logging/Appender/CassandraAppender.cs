// -----------------------------------------------------------------------
// <copyright file="CassandraAppender.cs" company="VTEX OnLine Applications">
//     Copyright (c) VTEX OnLine Applications. All rights reserved.
// </copyright>
// <author>Fábio Caldas</author>
// -----------------------------------------------------------------------
using System;
using Apache.Cassandra;
using CassandraLog4NetAppenderLibrary.Logging.LevelMapping;
using log4net.Appender;
using log4net.Core;
using Thrift.Transport;
using Thrift.Protocol;
using CassandraLog4NetAppenderLibrary.Cassandra;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Threading;

namespace CassandraLog4NetAppenderLibrary.Logging.Appender
{
    public class CassandraAppender : AppenderSkeleton
    {
        public static String HOST_IP = "host_ip";
        public static String HOST_NAME = "host_name";
        public static String APP_NAME = "app_name";
        public static String LOGGER_NAME = "logger_name";
        public static String LEVEL = "level";
        public static String CLASS_NAME = "class_name";
        public static String FILE_NAME = "file_name";
        public static String LINE_NUMBER = "line_number";
        public static String METHOD_NAME = "method_name";
        public static String MESSAGE = "message";
        public static String NDC = "ndc";
        public static String APP_START_TIME = "app_start_time";
        public static String THREAD_NAME = "thread_name";
        public static String THROWABLE_STR = "throwable_str_rep";
        public static String TIMESTAMP = "log_timestamp";

        private String keyspaceName = "Logging";
        private String columnFamily = "log_entries";
        private String appName = "default";
        private String placementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
        private Dictionary<String, String> strategyOptions = new Dictionary<String, String>();
        private Int32 replicationFactor = 1;
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
        private Int32 maxBufferedRows = 1;
        private String hosts = "localhost";
        private Int32 port = 9160;
        private Encoding charset = Encoding.UTF8;

        private Dictionary<Byte[], Dictionary<String, List<Mutation>>> rowBuffer;
        private Apache.Cassandra.Cassandra.Iface client;

        private ICassandraClientFactory cassandraClientFactory = null;

        private static String ip = GetIP();
        private static String hostname = GetHostName();

        public CassandraAppender() { }

        public void SetCassandraClientFactory(ICassandraClientFactory newFactory) 
        {
            cassandraClientFactory = newFactory;
        }

        public new Boolean RequiresLayout()
        {
            return false;
        }

        override public void ActivateOptions()
        {
            try
            {
                if (cassandraClientFactory == null)
                    cassandraClientFactory = new DefaultCassandraClientFactory(hosts, port);

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
                this.client.set_keyspace(this.keyspaceName);
            }
            catch (Exception exception)
            {
                throw new Exception("Error setting keyspace: " + this.keyspaceName, exception);
            }

            Reset();
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            Byte[] rowId = TimeGenerator.GetTimeUUID().ToCassandraByte();
            Dictionary<String, List<Mutation>> mutMap = new Dictionary<String, List<Mutation>>();
            mutMap.Add(this.columnFamily, CreateMutationList(loggingEvent));
            this.rowBuffer.Add(rowId, mutMap);
            FlushIfNecessary();
        }

        private void FlushIfNecessary()
        {
            if (this.rowBuffer.Count >= this.maxBufferedRows)
                Flush();
        }

        private void Flush()
        {
            if (this.rowBuffer.Count > 0)
            {
                try
                {
                    this.client.batch_mutate(this.rowBuffer, this.consistencyLevel);
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

        private void Reset()
        {
            this.rowBuffer = new Dictionary<Byte[], Dictionary<String, List<Mutation>>>();
        }

        private List<Mutation> CreateMutationList(LoggingEvent loggingEvent)
        {
            List<Mutation> mutList = new List<Mutation>();

            Int64 colTs = TimeGenerator.GetUnixTime();

            CreateMutation(mutList, "app_name", this.appName, colTs);
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
            //CreateMutation(mutList, "ndc", event.getNDC(), colTs); //REMOVED
            CreateMutation(mutList, "app_start_time", TimeGenerator.GetUnixTime(LoggingEvent.StartTime), colTs);
            CreateMutation(mutList, "thread_name", loggingEvent.ThreadName, colTs);
            CreateMutation(mutList, "log_timestamp", TimeGenerator.GetUnixTime(loggingEvent.TimeStamp), colTs);
            CreateMutation(mutList, "throwable_str_rep", loggingEvent.GetExceptionString(), colTs);

            return mutList;
        }

        private void CreateMutation(List<Mutation> mutList, String column, Int64 value, Int64 timestamp)
        {
            CreateMutation(mutList, column, value.ToCassandraByte(), timestamp);
        }

        private void CreateMutation(List<Mutation> mutList, String column, String value, Int64 timestamp)
        {
            if (!String.IsNullOrWhiteSpace(value))
                CreateMutation(mutList, column, value.ToCassandraByte(), timestamp);
        }

        private void CreateMutation(List<Mutation> mutList, String column, Byte[] value, Int64 timestamp)
        {
            Mutation mutation = new Mutation();
            Column col = new Column();
            col.Name = column.ToCassandraByte();
            col.Value = value;
            col.Timestamp = timestamp;
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

        public String GetKeyspaceName()
        {
            return this.keyspaceName;
        }

        public void SetKeyspaceName(String keyspaceName)
        {
            this.keyspaceName = keyspaceName;
        }

        public String GetHosts()
        {
            return this.hosts;
        }

        public void SetHosts(String hosts)
        {
            this.hosts = hosts;
        }

        public int GetPort()
        {
            return this.port;
        }

        public void SetPort(int port)
        {
            this.port = port;
        }

        public String GetColumnFamily()
        {
            return this.columnFamily;
        }

        public void SetColumnFamily(String columnFamily)
        {
            this.columnFamily = columnFamily;
        }

        public String GetPlacementStrategy()
        {
            return this.placementStrategy;
        }

        public void SetPlacementStrategy(String strategy)
        {
            if (strategy == null)
            {
                throw new NullReferenceException("placementStrategy can't be null");
            }
            this.placementStrategy = Unescape(strategy);
        }

        public String GetStrategyOptions()
        {
            return this.strategyOptions.ToString();
        }

        public void SetStrategyOptions(String newOptions)
        {
            if (newOptions == null)
                throw new NullReferenceException("strategyOptions can't be null.");

            try
            {
                //TODO: Convert java code
                //this.strategyOptions = ((Map)jsonMapper.readValue(unescape(newOptions), this.strategyOptions.getClass()));
            }
            catch (Exception exception)
            {
                throw new ArgumentException(String.Format("Invalid JSON map: {0}, error: {1}", newOptions, exception.Message));
            }
        }

        public Int32 GetReplicationFactor()
        {
            return this.replicationFactor;
        }

        public void SetReplicationFactor(int replicationFactor)
        {
            this.replicationFactor = replicationFactor;
        }

        public String GetConsistencyLevelWrite()
        {
            return this.consistencyLevel.ToString();
        }

        public void SetConsistencyLevelWrite(String consistencyLevelWrite)
        {
            Enum.TryParse<ConsistencyLevel>(Unescape(consistencyLevelWrite), out consistencyLevel);
        }

        public int GetMaxBufferedRows()
        {
            return this.maxBufferedRows;
        }

        public void SetMaxBufferedRows(int maxBufferedRows)
        {
            this.maxBufferedRows = maxBufferedRows;
        }

        public String GetAppName()
        {
            return this.appName;
        }

        public void SetAppName(String appName)
        {
            this.appName = appName;
        }

        private KsDef VerifyKeyspace()
        {
            KsDef ksDef = null;
            try
            {
                ksDef = this.client.describe_keyspace(this.keyspaceName);
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
                if (cfDef.Name.Equals(this.columnFamily))
                {
                    exists = true;
                    break;
                }
            }

            return exists;
        }

        private void CreateKeyspaceAndColumnFamily()
        {
            List<CfDef> cfDefList = new List<CfDef>();
            cfDefList.Add(CreateCfDef());
            try
            {
                KsDef ksDef = new KsDef();
                ksDef.Name = this.keyspaceName;
                ksDef.Strategy_class = this.placementStrategy;
                ksDef.Cf_defs = cfDefList;
                ksDef.Strategy_options = this.strategyOptions;
                if (this.placementStrategy.Equals("org.apache.cassandra.locator.SimpleStrategy") && !this.strategyOptions.ContainsKey("replication_factor"))
                    this.strategyOptions.Add("replication_factor", this.replicationFactor.ToString());

                this.client.system_add_keyspace(ksDef);
                int magnitude = this.client.describe_ring(this.keyspaceName).Count;
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
                this.client.set_keyspace(this.keyspaceName);
                this.client.system_add_column_family(cfDef);
                int magnitude = this.client.describe_ring(this.keyspaceName).Count;
                Thread.Sleep(1000 * magnitude);
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private CfDef CreateCfDef()
        {
            CfDef cfDef = new CfDef();
            cfDef.Keyspace = this.keyspaceName;
            cfDef.Name = this.columnFamily;
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
            //AddColumn(cfDef, "ndc", "UTF8Type"); //REMOVED
            AddColumn(cfDef, "app_start_time", "LongType");
            AddColumn(cfDef, "thread_name", "UTF8Type");
            AddColumn(cfDef, "throwable_str_rep", "UTF8Type");
            AddColumn(cfDef, "log_timestamp", "LongType");

            return cfDef;
        }

        private CfDef AddColumn(CfDef cfDef, String columnName, String validator)
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