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

namespace CassandraLog4NetAppenderLibrary.Logging.Appender
{
    public class CassandraAppender : AppenderSkeleton
    {
        public String ColumnFamily { get; private set; }
        public String Keyspace { get; private set; }
        public String KeyStrategy { get; private set; }
        public String ThriftAddress { get; private set; }
        public Int32 ThriftPort { get; private set; }

        public CassandraAppender() { }

        override public void ActivateOptions()
        {
            base.ActivateOptions();
            new CassandraLevelMapping().ActivateOptions();

            //TODO: #2 Create Cassandra Client
        }

        public void AddMapping(CassandraLevelMapping mapping)
        {
            ColumnFamily = mapping.ColumnFamily;
            Keyspace = mapping.Keyspace;
            KeyStrategy = mapping.KeyStrategy;
            ThriftAddress = mapping.ThriftAddress;

            Int32 baseThriftPort = 9160;
            Int32.TryParse(mapping.ThriftPort, out baseThriftPort);
            ThriftPort = baseThriftPort;
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            try
            {
                Byte[] key = null;
                ColumnParent column_parent = null;
                Column column = null;
                ConsistencyLevel consistency_level = ConsistencyLevel.ONE;

                //TODO: #1 Insert data
                //client.insert(key, column_parent, column, consistency_level);
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        //TODO: #1 Review retrieve data methods
        //private ColumnPath getColumnPath(String columnFamily, LoggingEvent loggingEvent)
        //{
        //    try
        //    {
        //        ColumnPath path = new ColumnPath();
        //        path.Column_family = columnFamily;
        //        if (KeyStrategy.Equals("uuid"))
        //        {
        //            path.Column = TimeGenerator.GetTimeUUID().ToString().ToCassandraByte();
        //        }
        //        else
        //        {
        //            String column = loggingEvent.LocationInformation.MethodName + "-" + loggingEvent.LocationInformation.LineNumber + "-" + DateTime.Now.ToString();
        //            path.Column = column.ToCassandraByte();
        //        }
        //        return path;
        //    }
        //    catch (Exception exception)
        //    {
        //        throw exception;
        //    }
        //}

        //private byte[] getStoreValue(LoggingEvent loggingEvent)
        //{
        //    try
        //    {
        //        return loggingEvent.MessageObject.ToCassandraByte();
        //    }
        //    catch (Exception exception)
        //    {
        //        throw exception;
        //    }
        //}

        public void Close()
        {
            //TODO: #2 Close Cassandra client and Thrifth transport
        }

        public Boolean RequiresLayout()
        {
            return true;
        }
    }
}