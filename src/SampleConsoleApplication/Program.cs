// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="VTEX OnLine Applications">
//     Copyright (c) VTEX OnLine Applications. All rights reserved.
// </copyright>
// <author>Fábio Caldas</author>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;

namespace SampleConsoleApplication
{
    class Program
    {
        private static ILog NormalLogger { get; set; }

        //ProxyLogger use ProxyCassandraClientFactory. This factory provide a proxy Cassandra Client that can retry any Cassandra command that raise error.
        private static ILog ProxyLogger { get; set; }

        static void Main(string[] args)
        {
            ConfigureLogger();

            NormalLogger.Info("Hello World");
        }

        private static void ConfigureLogger()
        {
            log4net.Config.XmlConfigurator.Configure();

            NormalLogger = log4net.LogManager.GetLogger("Normal");
            ProxyLogger = log4net.LogManager.GetLogger("Proxy");
        }
    }
}