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
        private static ILog Logger { get; set; }

        static void Main(string[] args)
        {
            ConfigureLogger();

            Logger.Info("TEST");
        }

        private static void ConfigureLogger()
        {
            log4net.Config.XmlConfigurator.Configure();
            Logger = log4net.LogManager.GetLogger("WhiteBoard");
        }
    }
}