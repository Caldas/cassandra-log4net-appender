# Cassandra Log4Net Appender

## Log4Net Appender to store your logs on Cassandra NoSQL
	
### Copyright (C) 2012 [F�bio Caldas](https://github.com/Caldas) / [Vtex](http://www.vtex.com.br)

## Contents
========

1. Description
2. Requirements
3. Using Cassandra Log4Net Appender
4. Copyright

### 1. Description
==============

This project is a c# port of [Log4J DataStax Cassandra Appender](http://www.datastax.com/docs/datastax_enterprise2.0/logging/log4j_logging) and it was developed inspired by [Sdolgy - Cassandra Log4j Appender](https://github.com/sdolgy/cassandra-log4j-appender/) 

A NuGet version is avaliable at: https://nuget.org/packages/CassandraLog4NetAppender

### 2. Requirements
==============

* Windows XP or greater
* Visual Studio 2010
* Net 4.0
* NuGet Package: log4net

### 3. Using Cassandra Log4Net Appender
==============

* Add at configSections
	&lt;section name="log4net" type="log4net.Config.Log4NetConfigurationSectionHandler, log4net" /&gt;

* Add Log4Net section 
<pre><code>
	&lt;log4net&gt;
		&lt;root&gt;
			&lt;level value="ALL" /&gt;
			&lt;appender-ref ref="CassandraAppender" /&gt;
		&lt;/root&gt;
		&lt;logger name="YOUR_LOGGER_NAME"&gt;
			&lt;appender-ref ref="CassandraAppender" /&gt;
		&lt;/logger&gt;
		&lt;appender name="CassandraAppender" type="CassandraLog4NetAppenderLibrary.Logging.Appender.CassandraAppender, CassandraLog4NetAppenderLibrary"&gt;
			&lt;mapping&gt;
				&lt;level value="ALL" /&gt;
				&lt;Hosts value="YOUR_HOST_NAME" /&gt;
				&lt;Port value="YOUR_HOST_PORT" /&gt;
				&lt;AppName value="SampleConsoleApplication" /&gt;
				&lt;KeyspaceName value="Logging" /&gt;
				&lt;ColumnFamily value="LogEntries" /&gt;
				&lt;PlacementStrategy value="org.apache.cassandra.locator.NetworkTopologyStrategy" /&gt;
				&lt;StrategyOptions value="Cassandra:1" /&gt;
				&lt;ReplicationFactor value="1" /&gt;
				&lt;ConsistencyLevelWrite value="QUORUM" /&gt;
				&lt;MaxBufferedRows value="1" /&gt;
			&lt;/mapping&gt;
		&lt;/appender&gt;
	&lt;/log4net&gt;
<code></pre>

* Configure and use Log4Net object

	log4net.Config.XmlConfigurator.Configure();
	
    var Logger = log4net.LogManager.GetLogger("YOUR_LOGGER_NAME");

### 4. Copyright
==============

Cassandra Log4Net Appender - Log4Net Appender to store your logs on Cassandra NoSQL
Copyright (C) 2012 [F�bio Caldas](https://github.com/Caldas) / [Vtex](http://www.vtex.com.br) &lt;fabio.caldas@vtex.com.br&gt;

This library is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this library. If not, see &lt;http://www.gnu.org/licenses/&gt;.