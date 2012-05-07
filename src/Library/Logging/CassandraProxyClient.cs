using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift.Transport;
using Thrift.Protocol;
using System.Threading;
using CassandraLog4NetAppenderLibrary.Cassandra;
using Thrift;
using System.Reflection;

namespace CassandraLog4NetAppenderLibrary.Logging
{
    public class CassandraProxyClient
    {
        private String host;
        private int port;
        private ConnectionStrategy connectionStrategy;
        private String lastUsedHost;
        private long lastPoolCheck;
        private List<TokenRange> ring;
        private Apache.Cassandra.Cassandra.Client client;
        private String ringKs;
        private CircuitBreaker breaker = new CircuitBreaker(1, 1);
        private Random random;
        private int lastUsedConnIndex;
        private int maxAttempts = 10;

        public static Apache.Cassandra.Cassandra.Iface newProxyConnection(String host, int port, ConnectionStrategy connectionStrategy)
        {
            //TODO: Implement LinFU Proxy
            throw new NotImplementedException();
            //return (Apache.Cassandra.Cassandra.Iface)Proxy.newProxyInstance(Cassandra.Client.class.getClassLoader(), Cassandra.Client.class.getInterfaces(), new CassandraProxyClient(host, port, connectionStrategy));
        }

        private Apache.Cassandra.Cassandra.Client CreateConnection(String host)
        {
            TSocket socket = new TSocket(host, this.port);
            TTransport trans = new TFramedTransport(socket);
            try
            {
                trans.Open();
            }
            catch (TTransportException exception)
            {
                throw new Exception("unable to connect to server", exception);
            }

            Apache.Cassandra.Cassandra.Client client = new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(trans));
            if (this.ringKs != null)
            {
                try
                {
                    client.set_keyspace(this.ringKs);
                }
                catch (Exception exception)
                {
                    throw exception;
                }
            }

            return client;
        }

        private CassandraProxyClient(String host, int port, ConnectionStrategy connectionStrategy)
        {
            this.host = host;
            this.port = port;
            this.connectionStrategy = connectionStrategy;
            this.lastUsedHost = host;

            if (connectionStrategy == ConnectionStrategy.RANDOM)
                this.random = new Random();
            else
                this.lastUsedConnIndex = 0;

            this.lastPoolCheck = 0L;

            Initialize();
        }

        private void Initialize()
        {
            int attempt = 0;
            while (attempt++ < maxAttempts)
            {
                AttemptReconnect();
                if (this.client != null)
                {
                    break;
                }
                Thread.Sleep(1050);
            }

            if (this.client == null)
                throw new Exception("Error connecting to node " + this.lastUsedHost);

            try
            {
                List<KsDef> allKs = this.client.describe_keyspaces();

                if ((allKs.Count == 0) || ((allKs.Count == 1) && (((KsDef)allKs[0]).Name.ToLower().Equals("system"))))
                    allKs.Add(CreateTmpKs());

                foreach (KsDef ks in allKs)
                {
                    if (!ks.Name.ToLower().Equals("system"))
                    {
                        this.ringKs = ks.Name;
                        break;
                    }
                }
            }
            catch (Exception exception)
            {
                throw exception;
            }

            CheckRing();
        }

        private KsDef CreateTmpKs()
        {
            KsDef tmpKs = new KsDef();
            tmpKs.Name = "proxy_client_ks";
            tmpKs.Strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            tmpKs.Strategy_options = new Dictionary<String, String>();
            tmpKs.Strategy_options.Add("replication_factor", "1");

            this.client.system_add_keyspace(tmpKs);

            return tmpKs;
        }

        private void CheckRing()
        {
            if (this.client == null)
            {
                this.breaker.Failure();
                return;
            }

            long now = TimeGenerator.GetUnixTime();

            if (now - this.lastPoolCheck > 60000L)
            {
                try
                {
                    if (this.breaker.Allow())
                    {
                        this.ring = this.client.describe_ring(this.ringKs);
                        this.lastPoolCheck = now;

                        if (this.connectionStrategy == ConnectionStrategy.ROUND_ROBIN)
                            this.lastUsedConnIndex = 0;

                        this.breaker.Success();
                    }
                }
                catch (TApplicationException)
                {
                    this.breaker.Failure();
                    AttemptReconnect();
                }
                catch (InvalidRequestException exception)
                {
                    throw exception;
                }
            }
        }

        private String GetNextServer(String host)
        {
            String endpoint = host;

            if (this.connectionStrategy == ConnectionStrategy.STICKY)
                return endpoint;

            Int32 randomIndex = this.random.Next(0, this.ring.Count - 1);
            List<String> endpoints = this.ring[randomIndex].Rpc_endpoints;

            while (!endpoint.Equals(host))
            {
                Int32 index = this.lastUsedConnIndex;

                if (this.connectionStrategy == ConnectionStrategy.RANDOM)
                    index = this.random.Next(0, endpoints.Count - 1);

                endpoint = (String)endpoints[index];

                if (this.connectionStrategy == ConnectionStrategy.ROUND_ROBIN)
                {
                    index++;

                    if (index == endpoints.Count)
                        index = 0;
                }
            }
            return endpoint;
        }

        private void AttemptReconnect()
        {
            if ((this.connectionStrategy == ConnectionStrategy.STICKY) || (this.ring == null) || (this.ring.Count == 0))
            {
                try
                {
                    this.client = CreateConnection(this.lastUsedHost);
                    this.breaker.Success();
                    return;
                }
                catch (Exception e)
                {
                }
            }

            if ((this.ring == null) || (this.ring.Count == 0))
            {
                this.client = null;
                return;
            }

            if (this.ring.Count == 1)
            {
                this.client = null;
                return;
            }

            String endpoint = GetNextServer(this.lastUsedHost);
            try
            {
                this.client = CreateConnection(endpoint);
                this.lastUsedHost = endpoint;
                this.breaker.Success();
                CheckRing();
            }
            catch (Exception)
            {
                this.client = null;
            }
        }

        public Object Invoke(Object proxy, System.Reflection.MethodBase m, Object[] args)
        {
            Object result = null;

            int tries = 0;

            if (this.ring == null)
                CheckRing();

            while ((result == null) && (tries++ < maxAttempts))
            {
                if (this.client == null)
                    this.breaker.Failure();

                try
                {
                    if (this.breaker.Allow())
                    {
                        result = m.Invoke(this.client, args);

                        if ((m.Name.ToLower().Equals("set_keyspace")) && (args.Length == 1))
                            this.ringKs = args[0].ToString();

                        this.breaker.Success();
                        return result;
                    }

                    while (!this.breaker.Allow())
                    {
                        Thread.Sleep(1050);
                    }
                    AttemptReconnect();

                    if (this.client != null)
                        tries--;
                }
                catch (TargetInvocationException e)
                {
                    Type exceptionType = e.GetType();
                    if (exceptionType == typeof(UnavailableException) || exceptionType == typeof(TimedOutException) || exceptionType == typeof(TTransportException))
                    {
                        this.breaker.Failure();

                        if (tries >= maxAttempts)
                            throw e;
                    }
                }
                catch (Exception exception)
                {
                    throw exception;
                }
            }

            throw new UnavailableException();
        }
    }
}