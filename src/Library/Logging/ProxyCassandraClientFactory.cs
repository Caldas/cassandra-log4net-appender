using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;
using System.Threading;
using Apache.Cassandra;
using CassandraLog4NetAppenderLibrary.Cassandra;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace CassandraLog4NetAppenderLibrary.Logging
{
    public class ProxyCassandraClientFactory : ICassandraClientFactory
    {
        public Apache.Cassandra.Cassandra.Iface CreateConnection(String host, Int32 port)
        {
            return CassandraClientProxy.NewProxyConnection(host, port);
        }
    }

    public class CassandraClientProxy : RealProxy
    {
        private String host;
        private int port;
        private String lastUsedHost;
        private long lastPoolCheck;
        private List<TokenRange> ring;
        private Apache.Cassandra.Cassandra.Client client;
        private String ringKs;
        private CircuitBreaker breaker = new CircuitBreaker(1, 1);
        private int lastUsedConnIndex;
        private int maxAttempts = 10;
        private Random random = new Random();

        public static Apache.Cassandra.Cassandra.Iface NewProxyConnection(String host, Int32 port)
        {
            CassandraClientProxy myProxy = new CassandraClientProxy(host, port);
            return (Apache.Cassandra.Cassandra.Iface)myProxy.GetTransparentProxy();
        }

        private CassandraClientProxy(String host, Int32 port) : base(typeof(Apache.Cassandra.Cassandra.Iface))
        {
            this.host = host;
            this.port = port;
            this.lastUsedHost = host;
            this.lastUsedConnIndex = 0;
            this.lastPoolCheck = 0L;

            Initialize();
        }

        public override ObjRef CreateObjRef(Type requestedType)
        {
            throw new NotSupportedException();
        }

        public override IMessage Invoke(IMessage message)
        {
            IMethodCallMessage methodMessage = new MethodCallMessageWrapper((IMethodCallMessage)message);

            // Obtain the actual method definition that is being called.
            MethodBase methodBase = methodMessage.MethodBase;
            Object[] args = methodMessage.Args;

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
                        result = methodBase.Invoke(this.client, args);

                        if ((methodBase.Name.ToLower().Equals("set_keyspace")) && (args.Length == 1))
                            this.ringKs = args[0].ToString();

                        this.breaker.Success();

                        return new ReturnMessage(result, methodMessage.Args, methodMessage.ArgCount, methodMessage.LogicalCallContext, methodMessage);
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

        private void AttemptReconnect()
        {
            if ((this.ring == null) || (this.ring.Count == 0))
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

            Int32 randomIndex = this.random.Next(0, this.ring.Count - 1);
            List<String> endpoints = this.ring[randomIndex].Rpc_endpoints;

            while (!endpoint.Equals(host))
            {
                Int32 index = this.lastUsedConnIndex;

                endpoint = (String)endpoints[index];

                index++;

                if (index == endpoints.Count)
                    index = 0;
            }
            return endpoint;
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
    }
}
