/// <summary>
/// This class was extracted from Cassandraemon v1.0.6 project (https://cassandraemon.codeplex.com/)
/// </summary>

using System;
using System.Collections.Generic;
using System.Threading;

namespace CassandraLog4NetAppenderLibrary.Cassandra
{
    public static class TimeGenerator
    {
        static readonly DateTime Epoch = new DateTime(1970, 1, 1);

        static readonly DateTime GregorianStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        static readonly object lockObject = new object();

        static byte[] node = new byte[6];

        static long lastTicks = 0;

        static int tickOffset = 0;

        static int precision = 0;

        static TimeGenerator()
        {
            Random random = new Random();
            random.NextBytes(node);

            MeasurePrecision();
        }

        private static void MeasurePrecision()
        {
            var timeList = new List<string>();

            for (int i = 0; i < 1000; i++)
            {
                timeList.Add(DateTime.UtcNow.Ticks.ToString());
            }

            Thread.Sleep(100);

            for (int i = 0; i < 1000; i++)
            {
                timeList.Add(DateTime.UtcNow.Ticks.ToString());
            }

            string first = timeList[0];
            for (int i = 0; i < first.Length; i++)
            {
                string firstEndWith = first.Substring(first.Length - 1 - i);

                if (!timeList.TrueForAll(x => x.EndsWith(firstEndWith)))
                {
                    precision = Convert.ToInt32(Math.Pow(10, i));
                    return;
                }
            }
        }

        public static long GetUnixTime()
        {
            return GetUnixTime(DateTime.UtcNow);
        }

        public static long GetUnixTime(DateTime datetime)
        {
            return Convert.ToInt64((datetime - Epoch).Ticks / 10);
        }

        public static Guid GetTimeUUID()
        {
            return GetTimeUUID(GetSensitiveTicks()); // ticks is millisecond interval, so add offset.
        }

        public static Guid GetTimeUUID(DateTime datetime)
        {
            return GetTimeUUID(datetime.Ticks);
        }

        public static Guid GetTimeUUID(long ticks)
        {
            ticks = ticks - GregorianStart.Ticks;

            byte[] guid = new byte[16];
            byte[] clockSeq = BigEndianBitConverter.GetBytes(Convert.ToInt16(Environment.TickCount % Int16.MaxValue));
            byte[] timestamp = BigEndianBitConverter.GetBytes(ticks);

            Array.Copy(node, 0, guid, 10, node.Length);
            Array.Copy(clockSeq, 0, guid, 8, clockSeq.Length);
            Array.Copy(timestamp, 4, guid, 0, 4);
            Array.Copy(timestamp, 2, guid, 4, 2);
            Array.Copy(timestamp, 0, guid, 6, 2);

            // set variant
            guid[8] &= (byte)0x3f;
            guid[8] |= (byte)0x80;

            // set version
            guid[6] &= (byte)0x0f;
            guid[6] |= (byte)((byte)0x01 << 4);

            // set node high order bit 1
            guid[10] |= (byte)0x80;

            return new Guid(guid);
        }

        public static DateTime GetDateTime(Guid timeUUID)
        {
            return GetDateTime(timeUUID, DateTimeKind.Unspecified);
        }

        public static DateTime GetDateTimeUtc(Guid timeUUID)
        {
            return GetDateTime(timeUUID, DateTimeKind.Utc);
        }

        public static DateTime GetDateTimeLocal(Guid timeUUID)
        {
            return GetDateTime(timeUUID, DateTimeKind.Local);
        }

        public static DateTime GetDateTime(Guid timeUUID, DateTimeKind kind)
        {
            byte[] uuidByte = timeUUID.ToByteArray();
            byte[] ticksByte = new byte[8];

            uuidByte[6] &= (byte)0x0f;

            Array.Copy(uuidByte, 6, ticksByte, 0, 2);
            Array.Copy(uuidByte, 4, ticksByte, 2, 2);
            Array.Copy(uuidByte, 0, ticksByte, 4, 4);

            long ticks = BigEndianBitConverter.ToInt64(ticksByte, 0);
            ticks = ticks + GregorianStart.Ticks;

            return new DateTime(ticks, kind);
        }

        public static DateTime GetDateTime(long unixTime)
        {
            return GetDateTime(unixTime, DateTimeKind.Unspecified);
        }

        public static DateTime GetDateTimeUtc(long unixTime)
        {
            return GetDateTime(unixTime, DateTimeKind.Utc);
        }

        public static DateTime GetDateTime(long unixTime, DateTimeKind kind)
        {
            return new DateTime(unixTime * 10, kind).AddYears(1969);
        }

        public static long GetSensitiveTicks()
        {
            long ticks = DateTime.UtcNow.Ticks;
            decimal d = new decimal(ticks);

            d = decimal.Divide(d, precision);
            d = decimal.Floor(d);
            d = decimal.Multiply(d, precision);

            lock (lockObject)
            {
                if (lastTicks == ticks)
                {
                    d = decimal.Add(d, ++tickOffset % precision);
                }
                else
                {
                    lastTicks = ticks;
                    tickOffset = 0;
                }
            }

            return decimal.ToInt64(d);
        }
    }
}