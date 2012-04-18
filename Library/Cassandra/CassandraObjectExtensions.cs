/// <summary>
/// This class was extracted from Cassandraemon v1.0.6 project (https://cassandraemon.codeplex.com/)
/// </summary>

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace CassandraLog4NetAppenderLibrary.Cassandra
{
    public static class ObjectExtensions
    {
        public static byte[] ToCassandraByte(this bool source)
        {
            return BigEndianBitConverter.GetBytes(source);
        }

        public static byte[] ToCassandraByte(this int source)
        {
            return BigEndianBitConverter.GetBytes(source);
        }

        public static byte[] ToCassandraByte(this long source)
        {
            return BigEndianBitConverter.GetBytes(source);
        }

        public static byte[] ToCassandraByte(this float source)
        {
            return BigEndianBitConverter.GetBytes(source);
        }

        public static byte[] ToCassandraByte(this double source)
        {
            return BigEndianBitConverter.GetBytes(source);
        }

        public static byte[] ToCassandraByte(this DateTime source)
        {
            return BigEndianBitConverter.GetBytes(source.Ticks);
        }

        public static byte[] ToCassandraByte(this TimeSpan source)
        {
            return BigEndianBitConverter.GetBytes(source.Ticks);
        }

        public static byte[] ToCassandraByte(this string source)
        {
            if (source == null) return null;
            return Encoding.UTF8.GetBytes((string)source);
        }

        public static byte[] ToCassandraByte(this CassandraBinary source)
        {
            if (source == null) return null;
            return source.Value;
        }

        public static byte[] ToCassandraByte(this object source)
        {
            if (source == null) return null;

            var type = source.GetType();
            var typeCode = Type.GetTypeCode(type);
            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return BigEndianBitConverter.GetBytes((bool)source);
                case TypeCode.Byte:
                    return new[] { (byte)source };
                case TypeCode.Char:
                    return BigEndianBitConverter.GetBytes((char)source);
                case TypeCode.DBNull:
                    break;
                case TypeCode.DateTime:
                    return BigEndianBitConverter.GetBytes(((DateTime)source).Ticks);
                case TypeCode.Decimal:
                    break;
                case TypeCode.Double:
                    return BigEndianBitConverter.GetBytes((double)source);
                case TypeCode.Empty:
                    break;
                case TypeCode.Int16:
                    return BigEndianBitConverter.GetBytes((short)source);
                case TypeCode.Int32:
                    return BigEndianBitConverter.GetBytes((int)source);
                case TypeCode.Int64:
                    return BigEndianBitConverter.GetBytes((long)source);
                case TypeCode.Object:
                    break;
                case TypeCode.SByte:
                    return new[] { (byte)(sbyte)source };
                case TypeCode.Single:
                    return BigEndianBitConverter.GetBytes((float)source);
                case TypeCode.String:
                    return Encoding.UTF8.GetBytes((string)source);
                case TypeCode.UInt16:
                    return BigEndianBitConverter.GetBytes((ushort)source);
                case TypeCode.UInt32:
                    return BigEndianBitConverter.GetBytes((uint)source);
                case TypeCode.UInt64:
                    return BigEndianBitConverter.GetBytes((ulong)source);
                default:
                    break;
            }
            if (source is byte[])
            {
                return source as byte[];
            }
            else if (source is sbyte[])
            {
                return (source as sbyte[]).Cast<byte>().ToArray();
            }
            else if (source is CassandraBinary)
            {
                return (source as CassandraBinary).Value;
            }
            else if (source is TimeSpan)
            {
                return BigEndianBitConverter.GetBytes(((TimeSpan)source).Ticks);
            }
            else if (source is Guid)
            {
                return ((Guid)source).ToByteArray();
            }
            else
            {
                if (source.GetType().IsSerializable)
                {
                    MemoryStream stream;
                    using (stream = new MemoryStream())
                    {
                        BinaryFormatter bf = new BinaryFormatter();
                        bf.Serialize(stream, source);
                    }
                    return stream.ToArray();
                }
                else
                {
                    throw new ArgumentException("Don't specify unserializable data.");
                }
            }
        }
    }
}