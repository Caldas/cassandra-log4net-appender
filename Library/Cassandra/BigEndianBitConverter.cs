/// <summary>
/// This class was extracted from Cassandraemon v1.0.6 project (https://cassandraemon.codeplex.com/)
/// </summary>

using System;
using System.Linq;

namespace CassandraLog4NetAppenderLibrary.Cassandra
{
    //
    public static class BigEndianBitConverter
    {
        private static byte[] ConvertEndian(byte[] value)
        {
            return BitConverter.IsLittleEndian ? value.Reverse().ToArray() : value;
        }

        public static byte[] GetBytes(bool value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(char value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(double value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(short value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(int value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(long value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(float value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(ushort value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(uint value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static byte[] GetBytes(ulong value)
        {
            return ConvertEndian(BitConverter.GetBytes(value));
        }

        public static bool ToBoolean(byte[] value, int startIndex)
        {
            return BitConverter.ToBoolean(ConvertEndian(value), startIndex);
        }

        public static char ToChar(byte[] value, int startIndex)
        {
            return BitConverter.ToChar(ConvertEndian(value), startIndex);
        }

        public static double ToDouble(byte[] value, int startIndex)
        {
            return BitConverter.ToDouble(ConvertEndian(value), startIndex);
        }

        public static short ToInt16(byte[] value, int startIndex)
        {
            return BitConverter.ToInt16(ConvertEndian(value), startIndex);
        }

        public static int ToInt32(byte[] value, int startIndex)
        {
            return BitConverter.ToInt32(ConvertEndian(value), startIndex);
        }

        public static long ToInt64(byte[] value, int startIndex)
        {
            return BitConverter.ToInt64(ConvertEndian(value), startIndex);
        }

        public static float ToSingle(byte[] value, int startIndex)
        {
            return BitConverter.ToSingle(ConvertEndian(value), startIndex);
        }

        public static ushort ToUInt16(byte[] value, int startIndex)
        {
            return BitConverter.ToUInt16(ConvertEndian(value), startIndex);
        }

        public static uint ToUInt32(byte[] value, int startIndex)
        {
            return BitConverter.ToUInt32(ConvertEndian(value), startIndex);
        }

        public static ulong ToUInt64(byte[] value, int startIndex)
        {
            return BitConverter.ToUInt64(ConvertEndian(value), startIndex);
        }

        public static string ToString(byte[] value)
        {
            return BitConverter.ToString(ConvertEndian(value));
        }

        public static string ToString(byte[] value, int startIndex)
        {
            return BitConverter.ToString(ConvertEndian(value), startIndex);
        }

        public static string ToString(byte[] value, int startIndex, int length)
        {
            return BitConverter.ToString(ConvertEndian(value), startIndex, length);
        }

    }
}