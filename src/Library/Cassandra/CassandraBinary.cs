// -----------------------------------------------------------------------
// <copyright company="Cassandraemon">
//     This class was extracted from Cassandraemon v1.0.6 project (https://cassandraemon.codeplex.com/)
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

namespace CassandraLog4NetAppenderLibrary.Cassandra
{
    [Serializable]
    public sealed class CassandraBinary : IEquatable<CassandraBinary>, IComparable<CassandraBinary>, IComparable
    {
        public byte[] Value { get; private set; }

        public CassandraBinary() { }

        public CassandraBinary(byte[] value)
        {
            Value = value;
        }

        public static implicit operator CassandraBinary(byte[] value)
        {
            return new CassandraBinary(value);
        }

        public static implicit operator byte[](CassandraBinary value)
        {
            if (object.ReferenceEquals(value, null)) { return null; }
            return value.Value;
        }

        public static bool operator ==(CassandraBinary value1, CassandraBinary value2)
        {
            return EqualsBinary(value1, value2);
        }

        public static bool operator ==(CassandraBinary value1, object value2)
        {
            return EqualsBinary(value1, value2);
        }

        public static bool operator ==(object value1, CassandraBinary value2)
        {
            return EqualsBinary(value1, value2);
        }

        public static bool operator !=(CassandraBinary value1, CassandraBinary value2)
        {
            return !EqualsBinary(value1, value2);
        }

        public static bool operator !=(CassandraBinary value1, object value2)
        {
            return !EqualsBinary(value1, value2);
        }

        public static bool operator !=(object value1, CassandraBinary value2)
        {
            return !EqualsBinary(value1, value2);
        }

        public static bool operator >=(CassandraBinary value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) >= 0 ? true : false;
        }

        public static bool operator >=(CassandraBinary value1, object value2)
        {
            return CompareToBinary(value1, value2) >= 0 ? true : false;
        }

        public static bool operator >=(object value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) >= 0 ? true : false;
        }

        public static bool operator <=(CassandraBinary value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) <= 0 ? true : false;
        }

        public static bool operator <=(CassandraBinary value1, object value2)
        {
            return CompareToBinary(value1, value2) <= 0 ? true : false;
        }

        public static bool operator <=(object value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) <= 0 ? true : false;
        }

        public static bool operator >(CassandraBinary value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) > 0 ? true : false;
        }

        public static bool operator >(CassandraBinary value1, object value2)
        {
            return CompareToBinary(value1, value2) > 0 ? true : false;
        }

        public static bool operator >(object value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) > 0 ? true : false;
        }

        public static bool operator <(CassandraBinary value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) < 0 ? true : false;
        }

        public static bool operator <(CassandraBinary value1, object value2)
        {
            return CompareToBinary(value1, value2) < 0 ? true : false;
        }

        public static bool operator <(object value1, CassandraBinary value2)
        {
            return CompareToBinary(value1, value2) < 0 ? true : false;
        }

        public bool Equals(CassandraBinary binary)
        {
            return EqualsBinary(Value, binary);
        }

        public override bool Equals(object obj)
        {
            if (obj == null) return base.Equals(obj);

            if (obj is CassandraBinary)
            {
                return Equals(obj as CassandraBinary);
            }
            else
            {
                throw new InvalidCastException("The 'obj' argument is not a CassandraBinary object.");
            }
        }

        private static bool EqualsBinary(object obj1, object obj2)
        {
            if (object.ReferenceEquals(obj1, obj2)) { return true; }
            if (obj1 == null || obj2 == null) { return false; }
            return EqualsBinary(obj1.ToCassandraByte(), obj2.ToCassandraByte());
        }

        private static bool EqualsBinary(byte[] x, byte[] y)
        {
            if (x == null || y == null) return false;
            if (x.Length != y.Length) return false;

            return x.SequenceEqual(y);
        }

        public override int GetHashCode()
        {
            if (Value == null) throw new InvalidOperationException("'Value' property is null in GetHashCode method.");

            int sum = 0;

            foreach (var b in Value)
            {
                sum = 33 * sum + b;
            }

            return sum;
        }

        public int CompareTo(CassandraBinary binary)
        {
            return CompareToBinary(Value, binary);
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;

            if (obj is CassandraBinary)
            {
                return CompareTo(obj as CassandraBinary);
            }
            else
            {
                throw new InvalidCastException("The 'obj' argument is not a CassandraBinary object.");
            }
        }

        private static int CompareToBinary(object obj1, object obj2)
        {
            return CompareToBinary(obj1.ToCassandraByte(), obj2.ToCassandraByte());
        }

        private static int CompareToBinary(byte[] x, byte[] y)
        {
            if (x == null) return -1;
            if (y == null) return 1;

            int minLength = Math.Min(x.Length, y.Length);

            for (int i = 0; i < minLength; i++)
            {
                if (x[i] == y[i]) continue;

                return x[i].CompareTo(y[i]);
            }

            if (x.Length == y.Length) return 0;

            if (x.Length > y.Length)
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }
    }
}