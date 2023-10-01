using System;
using System.Collections.Generic;
using System.Globalization;

namespace Simple.Core.Extensions
{
    internal static class TypeExtensions
    {
        public static object GetDefaultValue(this object value, Type type)
        {
            return type.Name switch
            {
                "String" => value ?? string.Empty,
                "DateTime" => ((DateTime)value).Max(),
                _ => value
            };
        }
        /// 获取type默认值，string 默认是empty
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object GetDefaultValue(this Type type)
        {
            object value = type.Name switch
            {
                nameof(Int16) => (short)0,
                nameof(Int32) => 0,
                nameof(Int64) => 0L,
                nameof(Double) => 0D,
                nameof(Decimal) => 0M,
                nameof(Single) => 0F,
                nameof(Boolean) => false,
                nameof(DateTime) => new DateTime(1900, 1, 1),
                "String[]" or "Int16[]" or "Int32[]" or "Int64[]" or "Double[]" or "Decimal[]" or "Single[]" => Array.CreateInstance(type, 0),
                nameof(Guid) => Guid.Empty,
                nameof(String) => string.Empty,
                _ => throw new NotSupportedException(type.Name)
            };

            return value;
        }
        /// <summary>
        /// 获取安全类型
        /// </summary>
        /// <param name="value">来源类型</param>
        /// <param name="type">转换的类型</param>
        /// <returns></returns>
        public static object GetValue(this object value, Type type)
        {
            if (type == null) throw new ArgumentNullException();
            if (value == null || value == DBNull.Value) return type.GetDefaultValue();
            object defaultValue = type.GetDefaultValue();
            if (type.IsGenericType) type = type.GenericTypeArguments[0];
            switch (value.GetType().Name)
            {
                case "String":
                    {
                        string val = (string)value;
                        if (!string.IsNullOrWhiteSpace(val))
                        {
                            switch (type.Name)
                            {
                                case "Int16":
                                    defaultValue = short.TryParse(val, out short shortVal) ? shortVal : defaultValue;
                                    break;
                                case "Int32":
                                    defaultValue = int.TryParse(val, out int intValue) ? intValue : defaultValue;
                                    break;
                                case "Int64":
                                    defaultValue = long.TryParse(val, out long longValue) ? longValue : defaultValue;
                                    break;
                                case "Boolean":
                                    defaultValue = val.Equals("1") || val.Equals("true", StringComparison.CurrentCultureIgnoreCase) || val.Equals("on", StringComparison.CurrentCultureIgnoreCase) || val.Equals("yes", StringComparison.CurrentCultureIgnoreCase);
                                    break;
                                case "Decimal":
                                    defaultValue = decimal.TryParse(val, out decimal decimalValue) ? decimalValue : defaultValue;
                                    break;
                                case "Double":
                                    defaultValue = double.TryParse(val, out double doubleValue) ? doubleValue : defaultValue;
                                    break;
                                case "Single":
                                    defaultValue = float.TryParse(val, out float floatValue) ? floatValue : defaultValue;
                                    break;
                                case "Byte":
                                    defaultValue = byte.TryParse(val, out byte byteValue) ? byteValue : defaultValue;
                                    break;
                                case "DateTime":
                                    defaultValue = DateTime.TryParse(val, out DateTime datetimeValue) ? datetimeValue : defaultValue;
                                    break;
                                case "String":
                                    defaultValue = val;
                                    break;
                                case "Guid":
                                    defaultValue = Guid.TryParse(val, out Guid guidValue) ? guidValue : defaultValue;
                                    break;
                                case "Int16[]":
                                case "Int32[]":
                                case "Int64[]":
                                case "Boolean[]":
                                case "Decimal[]":
                                case "Double[]":
                                case "Single[]":
                                case "Byte[]":
                                    {
                                        bool array = val.StartsWith("[") && val.EndsWith("]");
                                        if (array)
                                        {
                                            //defaultValue = JsonConvert.DeserializeObject(val, type);
                                        }
                                        else
                                        {
                                            switch (type.Name)
                                            {
                                                case "Int16[]":
                                                    defaultValue = val.GetArray(typeof(short));
                                                    break;
                                                case "Int32[]":
                                                    defaultValue = val.GetArray(typeof(int));
                                                    break;
                                                case "Int64[]":
                                                    defaultValue = val.GetArray(typeof(long));
                                                    break;
                                                case "Boolean[]":
                                                    defaultValue = val.GetArray(typeof(bool));
                                                    break;
                                                case "Decimal[]":
                                                    defaultValue = val.GetArray(typeof(decimal));
                                                    break;
                                                case "Double[]":
                                                    defaultValue = val.GetArray(typeof(double));
                                                    break;
                                                case "Single[]":
                                                    defaultValue = val.GetArray(typeof(float));
                                                    break;
                                                case "Byte[]":
                                                    defaultValue = val.GetArray(typeof(byte));
                                                    break;
                                            }

                                        }
                                    }
                                    break;
                                default:
                                    if (type.IsEnum)
                                    {
                                        defaultValue = Enum.Parse(type, val);
                                    }
                                    break;

                            }
                        }
                    }
                    break;
                default:
                    try
                    {
                        defaultValue = Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
                    }
                    finally
                    {

                    }
                    break;
            }
            return defaultValue;
        }

        public static T GetValue<T>(this object value)
        {
            return (T)value.GetValue(typeof(T));
        }

        public static bool IsType<T>(this object value)
        {
            if (value == null) return false;
            if (value is T) return true;
            return false;
        }

        internal static Type GetElementType(this Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null) return seqType;
            return ienum.GetGenericArguments()[0];
        }
        private static Type FindIEnumerable(this Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;
            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }
            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }
            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }
            return null;
        }
    }
}
