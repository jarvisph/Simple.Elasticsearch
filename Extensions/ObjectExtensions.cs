using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;

namespace Simple.Core.Extensions
{
    public static class ObjectExtensions
    {
        /// <summary>
        /// 将给定对象转换为不同类型
        /// </summary>
        /// <param name="obj"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns>Converted object</returns>
        public static T ToValue<T>(this object obj)
        {
            if (typeof(T) == typeof(Guid))
            {
                if (obj == null)
                {
                    obj = Guid.Empty;
                }
                return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromInvariantString(obj.ToString());
            }
            return (T)Convert.ChangeType(obj, typeof(T), CultureInfo.InvariantCulture);
        }
        public static string GetString(this object obj)
        {
            if (obj.GetType().IsArray)
            {
                Array array = (Array)obj;
                List<object> list = new List<object>();
                for (int i = 0; i < array.Length; i++)
                {
                    list.Add(array.GetValue(i));
                }
                return string.Join(",", list);
            }
            else
            {
                return obj.ToString();
            }
        }

        public static object ToValue(this object obj, Type type)
        {
            return Convert.ChangeType(obj, type, CultureInfo.InvariantCulture);
        }
        /// <summary>
        /// 判断是否标记特性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool HasAttribute<T>(this object obj) where T : Attribute
        {
            ICustomAttributeProvider custom = obj is ICustomAttributeProvider ? (ICustomAttributeProvider)obj : obj.GetType();
            foreach (var t in custom.GetCustomAttributes(false))
            {
                if (t.GetType().Equals(typeof(T))) return true;
            }
            return false;
        }
        /// <summary>
        /// 获取特性类
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static T GetAttribute<T>(this object obj)
        {
            if (obj == null) return default(T);
            ICustomAttributeProvider custom = obj is ICustomAttributeProvider ? (ICustomAttributeProvider)obj : obj.GetType();
            foreach (var item in custom.GetCustomAttributes(true))
            {
                if (item.GetType().Equals(typeof(T))) return (T)item;
            }
            return default(T);
        }
    }
}
