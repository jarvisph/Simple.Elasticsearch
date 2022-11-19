using System;
using System.Collections.Generic;

namespace Simple.Core.Extensions
{
    public static class CollectionExtensions
    {
        /// <summary>
        /// 是否为空
        /// </summary>
        public static bool IsNullOrEmpty<T>(this ICollection<T> source)
        {
            return source == null || source.Count <= 0;
        }

        /// <summary>
        /// 检查并添加集合
        /// </summary>
        /// <param name="source"></param>
        /// <param name="item"></param>
        /// <typeparam name="T">集合中项目的类型</typeparam>
        /// <returns>添加时返回True，不添加时返回False.</returns>
        public static bool AddIfNotContains<T>(this ICollection<T> source, T item)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (source.Contains(item))
            {
                return false;
            }
            source.Add(item);
            return true;
        }
        /// <summary>
        /// 获取字典中的值，无则返回false
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static bool TryGetValue<T>(this IDictionary<string, object> dictionary, string key, out T value)
        {
            object valueObj;
            if (dictionary.TryGetValue(key, out valueObj) && valueObj is T)
            {
                value = (T)valueObj;
                return true;
            }

            value = default(T);
            return false;
        }

        /// <summary>
        /// 找到不key值，赋默认值
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static TValue GetOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
        {
            TValue obj;
            return dictionary.TryGetValue(key, out obj) ? obj : default(TValue);
        }
        /// <summary>
        /// 获取key，指定默认值
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static TValue GetOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }
            return value;
        }
        /// <summary>
        /// Gets a value from the dictionary with given key. Returns default value if can not find.
        /// </summary>
        /// <param name="dictionary">Dictionary to check and get</param>
        /// <param name="key">Key to find the value</param>
        /// <param name="factory">A factory method used to create the value if not found in the dictionary</param>
        /// <typeparam name="TKey">Type of the key</typeparam>
        /// <typeparam name="TValue">Type of the value</typeparam>
        /// <returns>Value if found, default if can not found.</returns>
        public static TValue GetOrAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        {
            TValue obj;
            if (dictionary.TryGetValue(key, out obj))
            {
                return obj;
            }

            return dictionary[key] = factory(key);
        }

        /// <summary>
        /// Gets a value from the dictionary with given key. Returns default value if can not find.
        /// </summary>
        /// <param name="dictionary">Dictionary to check and get</param>
        /// <param name="key">Key to find the value</param>
        /// <param name="factory">A factory method used to create the value if not found in the dictionary</param>
        /// <typeparam name="TKey">Type of the key</typeparam>
        /// <typeparam name="TValue">Type of the value</typeparam>
        /// <returns>Value if found, default if can not found.</returns>
        public static TValue GetOrAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, Func<TValue> factory)
        {
            return dictionary.GetOrAdd(key, k => factory());
        }
        /// <summary>
        /// 合并字典
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="socure"></param>
        /// <param name="left"></param>
        /// <returns></returns>
        public static IDictionary<TKey, TValue> Merge<TKey, TValue>(this IDictionary<TKey, TValue> socure, IDictionary<TKey, TValue> left)
        {
            foreach (var item in left)
            {
                if (socure.ContainsKey(item.Key))
                {
                    throw new Exception("key contain dictionary");
                }
                socure.Add(item.Key, item.Value);
            }
            return socure;
        }
    }
}
