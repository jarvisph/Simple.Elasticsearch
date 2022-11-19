using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Simple.Core.Extensions
{
    public static class StringExtensions
    {
        /// <summary>
        /// 如果不以字符结尾，则在给定字符串的结尾添加一个字符
        /// </summary>
        public static string EnsureEndsWith(this string str, char c)
        {
            return EnsureEndsWith(str, c, StringComparison.Ordinal);
        }

        /// <summary>
        /// 如果不以字符结尾，则在给定字符串的结尾添加一个字符
        /// </summary>
        public static string EnsureEndsWith(this string str, char c, StringComparison comparisonType)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }

            if (str.EndsWith(c.ToString(), comparisonType))
            {
                return str;
            }

            return str + c;
        }



        /// <summary>
        ///如果不以字符开头，则将字符添加到给定字符串的开头
        /// </summary>
        public static string EnsureStartsWith(this string str, char c, StringComparison comparisonType)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }

            if (str.StartsWith(c.ToString(), comparisonType))
            {
                return str;
            }

            return c + str;
        }

        /// <summary>
        ///如果不以字符开头，则将字符添加到给定字符串的开头
        /// </summary>
        public static string EnsureStartsWith(this string str, char c, bool ignoreCase, CultureInfo culture)
        {
            if (str == null)
            {
                throw new ArgumentNullException("str");
            }

            if (str.StartsWith(c.ToString(culture), ignoreCase, culture))
            {
                return str;
            }

            return c + str;
        }
        /// <summary>
        /// 非空非NULL判断
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty(this string str)
        {
            return string.IsNullOrEmpty(str);
        }
        /// <summary>
        /// 非空非NULL非空格判断
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static bool IsNullOrWhiteSpace(this string str)
        {
            return string.IsNullOrWhiteSpace(str);
        }
        /// <summary>
        /// 是否相等
        /// </summary>
        /// <param name="str"></param>
        /// <param name="eq"></param>
        /// <returns></returns>
        public static bool IsEqual(this string str, string eq)
        {
            return str == eq;
        }
        /// <summary>
        /// 从字符串的开头获取字符串的子字符串
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="str"/> is null</exception>
        /// <exception cref="ArgumentException"><paramref name="len"/>超出字符串长度</exception>
        public static string Left(this string str, int len)
        {
            if (str == null)
            {
                throw new ArgumentNullException("str");
            }

            if (str.Length < len)
            {
                throw new ArgumentException("超出字符串长度");
            }

            return str.Substring(0, len);
        }

        /// <summary>
        /// 将字符串中的行尾转换为 <see cref="Environment.NewLine"/>.
        /// </summary>
        public static string NormalizeLineEndings(this string str)
        {
            return str.Replace("\r\n", "\n").Replace("\r", "\n").Replace("\n", Environment.NewLine);
        }

        /// <summary>
        /// 获取字符串中第n次出现字符的索引
        /// </summary>
        /// <param name="str"></param>
        /// <param name="c"></param>
        /// <param name="n"></param>
        public static int NthIndexOf(this string str, char c, int n)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }

            var count = 0;
            for (var i = 0; i < str.Length; i++)
            {
                if (str[i] != c)
                {
                    continue;
                }

                if ((++count) == n)
                {
                    return i;
                }
            }

            return -1;
        }
        /// <summary>
        /// 获取字符串中出现某字符串的所有索引位置
        /// </summary>
        /// <param name="str"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static int[] NthIndexOf(this string str, char c)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            Queue<int> buff = new Queue<int>();
            for (int i = 0; i < str.Length; i++)
            {
                if (str[i] == c)
                {
                    buff.Enqueue(i);
                }
            }
            return buff.ToArray();
        }
        /// <summary>
        /// 替换后面字符串
        /// </summary>
        /// <param name="str"></param>
        /// <param name="length"></param>
        /// <param name="symbol"></param>
        /// <returns></returns>
        public static string ReplaceLast(this string str, int length, char symbol = '*')
        {
            if (str.Length < length)
                return str;
            return str.Substring(0, length) + new string(symbol, str.Length - length);
        }
        /// <summary>
        /// 替换中间字符串
        /// </summary>
        /// <param name="str"></param>
        /// <param name="before"></param>
        /// <param name="after"></param>
        /// <param name="symbol"></param>
        /// <returns></returns>
        public static string ReplaceCenter(this string str, int before, int after, char symbol = '*')
        {
            if (str.Length < before || before + after < str.Length)
                return str;
            return str.Substring(0, before) + new string(symbol, str.Length - before + after) + str.Substring(str.Length + after);
        }
        /// <summary>
        ///从给定字符串的结尾移除给定后缀的第一个匹配项
        /// 排序很重要。如果其中一个后缀匹配，则不会测试其他后缀
        /// </summary>
        /// <param name="str"></param>
        /// <param name="postFixes"></param>
        /// <returns>修改过的字符串或相同的字符串（如果它没有任何给定的后缀）</returns>
        public static string RemovePostFix(this string str, params string[] postFixes)
        {
            if (str == null)
            {
                return null;
            }

            if (str == string.Empty)
            {
                return string.Empty;
            }

            if (postFixes.IsNullOrEmpty())
            {
                return str;
            }

            foreach (var postFix in postFixes)
            {
                if (str.EndsWith(postFix))
                {
                    return str.Left(str.Length - postFix.Length);
                }
            }

            return str;
        }


        /// <summary>
        ///从字符串末尾获取字符串的子字符串
        /// </summary>
        /// <exception cref="ArgumentNullException"> <paramref name="str"/> is null</exception>
        /// <exception cref="ArgumentException"><paramref name="len"/> 超出字符串长度</exception>
        public static string Right(this string str, int len)
        {
            if (str == null)
            {
                throw new ArgumentNullException("str");
            }

            if (str.Length < len)
            {
                throw new ArgumentException("超出字符串长度");
            }

            return str.Substring(str.Length - len, len);
        }

        /// <summary>
        /// 截取字符串
        /// </summary>
        public static string[] Split(this string str, string separator)
        {
            return str.Split(new[] { separator }, StringSplitOptions.None);
        }

        /// <summary>
        /// 截取字符串
        /// </summary>
        public static string[] Split(this string str, string separator, StringSplitOptions options)
        {
            return str.Split(new[] { separator }, options);
        }

        /// <summary>
        /// 使用string.Split方法将给定字符串拆分为 <see cref="Environment.NewLine"/>.
        /// </summary>
        public static string[] SplitToLines(this string str)
        {
            return str.Split(Environment.NewLine);
        }

        /// <summary>
        /// 使用string.Split方法将给定字符串拆分为 <see cref="Environment.NewLine"/>.
        /// </summary>
        public static string[] SplitToLines(this string str, StringSplitOptions options)
        {
            return str.Split(Environment.NewLine, options);
        }


        /// <summary>
        /// 将指定区域性中的PascalCase字符串转换为camelCase字符串。
        /// </summary>
        /// <param name="str">String to convert</param>
        /// <param name="culture">An object that supplies culture-specific casing rules</param>
        /// <returns>camelCase of the string</returns>
        public static string ToCamelCase(this string str, CultureInfo culture)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }

            if (str.Length == 1)
            {
                return str.ToLower(culture);
            }

            return char.ToLower(str[0], culture) + str.Substring(1);
        }

        /// <summary>
        /// 将给定的PascalCase/camelCase字符串转换为句子（按空格分隔单词）
        /// </summary>
        /// <param name="str">String to convert.</param>
        /// <param name="invariantCulture">Invariant culture</param>
        public static string ToSentenceCase(this string str, bool invariantCulture = false)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }

            return Regex.Replace(
                str,
                "[a-z][A-Z]",
                m => m.Value[0] + " " + (invariantCulture ? char.ToLowerInvariant(m.Value[1]) : char.ToLower(m.Value[1]))
            );
        }

        /// <summary>
        /// 将给定的PascalCase/camelCase字符串转换为句子（按空格分隔单词）
        /// </summary>
        /// <param name="str">String to convert.</param>
        /// <param name="culture">An object that supplies culture-specific casing rules.</param>
        public static string ToSentenceCase(this string str, CultureInfo culture)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }

            return Regex.Replace(str, "[a-z][A-Z]", m => m.Value[0] + " " + char.ToLower(m.Value[1], culture));
        }

        /// <summary>
        /// Converts camelCase string to PascalCase string.
        /// </summary>
        /// <param name="str">String to convert</param>
        /// <param name="invariantCulture">Invariant culture</param>
        /// <returns>PascalCase of the string</returns>
        public static string ToPascalCase(this string str, bool invariantCulture = true)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }

            if (str.Length == 1)
            {
                return invariantCulture ? str.ToUpperInvariant() : str.ToUpper();
            }

            return (invariantCulture ? char.ToUpperInvariant(str[0]) : char.ToUpper(str[0])) + str.Substring(1);
        }

        public static T[] GetArray<T>(this string str, char split = ',')
        {
            return (T[])str.GetArray(typeof(T), split);
        }
        public static Array GetArray(this string str, Type type, char split = ',')
        {
            if (string.IsNullOrWhiteSpace(str)) return Array.CreateInstance(type, 0);
            var values = str.Split(split).Where(t => !string.IsNullOrEmpty(t)).ToArray();
            Array array = Array.CreateInstance(type, values.Length);
            switch (type.Name)
            {
                case "Guid":
                case "Decimal":
                case "Double":
                case "DateTime":
                case "Int32":
                case "Byte":
                case "Int64":
                case "String":
                    {
                        for (int i = 0; i < values.Length; i++)
                        {
                            array.SetValue(values[i].GetValue(type), i);
                        }
                    }
                    break;
                default:
                    if (type.IsEnum)
                    {
                        for (int i = 0; i < values.Length; i++)
                        {
                            if (Enum.IsDefined(type, values[i]))
                            {
                                array.SetValue(Enum.Parse(type, values[i]), i);
                            }
                        }
                    }
                    break;
            }
            return array;
        }
    }
}

