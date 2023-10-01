using System;

namespace Simple.Core.Extensions
{
    internal static class DateTimeExtensions
    {
        public static DateTime Max(this DateTime time1, DateTime time2 = default)
        {
            if (time2 == default) time2 = new DateTime(1900, 1, 1);
            return time1 > time2 ? time1 : time2;
        }
        /// <summary>
        /// Unix时间戳格式
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static int ToUnixEpochDateInt(this DateTime date)
        {
            return (int)Math.Round((date.ToUniversalTime() - new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).TotalSeconds);
        }
        public static DateTime GetDateTime(this long timestamp)
        {
            return new DateTime(1970, 1, 1).Add(TimeZoneInfo.Local.BaseUtcOffset).AddSeconds(timestamp);
        }

        /// <summary>
        /// 获取时间戳
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static long GetTimestamp(this DateTime date)
        {
            return (date.ToUniversalTime().Ticks - 621355968000000000) / 10000;
        }
    }
}
