using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    internal static class CachedReflectionInfo
    {
        private static MethodInfo? s_Where_TSource_2;
        public static MethodInfo Where_TSource_2(Type TSource) =>
          (s_Where_TSource_2 ??
          (s_Where_TSource_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, bool>>, IElasticSearchQueryable<object>>(ElasticSearchQueryable.Where).GetMethodInfo().GetGenericMethodDefinition()))
           .MakeGenericMethod(TSource);

        private static MethodInfo? s_Any_TSource_1;
        public static MethodInfo Any_TSource_1(Type TSource) =>
            (s_Any_TSource_1 ??
            (s_Any_TSource_1 = new Func<IElasticSearchQueryable<object>, bool>(ElasticSearchQueryable.Any).GetMethodInfo().GetGenericMethodDefinition()))
             .MakeGenericMethod(TSource);


        private static MethodInfo? s_Any_TSource_2;
        public static MethodInfo Any_TSource_2(Type TSource) =>
             (s_Any_TSource_2 ??
             (s_Any_TSource_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, bool>>, bool>(ElasticSearchQueryable.Any).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource);

        private static MethodInfo? s_Count_TSource_1;
        public static MethodInfo Count_TSource_1(Type TSource) =>
            (s_Count_TSource_1 ??
            (s_Count_TSource_1 = new Func<IElasticSearchQueryable<object>, int>(ElasticSearchQueryable.Count).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource);

        private static MethodInfo? s_Count_TSource_2;
        public static MethodInfo Count_TSource_2(Type TSource) =>
            (s_Count_TSource_2 ??
            (s_Count_TSource_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, bool>>, int>(ElasticSearchQueryable.Count).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource);


        private static MethodInfo? s_FirstOrDefault_TSource_1;
        public static MethodInfo FirstOrDefault_TSource_1(Type TSource) =>
            (s_FirstOrDefault_TSource_1 ??
            (s_FirstOrDefault_TSource_1 = new Func<IElasticSearchQueryable<object>, object?>(ElasticSearchQueryable.FirstOrDefault).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource);

        private static MethodInfo? s_FirstOrDefault_TSource_2;
        public static MethodInfo FirstOrDefault_TSource_2(Type TSource) =>
             (s_FirstOrDefault_TSource_2 ??
             (s_FirstOrDefault_TSource_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, bool>>, object?>(ElasticSearchQueryable.FirstOrDefault).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource);


        private static MethodInfo? s_Max_TSource_TResult_1;

        public static MethodInfo Max_TSource_TResult_1(Type TSource, Type TResult) =>
             (s_Max_TSource_TResult_1 ??
             (s_Max_TSource_TResult_1 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, object>>, object>(ElasticSearchQueryable.Max).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource, TResult);

        private static MethodInfo? s_Min_TSource_TResult_1;

        public static MethodInfo Min_TSource_TResult_1(Type TSource, Type TResult) =>
             (s_Min_TSource_TResult_1 ??
             (s_Min_TSource_TResult_1 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, object>>, object>(ElasticSearchQueryable.Min).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource, TResult);

        private static MethodInfo? s_Average_TSource_TResult_1;

        public static MethodInfo Average_TSource_TResult_1(Type TSource, Type TResult) =>
             (s_Average_TSource_TResult_1 ??
             (s_Average_TSource_TResult_1 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, object>>, object>(ElasticSearchQueryable.Average).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource, TResult);

        private static MethodInfo? s_GroupBy_TSource_TKey_2;

        public static MethodInfo GroupBy_TSource_TKey_2(Type TSource, Type TKey) =>
             (s_GroupBy_TSource_TKey_2 ??
             (s_GroupBy_TSource_TKey_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, object>>, IElasticSearchQueryable<IGrouping<object, object>>>(ElasticSearchQueryable.GroupBy).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource, TKey);


        private static MethodInfo? s_Select_TSource_TResult_2;

        public static MethodInfo Select_TSource_TResult_2(Type TSource, Type TResult) =>
             (s_Select_TSource_TResult_2 ??
             (s_Select_TSource_TResult_2 = new Func<IElasticSearchQueryable<object>, Expression<Func<object, object>>, IElasticSearchQueryable<object>>(ElasticSearchQueryable.Select).GetMethodInfo().GetGenericMethodDefinition()))
              .MakeGenericMethod(TSource, TResult);

        private static MethodInfo? s_OrderByDescending_TSource_TKey_1;

        public static MethodInfo OrderByDescending_TSource_TKey_1(Type TSource, Type TKey) =>
            (s_OrderByDescending_TSource_TKey_1 ??
            (s_OrderByDescending_TSource_TKey_1 = new Func<IElasticSearchOrderedQueryable<object>, Expression<Func<object, object>>, IElasticSearchOrderedQueryable<object>>(ElasticSearchQueryable.OrderByDescending).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource, TKey);

        private static MethodInfo? s_OrderBy_TSource_TKey_1;
        public static MethodInfo OrderBy_TSource_TKey_1(Type TSource, Type TKey) =>
            (s_OrderBy_TSource_TKey_1 ??
            (s_OrderBy_TSource_TKey_1 = new Func<IElasticSearchOrderedQueryable<object>, Expression<Func<object, object>>, IElasticSearchOrderedQueryable<object>>(ElasticSearchQueryable.OrderBy).GetMethodInfo().GetGenericMethodDefinition()))
            .MakeGenericMethod(TSource, TKey);
    }
}
