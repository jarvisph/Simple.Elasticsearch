using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Simple.Elasticsearch.Linq
{
    public static class ElasticSearchQueryable
    {
        public static IElasticSearchQueryable<TDocument> Where<TDocument>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            return query.Provider.CreateQuery<TDocument>(
                Expression.Call(
                    null,
                    CachedReflectionInfo.Where_TSource_2(typeof(TDocument)),
                    query.Expression,
                    Expression.Quote(expression)
                    )
                );
        }
        public static IElasticSearchQueryable<TDocument> Where<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, TValue? value, Expression<Func<TDocument, bool>> expression) where TDocument : class where TValue : struct
        {
            if (value == null) return query;
            return query.Where(expression);
        }
        public static IElasticSearchQueryable<TDocument> Where<TDocument>(this IElasticSearchQueryable<TDocument> query, object value, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            if (value == null) return query;
            return query.Where(expression);
        }
        public static IElasticSearchQueryable<TDocument> ContainNot<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, TValue[] value, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            return null;
        }
        public static bool Any<TDocument>(this IElasticSearchQueryable<TDocument> query) where TDocument : class
        {
            return query.Provider.Execute<bool>(
                Expression.Call(null, CachedReflectionInfo.Any_TSource_1(typeof(TDocument)), query.Expression)
                );
        }
        public static bool Any<TDocument>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            return query.Provider.Execute<bool>(
                Expression.Call(null, CachedReflectionInfo.Any_TSource_2(typeof(TDocument)), query.Expression, Expression.Quote(expression))
                );
        }

        public static int Count<TDocument>(this IElasticSearchQueryable<TDocument> query) where TDocument : class
        {
            return query.Provider.Execute<int>(
                Expression.Call(null, CachedReflectionInfo.Count_TSource_1(typeof(TDocument)), query.Expression)
                );
        }
        public static int Count<TDocument>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            return query.Provider.Execute<int>(
                Expression.Call(null, CachedReflectionInfo.Count_TSource_2(typeof(TDocument)), query.Expression, Expression.Quote(expression))
                );
        }

        public static TDocument? FirstOrDefault<TDocument>(this IElasticSearchQueryable<TDocument> query) where TDocument : class
        {
            return query.Provider.Execute<TDocument>(
                Expression.Call(null, CachedReflectionInfo.FirstOrDefault_TSource_1(typeof(TDocument)), query.Expression)
                );
        }
        public static TDocument? FirstOrDefault<TDocument>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            return query.Provider.Execute<TDocument>(
                Expression.Call(null, CachedReflectionInfo.FirstOrDefault_TSource_2(typeof(TDocument)), query.Expression, Expression.Quote(expression))
                );
        }
        public static TValue Max<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            return query.Provider.Execute<TValue>(
                Expression.Call(null, CachedReflectionInfo.Max_TSource_TResult_1(typeof(TDocument), typeof(TValue)), query.Expression, Expression.Quote(expression))
                );
        }
        public static TValue Min<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            return query.Provider.Execute<TValue>(
                Expression.Call(null, CachedReflectionInfo.Min_TSource_TResult_1(typeof(TDocument), typeof(TValue)), query.Expression, Expression.Quote(expression))
                );
        }
        public static TValue Average<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            return query.Provider.Execute<TValue>(
                Expression.Call(null, CachedReflectionInfo.Average_TSource_TResult_1(typeof(TDocument), typeof(TValue)), query.Expression, Expression.Quote(expression))
                );
        }


        public static IElasticSearchQueryable<IGrouping<TKey, TDocument>> GroupBy<TDocument, TKey>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TKey>> keySelector) where TDocument : class
        {
            return query.Provider.CreateQuery<IGrouping<TKey, TDocument>>(
                Expression.Call(null, CachedReflectionInfo.GroupBy_TSource_TKey_2(typeof(TDocument), typeof(TKey)), query.Expression, Expression.Quote(keySelector))
                );
        }

        public static IElasticSearchQueryable<TValue> Select<TDocument, TValue>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TValue>> selector) where TDocument : class where TValue : class
        {
            return query.Provider.CreateQuery<TValue>(
                Expression.Call(null, CachedReflectionInfo.Select_TSource_TResult_2(typeof(TDocument), typeof(TValue)), query.Expression, Expression.Quote(selector))
                );
        }

        public static DateTime ToDateTime(this int date)
        {
            return DateTime.UtcNow;
        }

        public static IElasticSearchOrderedQueryable<TDocument> OrderByDescending<TDocument, TKey>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TKey>> keySelector) where TDocument : class
        {
            return (IElasticSearchOrderedQueryable<TDocument>)query.Provider.CreateQuery<TDocument>(
                Expression.Call(null, CachedReflectionInfo.OrderByDescending_TSource_TKey_1(typeof(TDocument), typeof(TKey)), query.Expression, Expression.Quote(keySelector))
                );
        }
        public static IElasticSearchOrderedQueryable<TDocument> OrderBy<TDocument, TKey>(this IElasticSearchQueryable<TDocument> query, Expression<Func<TDocument, TKey>> keySelector) where TDocument : class
        {
            return (IElasticSearchOrderedQueryable<TDocument>)query.Provider.CreateQuery<TDocument>(
                Expression.Call(null, CachedReflectionInfo.OrderBy_TSource_TKey_1(typeof(TDocument), typeof(TKey)), query.Expression, Expression.Quote(keySelector))
                );
        }
        public static IElasticSearchOrderedQueryable<TDocument> OrderBy<TDocument>(this IElasticSearchOrderedQueryable<TDocument> query, string field, string sort) where TDocument : class
        {
            return null;
        }
        public static IEnumerable<TDocument> Paged<TDocument>(this IElasticSearchOrderedQueryable<TDocument> query, int page, int size, out long total) where TDocument : class
        {
            return query.Provider.Paged<TDocument>(query.Expression, page, size, out total);
        }
    }
}
