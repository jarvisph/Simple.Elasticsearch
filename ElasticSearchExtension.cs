using Nest;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Simple.Elasticsearch
{
    public static class ElasticSearchExtension
    {
        /// <summary>
        /// 索引缓存
        /// </summary>
        private static readonly Dictionary<string, bool> IndexCache = new Dictionary<string, bool>();
        /// <summary>
        /// 生成更新脚本
        /// </summary>
        /// <param name="desc">ES对象</param>
        /// <param name="entity">要更新的对象</param>
        /// <param name="firstCharToLower">是否首字母小写（默认小写）</param>
        public static UpdateByQueryDescriptor<TEntity> Script<TEntity>(this UpdateByQueryDescriptor<TEntity> desc, object entity, bool firstCharToLower = true) where TEntity : class
        {
            var lstScript = new List<string>();
            var dicValue = new Dictionary<string, object>();

            foreach (PropertyInfo property in entity.GetType().GetProperties())
            {
                string field = property.GetFieldName();
                // 首字母小写
                if (firstCharToLower)
                {
                    var firstChar = field.Substring(0, 1).ToLower();
                    if (field.Length > 1) field = firstChar + field.Substring(1);
                    else field = firstChar;
                }

                lstScript.Add($"ctx._source.{field}=params.{field}");
                dicValue.Add(field, property.GetValue(entity));
            }

            return desc.Script(s => s.Source(string.Join(';', lstScript)).Params(dicValue));
        }

        /// <summary>
        /// 新增
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="document"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Insert<TDocument>(this IElasticClient client, TDocument document, DateTime? indexDateTime = null) where TDocument : class, IDocument
        {
            ElasticSearchIndexAttribute elasticsearch = typeof(TDocument).GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new ArgumentNullException("缺失ElasticSearchIndex特性");
            //检查是否已经创建索引
            if (indexDateTime.HasValue)
            {
                elasticsearch.SetIndexTime(indexDateTime.Value);
            }
            else
            {
                elasticsearch.SetIndexTime(DateTime.Now);
            }
            client.WhenNotExistsAddIndex<TDocument>(elasticsearch);
            IndexResponse response = client.Index(new IndexRequest<TDocument>(document, elasticsearch.IndexName));
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            return response.IsValid;
        }

        /// <summary>
        /// 批量插入
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="documents"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Insert<TDocument>(this IElasticClient client, IEnumerable<TDocument> documents, DateTime? indexDateTime = null) where TDocument : class, IDocument
        {
            ElasticSearchIndexAttribute elasticsearch = typeof(TDocument).GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new ArgumentNullException("缺失ElasticSearchIndex特性");
            if (indexDateTime.HasValue)
            {
                elasticsearch.SetIndexTime(indexDateTime.Value);
            }
            else
            {
                elasticsearch.SetIndexTime(DateTime.Now);
            }
            //检查是否已经创建索引
            client.WhenNotExistsAddIndex<TDocument>(elasticsearch);
            BulkResponse response = client.IndexMany(documents, elasticsearch.IndexName);
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            return response.IsValid;
        }


        /// <summary>
        /// 根据条件删除
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static bool Delete<TDocument>(this IElasticClient client, Func<DeleteByQueryDescriptor<TDocument>, IDeleteByQueryRequest> selector = null) where TDocument : class
        {
            string indexname = typeof(TDocument).GetIndexName();
            Func<DeleteByQueryDescriptor<TDocument>, IDeleteByQueryRequest> action = null;
            if (selector == null)
            {
                action = (s) =>
                {
                    return s.Index(indexname);
                };
            }
            else
            {
                action = (s) =>
                {
                    return selector.Invoke(s.Index(indexname));
                };
            }
            DeleteByQueryResponse response = client.DeleteByQuery(action);
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            return response.IsValid;
        }

        /// <summary>
        /// 获取表总记录数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static int Count<TDocument>(this IElasticClient client) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname)).Count;
        }
        /// <summary>
        /// 根据条件获取表总记录数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static int Count<TDocument, TValue>(this IElasticClient client, TValue value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            if (value == null) throw new NullReferenceException();
            if (field == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname).Query(q => q.Term(field, value))).Count;
        }
        /// <summary>
        /// 多条件获取表记录数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static int Count<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname).Query(q => q.Bool(b => b.Must(queries)))).Count;
        }
        /// <summary>
        /// 查询表记录数（指定查询条件）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static int Count<TDocument>(this IElasticClient client, Func<SearchDescriptor<TDocument>, ISearchRequest> search) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            if (search == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            Func<QueryContainerDescriptor<TDocument>, QueryContainer>[]? query = null;
            foreach (Delegate del in search.GetInvocationList())
            {
                if (del.Target == null) continue;
                dynamic target = del.Target;
                query = target.queries;
            }
            ICountRequest count(CountDescriptor<TDocument> q)
            {
                if (query == null)
                {
                    return q.Index(indexname);
                }
                else
                {
                    return q.Index(indexname).Query(q => q.Bool(b => b.Must(query)));
                }
            };
            return (int)client.Count<TDocument>(count).Count;
        }
        /// <summary>
        /// 查询表是否存在
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static bool Any<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            return client.Count(queries) > 0;
        }
        /// <summary>
        /// 查询表是否存在（指定查询条件）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static bool Any<TDocument>(this IElasticClient client, Func<SearchDescriptor<TDocument>, ISearchRequest> search) where TDocument : class, IDocument
        {
            return client.Count(search) > 0;
        }

        /// <summary>
        /// 条件查询表是否存在
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool Any<TDocument, TValue>(this IElasticClient client, TValue value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            return client.Count(value, field) > 0;
        }
        /// <summary>
        /// 获取第一条数据
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <returns>没有则为null</returns>
        public static TDocument? FirstOrDefault<TDocument, TValue>(this IElasticClient client, TValue value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            if (value == null) throw new NullReferenceException();
            if (field == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return client.Search<TDocument>(c => c.Index(indexname).Query(q => q.Term(field, value)).Size(1)).Documents?.FirstOrDefault();
        }
        /// <summary>
        /// 获取第一条数据
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <returns>没有则为null</returns>
        public static TDocument? FirstOrDefault<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return client.Search<TDocument>(s => s.Index(indexname).Query(q => q.Bool(b => b.Must(queries))).Size(1)).Documents?.FirstOrDefault();
        }

        /// <summary>
        /// 获取一条数据，取某个字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="field"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static TDocument? FirstOrDefault<TDocument>(this IElasticClient client, Expression<Func<TDocument, object>> field, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return client.Search<TDocument>(s => s.Index(indexname).Query(q => q.Bool(b => b.Must(queries))).Size(1).Select(field)).Documents?.FirstOrDefault();
        }
        /// <summary>
        /// 获取所有数据
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static IEnumerable<TDocument> GetAll<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class
        {
            var scrollTime = new Time(TimeSpan.FromSeconds(30));
            string indexname = typeof(TDocument).GetIndexName();
            int size = 1000;
            var searchResponse = client.Search<TDocument>(s => s.Index(indexname).Size(size).Scroll(scrollTime).Query(q => q.Bool(b => b.Must(queries))));
            return client.Scroll(searchResponse, size, scrollTime);
        }
        /// <summary>
        /// 查询条件（仅拼接查询条件，非真实查询）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Query<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            string indexname = typeof(TDocument).GetIndexName();
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).TrackTotalHits(true).Query(q => q.Bool(b => b.Must(queries)));
            }
            return query;
        }
        /// <summary>
        /// 查询条件与过滤条件
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="musts">查询条件</param>
        /// <param name="must_nots">过滤条件</param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Query<TDocument>(this IElasticClient client, IEnumerable<Func<QueryContainerDescriptor<TDocument>, QueryContainer>> musts, IEnumerable<Func<QueryContainerDescriptor<TDocument>, QueryContainer>> mustnots) where TDocument : class, IDocument
        {
            string indexname = typeof(TDocument).GetIndexName();
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).TrackTotalHits(true).Query(q => q.Bool(b => b.Must(musts).MustNot(mustnots)));
            }
            return query;
        }
        /// <summary>
        /// 指定索引或别名
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="indexname"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Query<TDocument>(this IElasticClient client, string indexname, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).TrackTotalHits(true).Query(q => q.Bool(b => b.Must(queries)));
            }
            return query;
        }
        /// <summary>
        /// 查询条件与过滤条件
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="indexname"></param>
        /// <param name="musts"></param>
        /// <param name="mustnots"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Query<TDocument>(this IElasticClient client, string indexname, IEnumerable<Func<QueryContainerDescriptor<TDocument>, QueryContainer>> musts, IEnumerable<Func<QueryContainerDescriptor<TDocument>, QueryContainer>> mustnots) where TDocument : class, IDocument
        {
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).TrackTotalHits(true).Query(q => q.Bool(b => b.Must(musts).MustNot(mustnots)));
            }
            return query;
        }

        /// <summary>
        /// 查询（真实查询）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="selector">检索条件</param>
        /// <returns></returns>
        public static List<TDocument> Search<TDocument>(this IElasticClient client, Func<QueryContainerDescriptor<TDocument>, QueryContainer> selector, params Expression<Func<TDocument, object>>[] fields) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).Query(q => q.Bool(b => b.Must(selector))).Select(fields);
            }
            return client.Search((Func<SearchDescriptor<TDocument>, ISearchRequest>)query).Documents.ToList();
        }
        /// <summary>
        /// 查询（真实查询）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="queries">查询条件</param>
        /// <returns></returns>
        public static List<TDocument> Search<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).Query(q => q.Bool(b => b.Must(queries)));
            }
            return client.Search((Func<SearchDescriptor<TDocument>, ISearchRequest>)query).Documents.ToList();
        }
        /// <summary>
        /// 查询（真实查询）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="fields">过滤字段</param>
        /// <returns></returns>
        public static List<TDocument> Search<TDocument>(this IElasticClient client, params Expression<Func<TDocument, object>>[] fields) where TDocument : class, IDocument
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchRequest query(SearchDescriptor<TDocument> q)
            {
                return q.Index(indexname).Select(fields);
            }
            return client.Search((Func<SearchDescriptor<TDocument>, ISearchRequest>)query).Documents.ToList();
        }

        public static QueryContainerDescriptor<TDocument> Where<TDocument>(this QueryContainerDescriptor<TDocument> query, string value, Expression<Func<TDocument, string>> field) where TDocument : class, IDocument
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            query.Term(field, value);
            return query;
        }

        /// <summary>
        /// 匹配一个或者多个值，同OR
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue? value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument where TValue : struct
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            query.Term(field, value);
            return query;
        }

        /// <summary>
        /// 匹配一个或者多个值，同OR
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="value"></param>
        /// <param name="script">脚本（查询数组中的字段）</param>
        /// <returns></returns>
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue? value, string script) where TDocument : class, IDocument where TValue : struct
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            query.Term(script, value);
            return query;
        }
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue value, string script) where TDocument : class, IDocument where TValue : struct
        {
            if (query == null) throw new NullReferenceException();
            query.Term(script, value);
            return query;
        }

        /// <summary>
        /// 匹配一个或者多个值，同OR
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue[] value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            if (field == null) return query;
            if (value.Length == 0)
            {
                switch (typeof(TValue).Name)
                {
                    case "Int16":
                        value = short.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Int32":
                        value = int.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Int64":
                        value = long.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Byte":
                        value = byte.MinValue.ToString().GetArray<TValue>();
                        break;
                    default:
                        break;
                }
            }
            query.Terms(t => t.Field(field).Terms(value));
            return query;
        }
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue[] value, string script) where TDocument : class, IDocument
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            if (script == null) return query;
            if (value.Length == 0)
            {
                switch (typeof(TValue).Name)
                {
                    case "Int16":
                        value = short.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Int32":
                        value = int.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Int64":
                        value = long.MinValue.ToString().GetArray<TValue>();
                        break;
                    case "Byte":
                        value = byte.MinValue.ToString().GetArray<TValue>();
                        break;
                    default:
                        break;
                }
            }
            query.Terms(t => t.Field(script).Terms(value));
            return query;
        }
        /// <summary>
        /// 范围查询
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue? value, Expression<Func<TDocument, TValue>> field, ExpressionType type) where TDocument : class, IDocument where TValue : struct
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            object v = value.Value;
            switch (type)
            {
                case ExpressionType.GreaterThan:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.GreaterThan((DateTime)v).Field(field));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.GreaterThan((long)v).Field(field));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.GreaterThan(Convert.ToDouble(v)).Field(field));
                    }
                    else
                    {
                        query.Range(r => r.GreaterThan((double)v).Field(field));
                    }
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.GreaterThanOrEquals((DateTime)v).Field(field));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.GreaterThanOrEquals((long)v).Field(field));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.GreaterThanOrEquals(Convert.ToDouble(v)).Field(field));
                    }
                    else
                    {
                        query.Range(r => r.GreaterThanOrEquals((double)v).Field(field));
                    }
                    break;
                case ExpressionType.LessThan:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.LessThan((DateTime)v).Field(field));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.LessThan((long)v).Field(field));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.LessThan(Convert.ToDouble(v)).Field(field));
                    }
                    else
                    {
                        query.Range(r => r.LessThan((double)v).Field(field));
                    }
                    break;
                case ExpressionType.LessThanOrEqual:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.LessThanOrEquals((DateTime)v).Field(field));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.LessThanOrEquals((long)v).Field(field));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.LessThanOrEquals(Convert.ToDouble(v)).Field(field));
                    }
                    else
                    {
                        query.Range(r => r.LessThanOrEquals((double)v).Field(field));
                    }
                    break;
            }
            return query;
        }

        public static QueryContainerDescriptor<TDocument> Where<TDocument, TValue>(this QueryContainerDescriptor<TDocument> query, TValue? value, string script, ExpressionType type) where TDocument : class, IDocument where TValue : struct
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            if (script == null) return query;
            object v = value.Value;
            switch (type)
            {
                case ExpressionType.GreaterThan:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.GreaterThan((DateTime)v).Field(script));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.GreaterThan((long)v).Field(script));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.GreaterThan(Convert.ToDouble(v)).Field(script));
                    }
                    else
                    {
                        query.Range(r => r.GreaterThan((double)v).Field(script));
                    }
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.GreaterThanOrEquals((DateTime)v).Field(script));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.GreaterThanOrEquals((long)v).Field(script));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.GreaterThanOrEquals(Convert.ToDouble(v)).Field(script));
                    }
                    else
                    {
                        query.Range(r => r.GreaterThanOrEquals((double)v).Field(script));
                    }
                    break;
                case ExpressionType.LessThan:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.LessThan((DateTime)v).Field(script));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.LessThan((long)v).Field(script));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.LessThan(Convert.ToDouble(v)).Field(script));
                    }
                    else
                    {
                        query.Range(r => r.LessThan((double)v).Field(script));
                    }
                    break;
                case ExpressionType.LessThanOrEqual:
                    if (value.Value is DateTime)
                    {
                        query.DateRange(dr => dr.LessThanOrEquals((DateTime)v).Field(script));
                    }
                    else if (value.Value is Int64)
                    {
                        query.LongRange(r => r.LessThanOrEquals((long)v).Field(script));
                    }
                    else if (value.Value is Decimal)
                    {
                        query.Range(r => r.LessThanOrEquals(Convert.ToDouble(v)).Field(script));
                    }
                    else
                    {
                        query.Range(r => r.LessThanOrEquals((double)v).Field(script));
                    }
                    break;
            }
            return query;
        }
        public static DeleteByQueryDescriptor<TDocument> Where<TDocument, TValue>(this DeleteByQueryDescriptor<TDocument> query, object value, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (value == null) return query;
            if (query == null) throw new NullReferenceException();
            switch (value.GetType().Name)
            {
                case "Guid":
                case "String":
                    query = query.Query(q => q.Term(c => c.Field(field.GetFieldName() + ".keyword").Value(value)));
                    break;
                default:
                    query = query.Query(q => q.Term(t => t.Field(field.GetFieldName()).Value(value)));
                    break;
            }
            return query;
        }
        public static DeleteByQueryDescriptor<TDocument> Where<TDocument, TValue>(this DeleteByQueryDescriptor<TDocument> query, TValue value, Expression<Func<TDocument, TValue>> field, ExpressionType type) where TDocument : class where TValue : struct, IDocument
        {
            if (query == null) throw new NullReferenceException();
            object v = value;
            switch (type)
            {
                case ExpressionType.GreaterThan:
                    if (value is DateTime)
                    {
                        query = query.Query(q => q.DateRange(dr => dr.GreaterThan((DateTime)v).Field(field)));
                    }
                    else
                    {
                        query = query.Query(q => q.Range(r => r.GreaterThan((double)v).Field(field)));
                    }
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (value is DateTime)
                    {
                        query = query.Query(q => q.DateRange(dr => dr.GreaterThanOrEquals((DateTime)v).Field(field)));
                    }
                    else
                    {
                        query = query.Query(q => q.Range(r => r.GreaterThanOrEquals((double)v).Field(field)));
                    }
                    break;
                case ExpressionType.LessThan:
                    if (value is DateTime)
                    {
                        query = query.Query(q => q.DateRange(dr => dr.LessThan((DateTime)v).Field(field)));
                    }
                    else
                    {
                        query = query.Query(q => q.Range(r => r.LessThan((double)v).Field(field)));
                    }
                    break;
                case ExpressionType.LessThanOrEqual:
                    if (value is DateTime)
                    {
                        query = query.Query(q => q.DateRange(dr => dr.LessThanOrEquals((DateTime)v).Field(field)));
                    }
                    else
                    {
                        query = query.Query(q => q.Range(r => r.LessThanOrEquals((double)v).Field(field)));
                    }
                    break;

            }
            return query;
        }
        /// <summary>
        /// 降序
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static SearchDescriptor<TDocument> OrderByDescending<TDocument, TValue>(this SearchDescriptor<TDocument> query, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            return query.Sort(c => c.Descending(field));
        }
        /// <summary>
        /// 降序，根据字段排序
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="query"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static SearchDescriptor<TDocument> OrderByDescending<TDocument>(this SearchDescriptor<TDocument> query, string field) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            return query.Sort(c => c.Descending(field));
        }
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> OrderByDescending<TDocument, TValue>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            return (s) =>
            {
                return search.Invoke(s.OrderByDescending(field));
            };
        }

        public static Func<SearchDescriptor<TDocument>, ISearchRequest> OrderByDescending<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, string field) where TDocument : class, IDocument
        {
            return (s) =>
            {
                return search.Invoke(s.OrderByDescending(field));
            };
        }
        /// <summary>
        /// 升序
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="query"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static SearchDescriptor<TDocument> OrderBy<TDocument, TValue>(this SearchDescriptor<TDocument> query, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            return query.Sort(c => c.Ascending(field));
        }
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> OrderBy<TDocument, TValue>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, TValue>> field) where TDocument : class, IDocument
        {
            return (s) =>
            {
                return search.Invoke(s.OrderBy(field));
            };
        }
        /// <summary>
        /// 分页
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="query"></param>
        /// <param name="page"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public static SearchDescriptor<TDocument> Paged<TDocument>(this SearchDescriptor<TDocument> query, int page, int limit) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            if (page == 1)
            {
                return query.Size(limit);
            }
            else
            {
                return query.From((page - 1) * limit).Size(limit);
            }
        }
        /// <summary>
        /// 分页，传入分页参数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="page"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Paged<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, int page, int limit) where TDocument : class, IDocument
        {
            return (s) =>
            {
                return search.Invoke(s.Paged(page, limit));
            };
        }
        /// <summary>
        /// 根据字段分组
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="search"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupBy<TDocument, Key>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, Key>> keySelector) where TDocument : class, IDocument
        {
            return (s) =>
            {
                s.Size(0).Aggregations(Aggregation(keySelector));
                search.Invoke(s);
                return s;
            };
        }
        /// <summary>
        /// 根据字符串字段进行分组
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupBy<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, string field) where TDocument : class, IDocument
        {
            return (s) =>
            {
                s.Size(0).Aggregations(aggs => aggs.Terms("group_by_script", t => t.Field(field)));
                search.Invoke(s);
                return s;
            };
        }

        /// <summary>
        /// 分组
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="keySelector">条件</param>
        /// <param name="keySelector">结果</param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupBy<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, object>> keySelector, Expression<Func<TDocument, object>> selector) where TDocument : class, IDocument
        {
            List<string> _script = new List<string>();
            Type type = typeof(TDocument);
            foreach (PropertyInfo property in keySelector.GetPropertys())
            {
                PropertyInfo propertyInfo = type.GetProperty(property.Name);
                if (propertyInfo == null) continue;
                string? fieldname = propertyInfo.GetFieldName();
                if (string.IsNullOrWhiteSpace(fieldname)) continue;
                _script.Add(fieldname);
            }
            return search.GroupBy(string.Join(",", _script), keySelector);
        }

        /// <summary>
        /// 聚合
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="search"></param>
        /// <param name="script"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupBy<TDocument, Key>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, string script, Expression<Func<TDocument, Key>> keySelector) where TDocument : class, IDocument
        {
            IAggregationContainer group(AggregationContainerDescriptor<TDocument> aggs)
            {
                string[] array = script.GetArray<string>().Select(c => $"doc['{c}'].value").ToArray();
                return aggs.Terms("group_by_script", t => t.Script(string.Join("+'-'+", array)).Aggregations(Aggregation(keySelector)));
            };
            return (s) =>
            {
                s.Size(0).Aggregations(group);
                search.Invoke(s);
                return s;
            };
        }

        /// <summary>
        /// 组装聚合的字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        private static Func<AggregationContainerDescriptor<TDocument>, IAggregationContainer> Aggregation<TDocument, Key>(Expression<Func<TDocument, Key>> keySelector) where TDocument : class, IDocument
        {
            return (s) =>
            {
                Type type = typeof(TDocument);
                foreach (var property in keySelector.GetPropertys())
                {
                    if (property == null) continue;
                    var propertyinfo = type.GetProperty(property.Name);
                    AggregateAttribute? aggregate = propertyinfo.GetAttribute<AggregateAttribute>();
                    if (aggregate == null) continue;
                    string? fieldname = aggregate.Name ?? propertyinfo.GetFieldName();
                    if (fieldname == null) continue;
                    if (aggregate.Type == AggregateType.Sum)
                    {
                        s.Sum(fieldname, c => c.Field(fieldname));
                    }
                    else if (aggregate.Type == AggregateType.Average)
                    {
                        s.Average(fieldname, c => c.Field(fieldname));
                    }
                    else if (aggregate.Type == AggregateType.Count)
                    {
                        s.ValueCount(fieldname, c => c.Field(fieldname));
                    }
                    else if (aggregate.Type == AggregateType.Max)
                    {
                        s.Max(fieldname, c => c.Field(fieldname));
                    }
                    else if (aggregate.Type == AggregateType.Min)
                    {
                        s.Min(fieldname, c => c.Field(fieldname));
                    }
                }
                return s;
            };
        }

        /// <summary>
        /// 按日期分组（默认按天）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupByDate<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, object>> keySelector, Expression<Func<TDocument, object>> selector) where TDocument : class, IDocument
        {
            return search.GroupByDate(DateInterval.Day, keySelector, selector);
        }
        /// <summary>
        /// 指定聚合方式
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="interval"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupByDate<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, DateInterval interval, Expression<Func<TDocument, object>> keySelector, Expression<Func<TDocument, object>> selector) where TDocument : class, IDocument
        {
            List<string> _script = new List<string>();
            Type type = typeof(TDocument);
            //时间对象字段
            string? datefield = string.Empty;
            foreach (PropertyInfo property in keySelector.GetPropertys())
            {
                PropertyInfo propertyInfo = type.GetProperty(property.Name);
                if (propertyInfo == null) continue;
                string? fieldname = propertyInfo.GetFieldName();
                if (propertyInfo.HasAttribute<DateAttribute>())
                {
                    datefield = fieldname;
                    continue;
                }
                if (string.IsNullOrWhiteSpace(fieldname)) continue;
                _script.Add(fieldname);
            }
            if (string.IsNullOrWhiteSpace(datefield)) throw new NullReferenceException("未标记时间特性");
            return search.GroupByDate(interval, _script.ToArray(), datefield, selector);


        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
        /// <param name="interval"></param>
        /// <param name="script"></param>
        /// <param name="dateKey">时间分组的key</param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupByDate<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, DateInterval interval, string[] script, string dateKey, Expression<Func<TDocument, object>> selector) where TDocument : class, IDocument
        {
            List<string> _script = new List<string>();
            foreach (var fieldname in script)
            {
                _script.Add($"doc['{fieldname}'].value");
            }
            return (s) =>
            {
                s.Size(0).Aggregations(ags => ags.DateHistogram(dateKey, d => d.Field(dateKey).CalendarInterval(interval).Format("yyyy-MM-dd")
                         .Aggregations(aggs => aggs.Terms("group_by_script", t => t.Script(string.Join("+'-'+", _script))
                         .Aggregations(Aggregation(selector))
                         ))));
                search.Invoke(s);
                return s;
            };
        }

        /// <summary>
        /// 查询指定字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="query"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static SearchDescriptor<TDocument> Select<TDocument>(this SearchDescriptor<TDocument> query, params Expression<Func<TDocument, object>>[] fields) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            return query.Source(sc => sc.Includes(ic => ic.Fields(fields)));
        }
        /// <summary>
        /// 查询指定字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="query"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Select<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, params Expression<Func<TDocument, object>>[] fields) where TDocument : class, IDocument
        {
            if (search == null) throw new NullReferenceException();
            return (s) =>
            {
                s.Select(fields);
                search.Invoke(s);
                return s;
            };
        }
        /// <summary>
        /// 转换聚合值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="response"></param>
        /// <returns></returns>
        public static TDocument ToAggregate<TDocument>(this ISearchResponse<TDocument> response) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            TDocument document = Activator.CreateInstance<TDocument>();
            IEnumerable<PropertyInfo> properties = typeof(TDocument).GetProperties().Where(c => c.HasAttribute<AggregateAttribute>());
            foreach (PropertyInfo property in properties)
            {
                AggregateAttribute aggregate = property.GetAttribute<AggregateAttribute>();
                if (aggregate == null) continue;
                string fieldname = aggregate.Name ?? property.GetFieldName();
                object? value = null;
                if (aggregate.Type == AggregateType.Sum)
                {
                    value = response.Aggregations.Sum(fieldname)?.Value;
                }
                else if (aggregate.Type == AggregateType.Average)
                {
                    value = response.Aggregations.Average(fieldname)?.Value;
                }
                else if (aggregate.Type == AggregateType.Count)
                {
                    value = response.Aggregations.ValueCount(fieldname)?.Value;
                }
                else if (aggregate.Type == AggregateType.Max)
                {
                    value = response.Aggregations.Max(fieldname)?.Value;
                }
                else if (aggregate.Type == AggregateType.Min)
                {
                    value = response.Aggregations.Min(fieldname)?.Value;
                }
                if (value == null) continue;
                property.SetValue(document, Convert.ChangeType(value, property.PropertyType));
            }
            return document;
        }

        public static IEnumerable<TDocument> ToAggregate<TDocument>(this ISearchResponse<TDocument> response, Expression<Func<TDocument, object>> keySelector) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            List<string> _script = new List<string>();
            Type type = typeof(TDocument);
            foreach (PropertyInfo property in keySelector.GetPropertys())
            {
                PropertyInfo propertyInfo = type.GetProperty(property.Name);
                if (propertyInfo == null) continue;
                string? fieldname = propertyInfo.GetFieldName();
                if (string.IsNullOrWhiteSpace(fieldname)) continue;
                _script.Add(fieldname);
            }
            return response.ToAggregate(string.Join("-", _script));
        }
        /// <summary>
        /// 转换聚合值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="response"></param>
        /// <param name="script"></param>
        /// <returns></returns>
        public static IEnumerable<TDocument> ToAggregate<TDocument>(this ISearchResponse<TDocument> response, string script) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            IEnumerable<PropertyInfo> properties = typeof(TDocument).GetProperties();
            string[] scripts = script.ToLower().GetArray<string>();
            foreach (var item in response.Aggregations.Terms("group_by_script").Buckets)
            {
                TDocument document = Activator.CreateInstance<TDocument>();
                string[] key_value = item.Key.GetArray<string>('-');
                foreach (PropertyInfo property in properties)
                {
                    object? value = null;
                    string name = property.GetFieldName().ToLower();
                    if (property.HasAttribute<CountAttribute>())
                    {
                        value = item.DocCount;
                    }
                    else if (scripts.Contains(name))
                    {
                        int index = Array.IndexOf(scripts, name);
                        if (property.PropertyType.IsEnum)
                        {
                            value = key_value[index].ToEnum(property.PropertyType);
                        }
                        else
                        {
                            value = key_value[index];
                        }
                    }
                    else
                    {
                        AggregateAttribute aggregate = property.GetAttribute<AggregateAttribute>();
                        if (aggregate == null) continue;
                        string fieldname = aggregate.Name ?? property.GetFieldName();
                        if (aggregate.Type == AggregateType.Sum)
                        {
                            value = item.Sum(fieldname)?.Value;
                        }
                        else if (aggregate.Type == AggregateType.Average)
                        {
                            value = item.Average(fieldname)?.Value;
                        }
                        else if (aggregate.Type == AggregateType.Count)
                        {
                            value = item.ValueCount(fieldname)?.Value;
                        }
                        else if (aggregate.Type == AggregateType.Max)
                        {
                            value = item.Max(fieldname)?.Value;
                        }
                        else if (aggregate.Type == AggregateType.Min)
                        {
                            value = item.Min(fieldname)?.Value;
                        }
                    }
                    if (value == null) continue;
                    property.SetValue(document, Convert.ChangeType(value, property.PropertyType));
                }
                yield return document;
            }
        }

        /// <summary>
        /// 时间聚合转换
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="response"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static IEnumerable<TDocument> ToDateAggregate<TDocument>(this ISearchResponse<TDocument> response, Expression<Func<TDocument, object>> keySelector) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            List<string> _script = new List<string>();
            Type type = typeof(TDocument);
            //时间对象字段
            string? datefield = string.Empty;
            foreach (PropertyInfo property in keySelector.GetPropertys())
            {
                PropertyInfo propertyInfo = type.GetProperty(property.Name);
                if (propertyInfo == null) continue;
                string? fieldname = propertyInfo.GetFieldName();
                if (propertyInfo.HasAttribute<DateAttribute>())
                {
                    datefield = fieldname;
                    continue;
                }
                if (string.IsNullOrWhiteSpace(fieldname)) continue;
                _script.Add(fieldname);
            }
            if (string.IsNullOrWhiteSpace(datefield)) throw new NullReferenceException("未标记时间特性");
            return response.ToDateAggregate(datefield, _script.ToArray());
        }
        /// <summary>
        /// 时间聚合转换
        /// </summary>
        /// <param name="response"></param>
        /// <param name="datefield">日期的名称</param>
        /// <param name="script">聚合条件脚本，多字段逗号分隔，注意（顺序跟Group中的script一致）</param>
        /// <returns></returns>
        public static IEnumerable<TDocument> ToDateAggregate<TDocument>(this ISearchResponse<TDocument> response, string datefield, string[] script) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw response.OriginalException;
            }
            if (response == null) throw new NullReferenceException();
            IEnumerable<PropertyInfo> properties = typeof(TDocument).GetProperties();
            foreach (DateHistogramBucket bucket in response.Aggregations.DateHistogram(datefield).Buckets)
            {
                TDocument document = Activator.CreateInstance<TDocument>();
                foreach (var item in bucket.Terms("group_by_script").Buckets)
                {
                    string[] key_value = item.Key.GetArray<string>('-');
                    foreach (PropertyInfo property in properties)
                    {
                        object? value = null;
                        string? name = property.GetFieldName();
                        if (property.HasAttribute<CountAttribute>())
                        {
                            value = item.DocCount;
                        }
                        else if (script.Contains(name))
                        {
                            int index = Array.IndexOf(script, name);
                            if (property.PropertyType.IsEnum)
                            {
                                value = key_value[index].ToEnum(property.PropertyType);
                            }
                            else
                            {
                                value = key_value[index];
                            }
                        }
                        else if (property.Name.ToLower() == datefield.ToLower())
                        {
                            value = bucket.Date;
                        }
                        else
                        {
                            AggregateAttribute? aggregate = property.GetAttribute<AggregateAttribute>();
                            if (aggregate == null) continue;
                            string? fieldname = aggregate.Name ?? property.GetFieldName();
                            if (aggregate.Type == AggregateType.Sum)
                            {
                                value = item.Sum(fieldname)?.Value;
                            }
                            else if (aggregate.Type == AggregateType.Average)
                            {
                                value = item.Average(fieldname)?.Value;
                            }
                            else if (aggregate.Type == AggregateType.Count)
                            {
                                value = item.ValueCount(fieldname)?.Value;
                            }
                            else if (aggregate.Type == AggregateType.Max)
                            {
                                value = item.Max(fieldname)?.Value;
                            }
                            else if (aggregate.Type == AggregateType.Min)
                            {
                                value = item.Min(fieldname)?.Value;
                            }
                        }
                        if (value == null) continue;
                        property.SetValue(document, Convert.ChangeType(value, property.PropertyType));
                    }
                    yield return document;
                }
            }
        }
        /// <summary>
        /// 单个分组ToList
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="response"></param>
        /// <returns></returns>
        public static List<T> ToList<T>(this AggregateDictionary response)
        {
            if (response == null) throw new NullReferenceException();
            List<T> list = new List<T>();
            foreach (var item in response.Terms("group_by_script").Buckets)
            {
                list.Add(item.Key.ToValue<T>());
            }
            return list;
        }

        #region ========== 扩展方法 ============

        /// <summary>
        /// 获取索引名称
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string GetIndexName(this Type type)
        {
            ElasticSearchIndexAttribute elasticsearch = type.GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new Exception("not index name");
            return elasticsearch.IndexName;
        }

        /// <summary>
        /// 获取字段名称
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="field"></param>
        /// <returns></returns>
        private static string GetFieldName<TDocument, TValue>(this Expression<Func<TDocument, TValue>> field) where TDocument : class
        {
            return field.GetPropertyInfo().GetFieldName();
        }
        /// <summary>
        /// 索引不存在时，创建索引
        /// </summary>
        private static void WhenNotExistsAddIndex<TDocument>(this IElasticClient client, ElasticSearchIndexAttribute elasticsearch) where TDocument : class
        {
            if (!IndexCache.ContainsKey(elasticsearch.IndexName) || !IndexCache[elasticsearch.IndexName])
            {
                if (!client.Indices.Exists(elasticsearch.IndexName).Exists)
                {
                    IndexCache[elasticsearch.IndexName] = client.CreateIndex<TDocument>(elasticsearch);
                }
            }
        }

        /// <summary>
        /// 创建索引
        /// </summary>
        private static bool CreateIndex<TDocument>(this IElasticClient client, ElasticSearchIndexAttribute elasticsearch) where TDocument : class
        {
            var rsp = client.Indices.Create(elasticsearch.IndexName, c => c
                .Map<TDocument>(m => m.AutoMap())
                .Aliases(des =>
                {
                    foreach (var aliasName in elasticsearch.AliasNames)
                    {
                        des.Alias(aliasName);
                    }

                    return des;
                }).Settings(s => s
                    .NumberOfReplicas(elasticsearch.ReplicasCount)
                    .NumberOfShards(elasticsearch.ShardsCount)
                    .RefreshInterval(new Time(TimeSpan.FromSeconds(1)))
                    )
            );
            return rsp.IsValid;
        }


        /// <summary>
        /// Scroll
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="response"></param>
        /// <param name="size"></param>
        /// <param name="scrollTime"></param>
        /// <returns></returns>
        private static IEnumerable<TDocument> Scroll<TDocument>(this IElasticClient client, ISearchResponse<TDocument> response, int size, Time scrollTime) where TDocument : class
        {
            if (!response.IsValid)
            {
                if (response.ServerError.Error.Type == "index_not_found_exception") yield break;
                throw response.OriginalException;
            }

            foreach (var item in response.Documents)
            {
                yield return item;
            }

            // 数量相等，说明还没有读完全部数据
            while (response.Documents.Count == size)
            {
                response = client.Scroll<TDocument>(scrollTime, response.ScrollId);
                if (response.Documents.Count > 0)
                {
                    foreach (var item in response.Documents)
                    {
                        yield return item;
                    }
                }
            }
            client.ClearScroll(s => s.ScrollId(response.ScrollId));
        }

        /// <summary>
        /// 获取ES实体字段名
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        private static string? GetFieldName(this PropertyInfo property)
        {
            NumberAttribute number = property.GetAttribute<NumberAttribute>();
            if (number != null)
            {
                if (!string.IsNullOrWhiteSpace(number.Name))
                {
                    return number.Name;
                }
            }
            KeywordAttribute keyword = property.GetAttribute<KeywordAttribute>();
            if (keyword != null)
            {
                if (!string.IsNullOrWhiteSpace(keyword.Name))
                {
                    return keyword.Name;
                }
            }
            DateAttribute date = property.GetAttribute<DateAttribute>();
            if (date != null)
            {
                if (!string.IsNullOrWhiteSpace(date.Name))
                {
                    return date.Name;
                }
            }
            TextAttribute text = property.GetAttribute<TextAttribute>();
            if (text != null)
            {
                if (!string.IsNullOrWhiteSpace(text.Name))
                {
                    return text.Name;
                }
            }
            if (!string.IsNullOrWhiteSpace(property.Name))
            {
                //首字母转小写
                return property.Name.Substring(0, 1).ToLower() + property.Name.Substring(1);
            }
            return property.Name;
        }

        /// <summary>
        /// 获取类的特性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        private static T? GetAttribute<T>(this object obj) where T : class
        {
            if (obj == null) return default;
            ICustomAttributeProvider custom = obj is ICustomAttributeProvider ? (ICustomAttributeProvider)obj : (ICustomAttributeProvider)obj.GetType();
            foreach (object t in custom.GetCustomAttributes(true))
            {
                if (t.GetType().Equals(typeof(T))) return (T)t;
            }
            return default;
        }
        /// <summary>
        /// 获取expression的属性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        private static PropertyInfo GetPropertyInfo<T, TKey>(this Expression<Func<T, TKey>> expression) where T : class
        {
            PropertyInfo property = null;
            switch (expression.Body.NodeType)
            {
                case ExpressionType.Convert:
                    property = (PropertyInfo)((MemberExpression)((UnaryExpression)expression.Body).Operand).Member;
                    break;
                case ExpressionType.MemberAccess:
                    property = (PropertyInfo)((MemberExpression)expression.Body).Member;
                    break;
            }
            return property;
        }
        /// <summary>
        /// 获取Propertys
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="exp"></param>
        /// <returns></returns>
        public static IEnumerable<PropertyInfo> GetPropertys<TSource, Key>(this Expression<Func<TSource, Key>> expression) where TSource : class
        {
            NewExpression? node = expression.Body as NewExpression;
            if (node != null)
            {
                foreach (MemberInfo member in node.Members)
                {
                    yield return (PropertyInfo)member;
                }
            }
            else
            {
                yield return expression.GetPropertyInfo();
            }
        }

        /// <summary>
        /// 将给定对象转换为不同类型
        /// </summary>
        /// <param name="obj"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns>Converted object</returns>
        private static T ToValue<T>(this object obj)
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
        /// <summary>
        /// 判断是否标记特性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        private static bool HasAttribute<T>(this Object obj) where T : Attribute
        {
            ICustomAttributeProvider custom = obj is ICustomAttributeProvider ? (ICustomAttributeProvider)obj : (ICustomAttributeProvider)obj.GetType();
            foreach (var t in custom.GetCustomAttributes(false))
            {
                if (t.GetType().Equals(typeof(T))) return true;
            }
            return false;
        }
        /// <summary>
        /// String转换枚举
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        private static T ToEnum<T>(this string value) where T : IComparable, IFormattable, IConvertible
        {
            if (string.IsNullOrWhiteSpace(value) || !typeof(T).IsEnum) return default;

            Type type = typeof(T);

            if (type.HasAttribute<FlagsAttribute>())
            {
                return ToFlagEnum<T>(value.Split(",").Where(c => !string.IsNullOrWhiteSpace(c) && Enum.IsDefined(type, c.Trim())).Select(c => Enum.Parse(type, value)).ToArray());
            }
            return Enum.IsDefined(type, value) ? (T)Enum.Parse(type, value) : default(T);
        }
        /// <summary>
        /// 转成flag枚举
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enums"></param>
        /// <returns></returns>
        private static T ToFlagEnum<T>(object[] enums) where T : IComparable, IFormattable, IConvertible
        {
            T result;
            switch (Enum.GetUnderlyingType(typeof(T)).Name)
            {
                case "Int16":
                    short int16 = 0;
                    foreach (object value in enums) int16 |= (short)value;
                    result = (T)Enum.ToObject(typeof(T), int16);
                    break;
                case "Int32":
                    int int32 = 0;
                    foreach (object value in enums) int32 |= (int)value;
                    result = (T)Enum.ToObject(typeof(T), int32);
                    break;
                case "Int64":
                    long int64 = 0;
                    foreach (object value in enums) int64 |= (long)value;
                    result = (T)Enum.ToObject(typeof(T), int64);
                    break;
                case "Byte":
                    byte bt = 0;
                    foreach (object value in enums) bt |= (byte)value;
                    result = (T)Enum.ToObject(typeof(T), bt);
                    break;
                default:
                    result = default;
                    break;
            }
            return result;
        }
        public static object ToEnum(this string value, Type type)
        {
            if (string.IsNullOrWhiteSpace(value) || !type.IsEnum) return default;
            return Enum.IsDefined(type, value) ? Enum.Parse(type, value) : default;
        }

        /// <summary>
        /// 把字符串转化成为数字数组
        /// </summary>
        /// <param name="str">用逗号隔开的数字</param>
        /// <param name="split"></param>
        /// <returns></returns>
        public static T[] GetArray<T>(this string str, char split = ',')
        {
            if (str == null) return Array.Empty<T>();
            str = str.Replace(" ", string.Empty);
            string regex = null;
            T[] result = Array.Empty<T>();
            switch (typeof(T).Name)
            {
                case "Int32":
                case "Byte":
                    regex = string.Format(@"(\d+{0})?\d$", split);
                    if (Regex.IsMatch(str, regex, RegexOptions.IgnoreCase))
                    {
                        result = str.Split(split).Where(t => t.IsType<T>()).ToList().ConvertAll(t => (T)Convert.ChangeType(t, typeof(T))).ToArray();
                    }
                    break;
                case "Guid":
                    regex = @"([0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12}" + split + @")?([0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12})$";
                    if (Regex.IsMatch(str, regex, RegexOptions.IgnoreCase))
                    {
                        result = str.Split(split).ToList().ConvertAll(t => (T)((object)Guid.Parse(t))).ToArray();
                    }
                    break;
                case "Decimal":
                    regex = string.Format(@"([0-9\.]+{0})?\d+$", split);
                    if (Regex.IsMatch(str, regex, RegexOptions.IgnoreCase))
                    {
                        result = str.Split(split).ToList().ConvertAll(t => (T)Convert.ChangeType(t, typeof(T))).ToArray();
                    }
                    break;
                case "Double":
                    result = str.Split(split).Where(t => t.IsType<T>()).Select(t => (T)Convert.ChangeType(t, typeof(T))).ToArray();
                    break;
                case "String":
                    result = str.Split(split).ToList().FindAll(t => !string.IsNullOrEmpty(t.Trim())).ConvertAll(t => (T)((object)t.Trim())).ToArray();
                    break;
                case "DateTime":
                    result = str.Split(split).ToList().FindAll(t => t.IsType<T>()).ConvertAll(t => (T)((object)DateTime.Parse(t))).ToArray();
                    break;
                default:
                    if (typeof(T).IsEnum)
                    {
                        result = str.Split(split).Where(t => Enum.IsDefined(typeof(T), t)).Select(t => (T)Enum.Parse(typeof(T), t)).ToArray();
                    }
                    break;
            }

            return result;
        }

        public static bool IsType<T>(this string value)
        {
            return IsType(value, typeof(T));
        }

        public static bool IsType(this string value, Type type)
        {
            bool isType;
            switch (type.Name)
            {
                case "Int32":
                    int int32;
                    isType = int.TryParse(value, out int32);
                    break;
                case "Int16":
                    short int16;
                    isType = short.TryParse(value, out int16);
                    break;
                case "Int64":
                    long int64;
                    isType = long.TryParse(value, out int64);
                    break;
                case "Guid":
                    Guid guid;
                    isType = Guid.TryParse(value, out guid);
                    break;
                case "DateTime":
                    DateTime dateTime;
                    isType = DateTime.TryParse(value, out dateTime);
                    break;
                case "Decimal":
                    decimal money;
                    isType = Decimal.TryParse(value, out money);
                    break;
                case "Double":
                    double doubleValue;
                    isType = Double.TryParse(value, out doubleValue);
                    break;
                case "String":
                    isType = true;
                    break;
                case "Boolean":
                    isType = Regex.IsMatch(value, "1|0|true|false", RegexOptions.IgnoreCase);
                    break;
                case "Byte":
                    byte byteValue;
                    isType = byte.TryParse(value, out byteValue);
                    break;
                default:
                    if (type.IsEnum)
                    {
                        isType = Enum.IsDefined(type, value);
                    }
                    else
                    {
                        throw new Exception("方法暂时未能检测该种类型" + type.FullName);
                    }
                    break;
            }
            return isType;
        }

        #endregion
    }
}

