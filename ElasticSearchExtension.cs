using Nest;
using Simple.Core.Extensions;
using Simple.Elasticsearch.Expressions;
using Simple.Elasticsearch.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Simple.Elasticsearch
{
    public static partial class ElasticSearchExtension
    {
        /// <summary>
        /// 索引缓存
        /// </summary>
        private static readonly Dictionary<string, bool> IndexCache = new Dictionary<string, bool>();

        /// <summary>
        /// 修改一个字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="value"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static bool Update<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> field, TValue value, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class, IDocument
        {
            if (value == null) return false;
            if (field == null) return false;
            string indexname = typeof(TDocument).GetIndexName();
            string fieldname = field.GetFieldName();
            UpdateByQueryResponse response = client.UpdateByQuery<TDocument>(c => c.Index(indexname).Query(q => q.Bool(b => b.Must(queries)))
                                                                                   .Script(s => s.Source($"ctx._source.{fieldname}=params.{fieldname}")
                                                                                   .Params(new Dictionary<string, object> { { fieldname, value } })));
            if (!response.IsValid) throw new ElasticSearchException(response.DebugInformation);
            return response.IsValid;
        }

        /// <summary>
        /// 修改指定的字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="document"></param>
        /// <param name="field"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        private static bool Update<TDocument>(this IElasticClient client, TDocument document, Expression<Func<TDocument, object>> field, QueryContainer query) where TDocument : class, IDocument
        {
            if (document == null) return false;
            if (field == null) return false;
            string indexname = typeof(TDocument).GetIndexName();
            IEnumerable<PropertyInfo> properties = field.GetPropertys();
            List<string> fields = new List<string>();
            Dictionary<string, object> param = new Dictionary<string, object>();
            foreach (PropertyInfo property in properties)
            {
                fields.Add(property.Name);
            }
            foreach (PropertyInfo property in document.GetType().GetProperties())
            {
                if (fields.Any(t => t == property.Name))
                {
                    param.Add(property.Name, property.GetValue(document));
                }
            }
            UpdateByQueryResponse response = client.UpdateByQuery<TDocument>(c => c.Index(indexname).Query(q => query)
                                                                                 .Script(s => s.Source(string.Join(";", fields.Select(c => $"ctx._source.{c}=params.{c}")))
                                                                                 .Params(param)));
            if (!response.IsValid) throw new ElasticSearchException(response.DebugInformation);
            return response.IsValid;
        }
        /// <summary>
        /// 修改字段内容
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="document"></param>
        /// <param name="expression"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static bool Update<TDocument>(this IElasticClient client, TDocument document, Expression<Func<TDocument, bool>> expression, Expression<Func<TDocument, object>> field) where TDocument : class, IDocument
        {
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.Update(document, field, query);
            }
        }
        /// <summary>
        /// 修改单个字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static bool Update<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> field, TValue value, Expression<Func<TDocument, bool>> expression) where TDocument : class, IDocument
        {
            if (value == null) return false;
            string indexname = typeof(TDocument).GetIndexName();
            string fieldname = field.GetFieldName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                UpdateByQueryResponse response = client.UpdateByQuery<TDocument>(c => c.Index(indexname).Query(q => query)
                                                                            .Script(s => s.Source($"ctx._source.{fieldname}=params.{fieldname}")
                                                                            .Params(new Dictionary<string, object>() { { fieldname, value } })));
                if (!response.IsValid) throw new ElasticSearchException(response.DebugInformation);
                return response.IsValid;
            }
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
            if (document == null) return false;
            ElasticSearchIndexAttribute? elasticsearch = typeof(TDocument).GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new ElasticSearchException("Not ElasticSearchIndexAttribute");
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
            ElasticSearchIndexAttribute? elasticsearch = typeof(TDocument).GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new ElasticSearchException("Not ElasticSearchIndexAttribute");
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
                throw new ElasticSearchException(response.DebugInformation);
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
                throw new ElasticSearchException(response.DebugInformation);
            return response.IsValid;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static bool Delete<TDocument>(this IElasticClient client, Expression<Func<TDocument, bool>> expression) where TDocument : class, IDocument
        {
            string indexname = typeof(TDocument).GetIndexName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                Func<DeleteByQueryDescriptor<TDocument>, IDeleteByQueryRequest> action = (d) =>
                {
                    return d.Index(indexname).Query(q => query);
                };
                DeleteByQueryResponse response = client.DeleteByQuery(action);
                if (!response.IsValid)
                    throw new ElasticSearchException(response.DebugInformation);
                return response.IsValid;
            }
        }


        /// <summary>
        /// 获取表总记录数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static int Count<TDocument>(this IElasticClient client) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname)).Count;
        }
        public static int Count<TDocument>(this IElasticClient client, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return (int)client.Count<TDocument>(c => c.Index(indexname).Query(q => query)).Count;
            }
        }
        /// <summary>
        /// 根据条件获取表总记录数
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static int Count<TDocument, TValue>(this IElasticClient client, TValue value, Expression<Func<TDocument, TValue>> field) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
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
        public static int Count<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname).Query(q => q.Bool(b => b.Must(queries)))).Count;
        }
        public static int Count<TDocument>(this IElasticClient client, QueryContainer query) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return (int)client.Count<TDocument>(c => c.Index(indexname).Query(q => query)).Count;
        }
        /// <summary>
        /// 查询表记录数（指定查询条件）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static int Count<TDocument>(this IElasticClient client, Func<SearchDescriptor<TDocument>, ISearchRequest> search) where TDocument : class
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
        /// 获取最小值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static TValue Min<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            string field = expression.GetFieldName();
            return client.Min<TDocument, TValue>(new QueryContainer(), new AggregationContainerDescriptor<TDocument>().Min(field, t => t.Field(expression)));
        }
        public static TValue Min<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> keySelect, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            string field = keySelect.GetFieldName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.Min<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Min(field, t => t.Field(expression)));
            }
        }
        public static TValue Min<TDocument, TValue>(this IElasticClient client, QueryContainer query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            string field = expression.GetFieldName();
            return client.Min<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Min(field, t => t.Field(expression)));
        }
        public static TValue Min<TDocument, TValue>(this IElasticClient client, QueryContainer query, AggregationContainerDescriptor<TDocument> aggregation) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchResponse<TDocument> response = client.Search<TDocument>(s => s.Index(indexname).Query(q => query).Aggregations(aggs => aggregation));
            if (!response.IsValid)
                throw new ElasticSearchException(response.DebugInformation);
            IDictionary<string, IAggregationContainer> aggs = ((IAggregationContainer)aggregation).Aggregations;
            if (aggs.Keys.Count == 0) throw new ElasticSearchException("Not specify field");
            ValueAggregate value = response.Aggregations.Min(aggs.Keys.First());
            if (value == null || value.Value == null) return default;
            return value.Value.Value.ToValue<TValue>();
        }

        /// <summary>
        /// 获取最大值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static TValue Max<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string field = expression.GetFieldName();
            return client.Max<TDocument, TValue>(new QueryContainer(), new AggregationContainerDescriptor<TDocument>().Max(field, t => t.Field(expression)));
        }
        public static TValue Max<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> keySelect, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            string field = keySelect.GetFieldName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.Max<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Max(field, t => t.Field(expression)));
            }
        }
        public static TValue Max<TDocument, TValue>(this IElasticClient client, QueryContainer query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            string field = expression.GetFieldName();
            return client.Max<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Max(field, t => t.Field(expression)));
        }
        public static TValue Max<TDocument, TValue>(this IElasticClient client, QueryContainer query, AggregationContainerDescriptor<TDocument> aggregation) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchResponse<TDocument> response = client.Search<TDocument>(s => s.Index(indexname).Query(q => query).Aggregations(aggs => aggregation));
            if (!response.IsValid)
                throw new ElasticSearchException(response.DebugInformation);
            IDictionary<string, IAggregationContainer> aggs = ((IAggregationContainer)aggregation).Aggregations;
            if (aggs.Keys.Count == 0) throw new ElasticSearchException("Not specify field");
            ValueAggregate value = response.Aggregations.Max(aggs.Keys.First());
            if (value == null || value.Value == null) return default;
            return value.Value.Value.ToValue<TValue>();
        }
        /// <summary>
        /// 平均值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NullReferenceException"></exception>
        public static TValue Average<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string field = expression.GetFieldName();
            return client.Average<TDocument, TValue>(new QueryContainer(), new AggregationContainerDescriptor<TDocument>().Average(field, t => t.Field(expression)));
        }
        public static TValue Average<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> keySelect, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            string field = keySelect.GetFieldName();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.Average<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Average(field, t => t.Field(expression)));
            }
        }
        public static TValue Average<TDocument, TValue>(this IElasticClient client, QueryContainer query, Expression<Func<TDocument, TValue>> expression) where TDocument : class
        {
            string field = expression.GetFieldName();
            return client.Average<TDocument, TValue>(query, new AggregationContainerDescriptor<TDocument>().Average(field, t => t.Field(expression)));
        }
        public static TValue Average<TDocument, TValue>(this IElasticClient client, QueryContainer query, AggregationContainerDescriptor<TDocument> aggregation) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            ISearchResponse<TDocument> response = client.Search<TDocument>(s => s.Index(indexname).Query(q => query).Aggregations(aggs => aggregation));
            if (!response.IsValid)
                throw new ElasticSearchException(response.DebugInformation);
            IDictionary<string, IAggregationContainer> aggs = ((IAggregationContainer)aggregation).Aggregations;
            if (aggs.Keys.Count == 0) throw new ElasticSearchException("Not specify field");
            ValueAggregate value = response.Aggregations.Average(aggs.Keys.First());
            if (value == null || value.Value == null) return default;
            return value.Value.Value.ToValue<TValue>();
        }


        /// <summary>
        /// 查询表是否存在
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static bool Any<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class
        {
            return client.Count(queries) > 0;
        }
        public static bool Any<TDocument>(this IElasticClient client, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            return client.Count(expression) > 0;
        }
        public static bool Any<TDocument>(this IElasticClient client, QueryContainer query) where TDocument : class
        {
            return client.Count<TDocument>(query) > 0;
        }
        /// <summary>
        /// 查询表是否存在（指定查询条件）
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static bool Any<TDocument>(this IElasticClient client, Func<SearchDescriptor<TDocument>, ISearchRequest> search) where TDocument : class
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
        public static bool Any<TDocument, TValue>(this IElasticClient client, TValue value, Expression<Func<TDocument, TValue>> field) where TDocument : class
        {
            return client.Count(value, field) > 0;
        }

        /// <summary>
        /// 获取第一条数据
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns>没有则为null</returns>
        public static TDocument? FirstOrDefault<TDocument>(this IElasticClient client, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return client.Search<TDocument>(s => s.Index(indexname).Query(q => q.Bool(b => b.Must(queries))).Size(1)).Documents?.FirstOrDefault();
        }
        public static TDocument? FirstOrDefault<TDocument>(this IElasticClient client, QueryContainer query) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            string indexname = typeof(TDocument).GetIndexName();
            return client.Search<TDocument>(s => s.Index(indexname).Query(q => query).Size(1)).Documents?.FirstOrDefault();
        }
        public static TDocument? FirstOrDefault<TDocument>(this IElasticClient client, Expression<Func<TDocument, bool>> expression) where TDocument : class
        {
            if (client == null) throw new NullReferenceException();
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.FirstOrDefault<TDocument>(query);
            }
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

        public static IEnumerable<TDocument> GetAll<TDocument>(this IElasticClient client, Expression<Func<TDocument>> expression) where TDocument : class
        {
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = visitor.Query();
                return client.GetAll<TDocument>(query);
            }
        }
        public static IEnumerable<TDocument> GetAll<TDocument>(this IElasticClient client, QueryContainer query) where TDocument : class
        {
            var scrollTime = new Time(TimeSpan.FromSeconds(30));
            string indexname = typeof(TDocument).GetIndexName();
            int size = 1000;
            var searchResponse = client.Search<TDocument>(s => s.Index(indexname).Size(size).Scroll(scrollTime).Query(q => query));
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
        /// 创建一个空的ElasticSearchQueryable
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static IElasticSearchQueryable<TDocument> Query<TDocument>(this IElasticClient client) where TDocument : class, IDocument
        {
            return new ElasticSearchQueryProvider<TDocument>(client);
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.GreaterThan((int)v).Field(field));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.GreaterThanOrEquals((int)v).Field(field));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.LessThan((int)v).Field(field));
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
                    else if (value.Value is long)
                    {
                        query.LongRange(r => r.LessThanOrEquals((long)v).Field(field));
                    }
                    else if (value.Value is decimal)
                    {
                        query.Range(r => r.LessThanOrEquals(Convert.ToDouble(v)).Field(field));
                    }
                    else if (value.Value is int)
                    {
                        query.Range(r => r.LessThanOrEquals((int)v).Field(field));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.GreaterThan((int)v).Field(script));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.GreaterThanOrEquals((int)v).Field(script));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.LessThan((int)v).Field(script));
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
                    else if (value.Value is int)
                    {
                        query.Range(r => r.LessThanOrEquals((int)v).Field(script));
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
        public static SearchDescriptor<TDocument> OrderByDescending<TDocument, TValue>(this SearchDescriptor<TDocument> query, Expression<Func<TDocument, TValue>> field, string fieldname, string order) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            if (!string.IsNullOrWhiteSpace(fieldname) && !string.IsNullOrWhiteSpace(order))
            {
                switch (order.ToLower())
                {
                    case "descending":
                        return query.Sort(c => c.Descending(fieldname));
                    case "ascending":
                        return query.Sort(c => c.Ascending(fieldname));
                }
            }
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
        /// <summary>
        /// 指定字段排序
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="search"></param>
        /// <param name="field"></param>
        /// <param name="fieldname"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> OrderByDescending<TDocument, TValue>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, TValue>> field, string fieldname, string order) where TDocument : class, IDocument
        {
            return (s) =>
            {
                return search.Invoke(s.OrderByDescending(field, fieldname, order));
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
        public static SearchDescriptor<TDocument> OrderBy<TDocument>(this SearchDescriptor<TDocument> query, string field) where TDocument : class, IDocument
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
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> OrderBy<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, string field) where TDocument : class, IDocument
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
        public static IEnumerable<TDocument> Paged<TDocument>(this IElasticClient client, Type type, QueryContainer query, SortDescriptor<TDocument> sort, int page, int size, out long total) where TDocument : class
        {
            total = 0;
            string indexname = type.GetIndexName();
            var response = client.Search<TDocument>(s => s.TrackTotalHits(true).Index(indexname).Query(q => query).Size(size).From((page - 1) * size).Sort(s => sort));
            if (!response.IsValid) throw new ElasticSearchException(response.DebugInformation);
            total = response.Total;
            return response.Documents;
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
                s.Size(0).Aggregations(aggs => aggs.Terms("group_by_script", t => t.Field(field).Size(1_000_00)));
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
            return search.GroupBy(string.Join(",", _script), selector);
        }

        /// <summary>
        /// 聚合
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="search"></param>
        /// <param name="script"></param>
        /// <returns></returns>
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> GroupBy<TDocument, Key>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, string script, Expression<Func<TDocument, Key>> selector) where TDocument : class, IDocument
        {
            IAggregationContainer group(AggregationContainerDescriptor<TDocument> aggs)
            {
                string[] array = script.GetArray<string>().Select(c => $"doc['{c}'].value").ToArray();
                return aggs.Terms("group_by_script", t => t.Script(string.Join("+'-'+", array)).Size(1_000_000).Aggregations(Aggregation(selector)));
            };
            return (s) =>
            {
                s.Size(0).Aggregations(group);
                search.Invoke(s);
                return s;
            };
        }

        /// <summary>
        /// 聚合
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="client"></param>
        /// <param name="field"></param>
        /// <param name="queries"></param>
        /// <returns></returns>
        public static IEnumerable<TValue> GroupBy<TDocument, TValue>(this IElasticClient client, Expression<Func<TDocument, TValue>> field, params Func<QueryContainerDescriptor<TDocument>, QueryContainer>[] queries) where TDocument : class where TValue : struct
        {
            string indexname = typeof(TDocument).GetIndexName();
            int size = 2147483647;//size设置支持最大数量： 2147483647

            string filedname = field.GetFieldName();
            var response = client.Search<TDocument>(s => s.Index(indexname).Size(0).Query(q => q.Bool(b => b.Must(queries)))
                                       .Aggregations(aggs => aggs.Terms("group_by_script", t => t.Field(field).Order(c => c.Ascending(filedname)).ShowTermDocCountError(true).Size(size))));
            if (!response.IsValid)
            {
                throw response.OriginalException;
            }
            var buckets = response.Aggregations.Terms("group_by_script").Buckets;
            foreach (var item in buckets)
            {
                yield return item.Key.ToValue<TValue>();
            }
        }

        public static IEnumerable<TDocument> GroupBy<TDocument>(this IElasticClient client, Type type, QueryContainer query, AggregationContainerDescriptor<TDocument> aggregation, List<Tuple<string, string, Type>>? select) where TDocument : class
        {
            string indexname = type.GetIndexName();
            var response = client.Search<TDocument>(s => s.Index(indexname).Size(0).Query(q => query)
                                  .Aggregations(agga => aggregation));
            if (select == null) yield break;
            if (!response.IsValid)
                throw new ElasticSearchException(response.DebugInformation);
            IDictionary<string, IAggregationContainer> _dictionary = ((IAggregationContainer)aggregation).Aggregations;
            KeyValuePair<string, IAggregationContainer> _aggs = _dictionary.FirstOrDefault();
            string[] array = _aggs.Key.Split("_");
            List<object> args;
            if (select.Any(c => c.Item2 == "DateTime"))
            {
                MultiBucketAggregate<DateHistogramBucket> _bucket = response.Aggregations.DateHistogram(_aggs.Key);
                foreach (DateHistogramBucket bucket in _bucket.Buckets)
                {
                    if (_aggs.Value.Aggregations.Any())
                    {
                        var _terms_aggs = _aggs.Value.Aggregations.FirstOrDefault();
                        array = _terms_aggs.Key.Split('_');
                        foreach (var item in bucket.Terms(_terms_aggs.Key).Buckets)
                        {
                            args = new List<object>();
                            args.ConvertAggregationValue(item, select, array, bucket);
                            yield return (TDocument)Activator.CreateInstance(typeof(TDocument), args.ToArray());
                        }
                    }
                    else
                    {
                        args = new List<object>();
                        args.ConvertAggregationValue(bucket, select, array);
                        yield return (TDocument)Activator.CreateInstance(typeof(TDocument), args.ToArray());
                    }
                }
            }
            else
            {
                TermsAggregate<string> _bucket = response.Aggregations.Terms(_aggs.Key);
                if (_bucket == null)
                {
                    AggregateDictionary dictionary = response.Aggregations;
                    args = new List<object>();
                    args.ConvertAggregationValue(dictionary, select);
                    yield return (TDocument)Activator.CreateInstance(typeof(TDocument), args.ToArray());
                }
                else
                {
                    foreach (KeyedBucket<string> item in _bucket.Buckets)
                    {
                        args = new List<object>();
                        args.ConvertAggregationValue(item, select, array);
                        yield return (TDocument)Activator.CreateInstance(typeof(TDocument), args.ToArray());
                    }
                }
            }
        }
        /// <summary>
        /// 聚合数据转Select匿名对象数据
        /// </summary>
        /// <param name="args"></param>
        /// <param name="bucket"></param>
        /// <param name="cell"></param>
        /// <exception cref="ElasticSearchException"></exception>
        private static void ConvertAggregationValue(this List<object> args, AggregateDictionary bucket, List<Tuple<string, string, Type>> select, string[]? script = null, DateHistogramBucket? date_bucket = null)
        {
            foreach (var item in select)
            {
                object? value;
                switch (item.Item2)
                {
                    case "Count":
                        if (bucket is KeyedBucket<string>)
                        {
                            value = ((KeyedBucket<string>)bucket).DocCount;
                        }
                        else
                        {
                            value = bucket.Count;
                        }
                        break;
                    case "Sum":
                        value = bucket.Sum(item.Item1).Value ?? 0;
                        break;
                    case "Max":
                        value = bucket.Max(item.Item1).Value ?? 0;
                        break;
                    case "Min":
                        value = bucket.Min(item.Item1).Value ?? 0;
                        break;
                    case "Average":
                        value = bucket.Average(item.Item1).Value ?? 0;
                        break;
                    case "Key":
                        KeyedBucket<string> keyBucket = (KeyedBucket<string>)bucket;
                        string[] keys = keyBucket.Key.Split("-");
                        int index = Array.IndexOf(script, item.Item1);
                        value = index == -1 ? null : keys[index];
                        break;
                    case "DateTime":
                        if (date_bucket == null)
                        {
                            DateHistogramBucket date_histogram = (DateHistogramBucket)bucket;
                            value = date_histogram.KeyAsString;
                        }
                        else
                        {
                            value = date_bucket.KeyAsString;
                        }
                        break;
                    default:
                        throw new ElasticSearchException($"Not implemented {item.Item2}");
                }
                args.Add(value.GetValue(item.Item3));
            }
        }
        /// <summary>
        /// 组装聚合的字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        private static Func<AggregationContainerDescriptor<TDocument>, IAggregationContainer> Aggregation<TDocument, Key>(Expression<Func<TDocument, Key>> selector) where TDocument : class, IDocument
        {
            return (s) =>
            {
                Type type = typeof(TDocument);
                foreach (var property in selector.GetPropertys())
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
                s.Size(0).Aggregations(ags => ags.DateHistogram(dateKey, d => d.Field(dateKey).Interval(interval).Format("yyyy-MM-dd")
                         .Aggregations(aggs => aggs.Terms("group_by_script", t => t.Script(string.Join("+'-'+", _script)).Size(1_000_000)
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
        public static SearchDescriptor<TDocument> Select<TDocument>(this SearchDescriptor<TDocument> query, string[] fields) where TDocument : class, IDocument
        {
            if (query == null) throw new NullReferenceException();
            return query.Source(sc => sc.Includes(ic => ic.Fields(fields)));
        }
        /// <summary>
        /// 查询指定字段
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="search"></param>
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
        public static Func<SearchDescriptor<TDocument>, ISearchRequest> Select<TDocument>(this Func<SearchDescriptor<TDocument>, ISearchRequest> search, Expression<Func<TDocument, object>> field) where TDocument : class, IDocument
        {
            if (search == null) throw new NullReferenceException();
            return (s) =>
            {

                string[] fields = field.GetPropertys().Select(c => c.Name).ToArray();
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
            return response.ToAggregate(_script.ToArray());
        }
        /// <summary>
        /// 转换聚合值
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="response"></param>
        /// <param name="script"></param>
        /// <returns></returns>
        public static IEnumerable<TDocument> ToAggregate<TDocument>(this ISearchResponse<TDocument> response, string[] scripts) where TDocument : class, IDocument
        {
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            IEnumerable<PropertyInfo> properties = typeof(TDocument).GetProperties();
            foreach (var item in response.Aggregations.Terms("group_by_script").Buckets)
            {
                TDocument document = Activator.CreateInstance<TDocument>();
                string[] key_value = item.Key.GetArray<string>('-');
                foreach (PropertyInfo property in properties)
                {
                    if (!property.CanWrite) continue;
                    object? value = null;
                    string? name = property.GetFieldName();
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
                throw new Exception(response.DebugInformation);
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
                        if (!property.CanWrite) continue;
                        object? value = null;
                        string? name = property.GetFieldName();
                        DateAttribute? dateAttribute = property.GetAttribute<DateAttribute>();
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
                        else if (dateAttribute != null)
                        {
                            if (dateAttribute.Name == name)
                            {
                                value = bucket.Date;
                            }
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
            ElasticSearchIndexAttribute? elasticsearch = type.GetAttribute<ElasticSearchIndexAttribute>();
            if (elasticsearch == null) throw new ElasticSearchException(nameof(ElasticSearchIndexAttribute));
            return elasticsearch.IndexName;
        }

        /// <summary>
        /// 获取字段名称
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="field"></param>
        /// <returns></returns>
        private static string GetFieldName<TDocument, TValue>(this Expression<Func<TDocument, TValue>> field)
        {
            PropertyInfo property = field.GetPropertyInfo();
            return property.GetFieldName();
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
        /// 手动创建索引
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="client"></param>
        /// <param name="indexDateTime"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static void CreateIndex<TDocument>(this IElasticClient client, DateTime? indexDateTime) where TDocument : class
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
        }
        /// <summary>
        /// 创建索引
        /// </summary>
        private static bool CreateIndex<TDocument>(this IElasticClient client, ElasticSearchIndexAttribute elasticsearch) where TDocument : class
        {
            var response = client.Indices.Create(elasticsearch.IndexName, c => c
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
            if (!response.IsValid)
            {
                throw new Exception(response.DebugInformation);
            }
            return response.IsValid;
        }


        /// <summary>
        /// Scroll 文档对象
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
        public static string? GetFieldName(this PropertyInfo property)
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
            return property.Name;
        }
        #endregion
    }
}

