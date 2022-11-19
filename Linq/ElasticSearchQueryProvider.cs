using Nest;
using Simple.Elasticsearch.Expressions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Simple.Elasticsearch.Linq
{
    internal class ElasticSearchQueryProvider<TDocument> : IElasticSearchQueryProvider, IElasticSearchOrderedQueryable<TDocument> where TDocument : class
    {
        private readonly Expression _expression;
        private readonly IElasticClient _client;
        public ElasticSearchQueryProvider(Expression expression, IElasticClient client)
        {
            this._expression = expression;
            this._client = client;
        }
        public ElasticSearchQueryProvider(IElasticClient client)
        {
            this._client = client;
            this._expression = Expression.Constant(this);
        }

        public Type ElementType => typeof(TDocument);

        public IElasticSearchQueryProvider Provider => this;


        Expression IElasticSearchQueryable.Expression => _expression;

        public IElasticSearchQueryable CreateQuery(Expression expression)
        {
            throw new NotImplementedException();
        }

        public IElasticSearchQueryable<TElement> CreateQuery<TElement>(Expression expression) where TElement : class
        {
            return new ElasticSearchQueryProvider<TElement>(expression, this._client);
        }

        public object Execute(Expression expression)
        {
            throw new NotImplementedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            object? value = null;
            using (IElasticSearchExpressionVisitor<TDocument> vistor = new ElasticSearchExpressionVisitor<TDocument>(expression))
            {
                var query = vistor.Query();
                if (vistor.Cells.Contains("GroupBy"))
                {

                    switch (vistor.Cell)
                    {
                        case "FirstOrDefault":
                            value = _client.GroupBy(vistor.Type, query, vistor.Aggregation(out List<Tuple<string, string, Type>>? select), select).FirstOrDefault();
                            break;
                        default:
                            throw new ElasticSearchException($"Not implemented {vistor.Cell}");
                    }
                }
                else
                {
                    switch (vistor.Cell)
                    {
                        case "Any":
                            value = _client.Any<TDocument>(query);
                            break;
                        case "Count":
                            value = _client.Count<TDocument>(query);
                            break;
                        case "FirstOrDefault":
                            value = _client.FirstOrDefault<TDocument>(query);
                            break;
                        case "Max":
                            value = _client.Max<TDocument, TResult>(query, vistor.Aggregation(out _));
                            break;
                        case "Min":
                            value = _client.Min<TDocument, TResult>(query, vistor.Aggregation(out _));
                            break;
                        case "Average":
                            value = _client.Average<TDocument, TResult>(query, vistor.Aggregation(out _));
                            break;
                        default:
                            throw new ElasticSearchException($"Not implemented {vistor.Cell}");
                    }
                }
            }
            return (TResult)value;
        }

        public IEnumerator<TDocument> GetEnumerator()
        {
            using (IElasticSearchExpressionVisitor<TDocument> visitor = new ElasticSearchExpressionVisitor<TDocument>(_expression))
            {
                var query = visitor.Query();
                if (visitor.Cells.Contains("GroupBy"))
                {
                    var group = visitor.Aggregation(out List<Tuple<string, string, Type>>? select);
                    return _client.GroupBy(visitor.Type, query, group, select).GetEnumerator();
                }
                else
                {
                    return _client.GetAll<TDocument>(query).GetEnumerator();
                }
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TElement> Paged<TElement>(Expression expression, int page, int size, out long total) where TElement : class
        {
            using (IElasticSearchExpressionVisitor<TElement> visitor = new ElasticSearchExpressionVisitor<TElement>(expression))
            {
                var query = visitor.Query();
                var sort = visitor.Sort();
                return _client.Paged(visitor.Type, query, sort, page, size, out total);
            }
        }
    }
}
