using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Simple.Elasticsearch.Linq
{

    internal class ElasticSearchQueryProvider : IQueryProvider
    {
        protected readonly IElasticClient _client;
        public ElasticSearchQueryProvider(IElasticClient client)
        {
            _client = client;
        }
        public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = expression.Type.GetElementType();
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(ElasticSearchQueryable<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression) 
        {
            return new ElasticSearchQueryable<TElement>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            return default;
        }
        public IEnumerable<TEntity> Executeable<TEntity>(Expression expression) 
        {
            using (IElasticSearchExpressionVisitor visitor = new ElasticSearchExpressionVisitor(expression))
            {
                //string indexname = typeof(TEntity).GetIndexName();
                //List<Func<QueryContainerDescriptor<TEntity>, QueryContainer>> must = new List<Func<QueryContainerDescriptor<TEntity>, QueryContainer>>();
                //foreach (var entity in new int[] { })
                //{
                //    must.Add(c => c.Term("", 1));
                //}
                //_client.Search<TEntity>(c => c.Index(indexname).Query(q => q.Bool(b => b.Must(must))));
                yield return default(TEntity);
            }
        }
        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotImplementedException();
        }
    }
}
