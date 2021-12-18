using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    /// <summary>
    /// 实现ES Queryable
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    internal class ElasticSearchQueryable<TEntity> : IQueryable<TEntity>, IQueryable, IEnumerable<TEntity>, IEnumerable, IOrderedQueryable<TEntity>, IOrderedQueryable 
    {
        private readonly ElasticSearchQueryProvider _provider;
        private readonly Expression _expression;
        public ElasticSearchQueryable(ElasticSearchQueryProvider provider)
        {
            _provider = provider;
            _expression = Expression.Constant(this);
        }
        public ElasticSearchQueryable(ElasticSearchQueryProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(IQueryable<TEntity>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }
            this._provider = provider;
            this._expression = expression;
        }
        public Type ElementType
        {
            get
            {
                return typeof(TEntity).GetType();
            }
        }

        public Expression Expression => _expression;

        public IQueryProvider Provider => _provider;

        public IEnumerator<TEntity> GetEnumerator()
        {
            return this._provider.Executeable<TEntity>(this._expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)this._provider.Execute(this._expression)).GetEnumerator();
        }
    }
}
