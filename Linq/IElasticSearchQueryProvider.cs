using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    public interface IElasticSearchQueryProvider
    {
        IElasticSearchQueryable CreateQuery(Expression expression);

        IElasticSearchQueryable<TElement> CreateQuery<TElement>(Expression expression) where TElement : class;

        object Execute(Expression expression);

        IEnumerable<TElement> Paged<TElement>(Expression expression, int page, int size, out long total) where TElement : class;

        TResult Execute<TResult>(Expression expression);
    }
}
