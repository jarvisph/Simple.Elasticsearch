using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    public interface IElasticSearchQueryable : IEnumerable
    {
        Expression Expression { get; }
        Type ElementType { get; }
        IElasticSearchQueryProvider Provider { get; }
    }
    public interface IElasticSearchQueryable<TDocument> : IEnumerable<TDocument>, IEnumerable, IElasticSearchQueryable where TDocument : class
    {

    }
}
