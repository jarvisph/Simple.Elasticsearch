using Nest;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Expressions
{
    internal interface IElasticSearchExpressionVisitor<TDocument> : IDisposable where TDocument : class
    {
        QueryContainer Query();
        SortDescriptor<TDocument> Sort();
        AggregationContainerDescriptor<TDocument> Aggregation(out List<Tuple<string, string, Type>>? select);
        Type Type { get; }
        string? Cell { get; }
        List<string> Cells { get; }
    }
}
