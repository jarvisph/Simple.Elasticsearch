using Nest;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;

namespace Simple.Elasticsearch
{
    /// <summary>
    /// 查询
    /// </summary>
    partial class ElasticSearchExtension
    {
        /// <summary>
        /// 通配符查询
        /// </summary>
        public static QueryContainerDescriptor<TDocument> Wildcard<TDocument>(this QueryContainerDescriptor<TDocument> query, string value, Expression<Func<TDocument, string>> field) where TDocument : class, IDocument
        {
            return query.Wildcard(value, $"*{value}*", field);
        }

        /// <summary>
        /// 通配符查询
        /// </summary>
        public static QueryContainerDescriptor<TDocument> Wildcard<TDocument>(this QueryContainerDescriptor<TDocument> query, string value, string pattern, Expression<Func<TDocument, string>> field) where TDocument : class, IDocument
        {
            if (string.IsNullOrEmpty(value)) return query;
            if (query == null) throw new NullReferenceException();
            query.Wildcard(field, pattern);
            return query;
        }

        /// <summary>
        /// 正则表达式查询
        /// </summary>
        /// <param name="regex">查询用的正则表达式</param>
        public static QueryContainerDescriptor<TDocument> Regexp<TDocument>(this QueryContainerDescriptor<TDocument> query, string regex, Expression<Func<TDocument, string>> field) where TDocument : class, IDocument
        {
            if (string.IsNullOrEmpty(regex)) return query;
            if (query == null) throw new NullReferenceException();
            query.Regexp(t => t
                .Field(field)
                .Value(regex)
                );
            return query;
        }
    }
}
