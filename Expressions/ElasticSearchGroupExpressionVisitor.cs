using Nest;
using Simple.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Simple.Elasticsearch.Expressions
{
    internal class ElasticSearchGroupExpressionVisitor : ExpressionVisitorBase
    {
        private readonly Stack<Tuple<string, string, DateInterval?>> _group = new Stack<Tuple<string, string, DateInterval?>>();
        public ElasticSearchGroupExpressionVisitor()
        {

        }
        /// <summary>
        /// 获取聚合内容
        /// </summary>
        /// <param name="node"></param>
        /// <returns>Item1=字段，Item2=聚合类型，Item3=时间聚合类型</returns>
        public new IEnumerable<Tuple<string, string, DateInterval?>> Visit(Expression node)
        {
            base.Visit(node);
            while (_group.Count > 0)
            {
                yield return _group.Pop();
            }
        }
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member.DeclaringType.Name == "DateTime")
            {
                MemberExpression member = (MemberExpression)node.Expression;
                _group.Push(new Tuple<string, string, DateInterval?>(member.Member.Name, "date", node.Member.Name.ToEnum<DateInterval>()));
            }
            else
            {
                _group.Push(new Tuple<string, string, DateInterval?>(node.Member.HasAttribute<KeywordAttribute>() ? $"{node.Member.Name}.keyword" : node.Member.Name, "terms", null));
            }
            return node;
        }
        protected override Expression VisitNew(NewExpression node)
        {
            return base.VisitNew(node);
        }
    }
}
