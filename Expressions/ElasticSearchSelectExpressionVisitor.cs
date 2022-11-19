using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Simple.Elasticsearch.Expressions
{
    internal class ElasticSearchSelectExpressionVisitor : ExpressionVisitor
    {
        private readonly Queue<MemberInfo> _field = new Queue<MemberInfo>();
        private readonly Queue<Tuple<string, string, Type>> _select = new Queue<Tuple<string, string, Type>>();
        /// <summary>
        /// 获取Select字段类型属性
        /// </summary>
        /// <param name="node"></param>
        /// <returns>Item1=字段，Item2=Call类型，Item3=字段类型Type</returns>
        public new IEnumerable<Tuple<string, string, Type>> Visit(Expression node)
        {
            NewExpression expression = (NewExpression)node;
            foreach (MemberInfo member in expression.Members)
            {
                _field.Enqueue(member);
            }
            base.Visit(node);
            while (_select.Count > 0)
            {
                yield return _select.Dequeue();
            }
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            MemberInfo member = _field.Dequeue();
            _select.Enqueue(new Tuple<string, string, Type>(member.Name, "Key", node.Type));
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            MemberInfo member = _field.Dequeue();
            switch (node.Method.Name)
            {
                case "Sum":
                case "Max":
                case "Min":
                case "Average":
                case "Count":
                    _select.Enqueue(new Tuple<string, string, Type>(member.Name, node.Method.Name, node.Type));
                    break;
                case "ToDateTime":
                    _select.Enqueue(new Tuple<string, string, Type>(member.Name, "DateTime", typeof(DateTime)));
                    break;
            }
            return node;
        }
    }
}
