using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Linq;

namespace Simple.Elasticsearch.Linq
{
    /// <summary>
    /// ES 表达式树解析
    /// </summary>
    internal class ElasticSearchExpressionVisitor : ExpressionVisitor, IElasticSearchExpressionVisitor
    {
        /// <summary>
        /// where 组装
        /// </summary>
        public Dictionary<string, Tuple<ExpressionType, object>> Where = new Dictionary<string, Tuple<ExpressionType, object>>();

        /// <summary>
        /// 包含那些方法
        /// </summary>
        public List<MethodType> Method = new List<MethodType>();
        /// <summary>
        /// 当前正在执行的方法类型
        /// </summary>
        public MethodType MethodType { get; private set; }

        public ElasticSearchExpressionVisitor()
        {

        }
        public ElasticSearchExpressionVisitor(Expression expression)
        {
            this.Visit(expression);
        }

        public override Expression Visit(Expression node)
        {
            return base.Visit(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            return base.VisitConstant(node);
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node == null) throw new ArgumentNullException("MethodCallExpression");
            this.MethodType = node.Method.Name.ToEnum<MethodType>();
            this.AddMethod();
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                this.Visit(node.Arguments[node.Arguments.Count - 1 - i]);
            }
            return node;
        }
        protected override Expression VisitBinary(BinaryExpression node)
        {
            return base.VisitBinary(node);
        }
        protected override Expression VisitNew(NewExpression node)
        {
            return base.VisitNew(node);
        }
        protected override Expression VisitMember(MemberExpression node)
        {
            return base.VisitMember(node);
        }
        private void AddMethod()
        {
            if (this.Method.Any(c => c != this.MethodType))
            {
                this.Method.Add(this.MethodType);
            }
        }
        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
