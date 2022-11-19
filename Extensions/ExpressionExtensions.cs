using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using System.Reflection;

namespace Simple.Core.Extensions
{
    public static class ExpressionExtensions
    {
        /// <summary>
        /// 获取Experssion字段名
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="exp"></param>
        /// <returns></returns>
        public static string GetFieldName<TEntity, TValue>(this Expression<Func<TEntity, TValue>> exp)
        {
            MemberExpression node = exp.Body as MemberExpression;
            ColumnAttribute column = node.Member.GetAttribute<ColumnAttribute>();
            return column == null ? node.Member.Name : column.Name;
        }
        /// <summary>
        /// 获取表达式字段名称，支持传入特性类
        /// </summary>
        /// <typeparam name="TAttribute"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static string GetFieldName<TAttribute>(this Expression expression) where TAttribute : ColumnAttribute
        {
            MemberExpression member = (MemberExpression)expression;
            ColumnAttribute column = member.Member.GetAttribute<ColumnAttribute>();
            return column == null ? member.Member.Name : column.Name;
        }
        public static string GetFieldName<TAttribute>(this MemberInfo member) where TAttribute : ColumnAttribute
        {
            ColumnAttribute column = member.GetAttribute<ColumnAttribute>();
            return column == null ? member.Name : column.Name;
        }
        /// <summary>
        /// 获取expression的属性
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static PropertyInfo GetPropertyInfo<TSource, TKey>(this Expression<Func<TSource, TKey>> expression)
        {
            PropertyInfo property = null;
            switch (expression.Body.NodeType)
            {
                case ExpressionType.Convert:
                    property = (PropertyInfo)((MemberExpression)((UnaryExpression)expression.Body).Operand).Member;
                    break;
                case ExpressionType.MemberAccess:
                    property = (PropertyInfo)((MemberExpression)expression.Body).Member;
                    break;
            }
            return property;
        }
        /// <summary>
        /// 获取Propertys
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="Key"></typeparam>
        /// <param name="exp"></param>
        /// <returns></returns>
        public static IEnumerable<PropertyInfo> GetPropertys<TSource, Key>(this Expression<Func<TSource, Key>> expression)
        {
            NewExpression? node = expression.Body as NewExpression;
            if (node != null)
            {
                foreach (MemberInfo member in node.Members)
                {
                    yield return (PropertyInfo)member;
                }
            }
            else
            {
                yield return expression.GetPropertyInfo();
            }
        }
        /// <summary>
        /// 获取表达式对应sql运算类型
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string GetExpressionType(this ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.OrElse:
                case ExpressionType.Or: return "OR";
                case ExpressionType.AndAlso:
                case ExpressionType.And: return "AND";
                case ExpressionType.GreaterThan: return ">";
                case ExpressionType.GreaterThanOrEqual: return ">=";
                case ExpressionType.LessThan: return "<";
                case ExpressionType.LessThanOrEqual: return "<=";
                case ExpressionType.NotEqual: return "<>";
                case ExpressionType.Add: return "+";
                case ExpressionType.Subtract: return "-";
                case ExpressionType.Multiply: return "*";
                case ExpressionType.Divide: return "/";
                case ExpressionType.Modulo: return "%";
                case ExpressionType.Equal: return "=";
            }
            return string.Empty;
        }
    }
    internal class MemberAccesses : ExpressionVisitor
    {
        private ParameterExpression _parameter;
        public MemberExpression Member { get; private set; }

        public MemberAccesses(ParameterExpression parameter)
        {
            this._parameter = parameter;
        }
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _parameter)
            {
                Member = node;
            }
            return base.VisitMember(node);
        }
    }
}
