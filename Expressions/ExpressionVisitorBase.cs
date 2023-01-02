using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Simple.Elasticsearch.Expressions
{
    public abstract class ExpressionVisitorBase : ExpressionVisitor
    {
        protected readonly Stack<object> _value = new Stack<object>();
        protected readonly Stack<Expression> _field = new Stack<Expression>();

        public List<string> Cells { get; private set; } = new List<string>();
        public Type Type { get; protected set; }


        protected virtual Expression VisitMemberAccess(MemberExpression node)
        {
            return node.Expression;
        }
        protected virtual Expression VisitConstant(ConstantExpression node, MemberInfo member)
        {
            switch (member.MemberType)
            {
                case MemberTypes.Field:
                    _value.Push(((FieldInfo)member).GetValue(node.Value));
                    break;
                case MemberTypes.Property:
                    _value.Push(((PropertyInfo)member).GetValue(node.Value));
                    break;
            }
            return node;
        }
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node == null) throw new ArgumentNullException(nameof(MemberExpression));
            if (node.Expression == null)
            {
                switch (node.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        switch (node.Member.Name)
                        {
                            case "Now":
                                _value.Push(DateTime.Now);
                                break;
                            default:
                                break;
                        }
                        break;
                }
            }
            else
            {
                switch (node.Expression.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        MemberExpression member = (MemberExpression)node.Expression;
                        ConstantExpression constant = (ConstantExpression)member.Expression;
                        switch (member.Member.MemberType)
                        {
                            case MemberTypes.Constructor:
                                break;
                            case MemberTypes.Event:
                                break;
                            case MemberTypes.Field:
                                {
                                    FieldInfo field = ((FieldInfo)member.Member);
                                    if (field.FieldType.BaseType.Name == "ValueType")
                                    {
                                        object model = field.GetValue(constant.Value);
                                        _value.Push(model);
                                    }
                                    else
                                    {
                                        var model = field.GetValue(constant.Value);
                                        _value.Push(model.GetType().GetProperty(node.Member.Name).GetValue(model));
                                    }
                                }
                                break;
                            case MemberTypes.Method:
                                break;
                            case MemberTypes.Property:
                                {
                                    var model = ((PropertyInfo)member.Member).GetValue(constant.Value);
                                    _value.Push(model.GetType().GetProperty(node.Member.Name).GetValue(model));
                                }
                                break;
                            case MemberTypes.TypeInfo:
                                break;
                            case MemberTypes.Custom:
                                break;
                            case MemberTypes.NestedType:
                                break;
                            case MemberTypes.All:
                                break;
                        }
                        break;
                    case ExpressionType.Constant:
                        this.VisitConstant((ConstantExpression)node.Expression, node.Member);
                        break;
                    case ExpressionType.Parameter:
                        _field.Push(node);
                        break;
                }
            }
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            switch (node.Type.Name)
            {
                case "Int16":
                case "Int32":
                case "Int64":
                case "Boolean":
                case "String":
                case "Decimal":
                case "Single":
                case "Double":
                case "DateTime":
                case "Byte":
                    _value.Push(node.Value);
                    break;
                default:
                    if (node.Type.IsGenericType)
                    {
                        this.Type = node.Type.GenericTypeArguments[0];
                    }
                    break;
            }
            return node;
        }
        protected virtual Expression VisitLambda(LambdaExpression node)
        {
            switch (node.Body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    this.VisitMemberAccess((MemberExpression)node.Body);
                    break;
            }
            return node;
        }
    }
}
