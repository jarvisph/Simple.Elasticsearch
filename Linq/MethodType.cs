using System;
using System.Collections.Generic;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    /// <summary>
    /// 方法类型
    /// </summary>
    internal enum MethodType
    {
        None,
        StartsWith,
        Contains,
        EndsWith,
        OrderByDescending,
        OrderBy,
        Where,
        Select,
        Any,
        Count,
        FirstOrDefault,
        Take,
        Skip
    }
}
