using System;

namespace Simple.Elasticsearch
{
    /// <summary>
    /// ElasticSearchIndex特性类
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ElasticSearchIndexAttribute : Attribute
    {
        /// <summary>
        /// 索引名称（注意，ES索引不支持大写）
        /// </summary>
        public string IndexName { get; private set; }
        /// <summary>
        /// 别名（同理索引规则）
        /// </summary>
        public string[] AliasNames { get; set; }
        /// <summary>
        /// 副本数量（默认0）
        /// </summary>
        public int ReplicasCount { get; set; }
        /// <summary>
        /// 分片数量（默认3）
        /// </summary>
        public int ShardsCount { get; set; }
        /// <summary>
        /// 格式(默认按月创建索引，指定none，不使用索引规则)
        /// </summary>
        public string Format { get; set; }

        public ElasticSearchIndexAttribute(string indexname) : this(indexname, new[] { indexname })
        {

        }
        public ElasticSearchIndexAttribute(string indexname, string[] aliasnams, int replicascount = 0, int shardscount = 3, string fomat = "yyyy_MM")
        {
            this.IndexName = indexname;
            this.AliasNames = aliasnams;
            this.ReplicasCount = replicascount;
            this.ShardsCount = shardscount;
            this.Format = fomat;
        }
        /// <summary>
        /// 自定义索引
        /// </summary>
        /// <param name="datetime"></param>
        public void SetIndexTime(DateTime datetime)
        {
            if (this.Format != "none")
            {
                this.IndexName = $"{this.IndexName}_{datetime.ToString(this.Format)}";
            }
        }
    }
}