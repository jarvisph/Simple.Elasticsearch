﻿using System;

namespace Simple.Elasticsearch
{
    /// <summary>
    /// ES索引名称标记
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ElasticSearchIndexAttribute : Attribute
    {
        /// <summary>
        /// 索引名称
        /// </summary>
        public string IndexName { get; private set; }
        /// <summary>
        /// 别名
        /// </summary>
        public string[] AliasNames { get; set; }
        /// <summary>
        /// 副本数量
        /// </summary>
        public int ReplicasCount { get; set; }
        /// <summary>
        /// 分片数量
        /// </summary>
        public int ShardsCount { get; set; }
        /// <summary>
        /// 格式(空或null 索引不转换格式索引)
        /// </summary>
        public string Format { get; set; }

        /// <summary>
        /// 初始构造
        /// </summary>
        /// <param name="indexname"></param>
        public ElasticSearchIndexAttribute(string indexname) : this(indexname, new[] { indexname })
        {

        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="indexname">索引名称</param>
        /// <param name="aliasnams">别名</param>
        /// <param name="replicascount">副本数量</param>
        /// <param name="shardscount">分片数量</param>
        /// <param name="fomat">格式</param>
        public ElasticSearchIndexAttribute(string indexname, string[] aliasnams, int replicascount = 0, int shardscount = 3, string fomat = "yyyy_MM")
        {
            this.IndexName = indexname;
            this.AliasNames = aliasnams;
            this.ReplicasCount = replicascount;
            this.ShardsCount = shardscount;
            this.Format = fomat;
        }
        /// <summary>
        /// 设置索引，根据自定义时间组装
        /// </summary>
        /// <param name="datetime"></param>
        public void SetIndexTime(DateTime datetime)
        {
            if (!string.IsNullOrWhiteSpace(this.Format))
            {
                this.IndexName = $"{this.IndexName}_{datetime.ToString(this.Format)}";
            }
        }
    }
}