using Elasticsearch.Net;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Simple.Elasticsearch
{
    public abstract class ElasticSearchContext
    {
        protected IElasticClient Client { get; private set; }
        public ElasticSearchContext(IElasticClient client)
        {
            this.Client = client;
        }
        public ElasticSearchContext(string connectionString)
        {
            var urls = connectionString.Split(';').Select(http => new Uri(http));
            var staticConnectionPool = new StaticConnectionPool(urls);
            var settings = new ConnectionSettings(staticConnectionPool).DefaultFieldNameInferrer(name => name);
            this.Client = new ElasticClient(settings);
        }
    }
}
