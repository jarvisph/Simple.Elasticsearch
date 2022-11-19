using System;
using System.Collections.Generic;
using System.Text;

namespace Simple.Elasticsearch
{
    public class ElasticSearchException : Exception
    {
        public ElasticSearchException(string message) : base(message)
        {

        }
    }
}
