# Simple.Elasticsearch

> net core 对Elasticsearch 扩展，使代码书写更简洁、调用更方便，操作更简单

* ElasticSearchIndex特性介绍
 - IndexName        --索引名称
 - AliasNames        --别名
 - ReplicasCount    --分片数量
 - ShardsCount      --副本数量
 - Format                --索引名格式（默认yyyy_MM）

> es实体必须继承IDocument，并标记添加ElasticSearchIndex

	IElasticClient client = new ElasticClient(new Uri("localhost:9100"));
 
	//新增           
	client.Insert(new UserOrder { CreateTime = DateTime.Now, Money = 100, OrderID = "123456789", UserID = 1 });

	//拼接查询语句
	var query = client.Query<UserOrder>(c => c.Where(1, t => t.UserID),
	                                    c => c.Where(100, t => t.Money, ExpressionType.GreaterThanOrEqual));
	//查询数据         
	var list = client.Search(query).Documents.ToList();
 
	//查询单个数据           
	var order = client.FirstOrDefault<UserOrder>(c => c.Where("123456789", t => t.OrderID));
	 
	//是否存在           
	bool exists = client.Any<UserOrder>(c => c.Where("123456789", t => t.OrderID));
 
	//总数            
	int count = client.Count<UserOrder>(c => c.Where(1, t => t.UserID));

