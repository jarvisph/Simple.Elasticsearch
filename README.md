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


> es 功能升级 linq 支持

	/// <summary>
        /// 基于Queryable查询
        /// </summary>
        /// <param name="client"></param>
        public void Queryable(IElasticClient client)
        {
            int userId = 10001;
            int[] sites = new[] { 1000, 1001 };
            //Nest 写法
            var response = client.Search<UserESModel>(c => c.Index("user").Query(q => q.Bool(b => b.Must(m => m.Term(t => t.Field(fd => fd.ID).Value(userId)),
                                                                                            m => m.Term(t => t.Field(fd => fd.IsTest).Value(false)),
                                                                                            m => m.Term(t => t.Field(fd => fd.UserName).Value(null)),
                                                                                            m => m.DateRange(d => d.Field(fd => fd.CreateAt).LessThan(DateTime.Now)),
                                                                                            m => m.Range(d => d.Field(fd => fd.SiteID).GreaterThan(0)))
                                                                                     .MustNot(n => n.Term(t => t.Field(fd => fd.Money).Value(0)),
                                                                                              n => n.Terms(t => t.Field(fd => fd.SiteID).Terms(sites))))));
            //linq to elasticsearch写法  query仅拼接查询语句，没有进行真实查询
            var query = client.Query<UserESModel>().Where(c => c.ID == userId)
                                                   .Where(c => c.Money != 0)
                                                   .Where(c => c.SiteID > 0)
                                                   .Where(DateTime.Now, c => c.CreateAt < DateTime.Now)
                                                   .Where(c => c.UserName.Contains("ceshi"))
                                                   .Where(c => !c.IsTest)
                                                   .Where(sites, c => !sites.Contains(c.SiteID))
                                                   .Where(null, t => t.UserName == null);

            var user = client.FirstOrDefault<UserESModel>(t => t.UserName.Contains("ceshi") && t.ID == userId && t.Money != 0 && t.CreateAt < DateTime.Now);

            //是否存在
            bool exists = query.Any();
            //总数
            int count = query.Count();
            //单条数据
            user = query.FirstOrDefault();
            //最大值
            decimal max = query.Max(t => t.Money);
            //最小值
            decimal min = query.Min(t => t.Money);
            //平均值
            decimal average = query.Average(t => t.Money);

            {
                //无条件聚合
                var group = query.GroupBy(c => true).Select(c => new { Count = c.Count(), Money = c.Sum(t => t.Money) });

                var group_list = group.ToList();

                var group_firt = group.FirstOrDefault();
            }

            {
                //条件聚合
                var group = query.GroupBy(c => new { c.SiteID, c.ID }).Select(c => new { c.Key.SiteID, Money = c.Sum(t => t.Money), });

                var group_list = group.ToList();

                var group_firt = group.FirstOrDefault();
            }

            {
                //日期+条件聚合
                var group = query.GroupBy(c => new { c.CreateAt.Month, c.SiteID }).Select(c => new
                {
                    CreateAt = c.Key.Month.ToDateTime(),
                    c.Key.SiteID,
                    Money = c.Sum(t => t.Money)
                });

                var group_list = group.ToList();

                var group_firt = group.FirstOrDefault();

            }

            //分页获取数据 倒序
            var desc = query.OrderByDescending(c => c.CreateAt).Paged(1, 20, out long total);

            //升序
            var asc = query.OrderBy(c => c.CreateAt).Paged(1, 20, out total);
        }

        /// <summary>
        /// Insert
        /// </summary>
        /// <param name="client"></param>
        public void Insert(IElasticClient client)
        {
            UserESModel user = new UserESModel
            {
                ID = 10001,
                CreateAt = DateTime.Now,
                Money = 10000,
                UserName = "ceshi01"
            };
            //单个插入
            client.Insert(user);
            //批量插入
            client.Insert(new[] { user });
        }

        /// <summary>
        /// Delete
        /// </summary>
        /// <param name="client"></param>
        public void Delete(IElasticClient client)
        {
            int userId = 10001;
            //删除
            client.Delete<UserESModel>(c => c.ID == userId);
        }

        /// <summary>
        /// Update
        /// </summary>
        /// <param name="client"></param>
        public void Update(IElasticClient client)
        {
            int userId = 10001;
            //修改指定的字段（PS，如果修改多个字段，建议使用Insert，ES机制如果存在，则修改全部字段）
            client.Update(new UserESModel
            {
                NickName = "李四",
                Money = 10001
            }, c => c.ID == userId, c => new
            {
                c.Money,
                c.NickName
            });
            //修改单个字段
            client.Update<UserESModel, decimal>(c => c.Money, 100, c => c.ID == userId);
        }

        public void Select(IElasticClient client)
        {
            int userId = 10001;
            //条件查询
            var user = client.FirstOrDefault<UserESModel>(t => t.UserName.Contains("ceshi") && t.ID == userId && t.Money != 0 && t.CreateAt < DateTime.Now);
            //是否存在
            bool exists = client.Any<UserESModel>(t => t.ID == userId);
            //记录数
            int count = client.Count<UserESModel>(t => t.ID == userId);
            //最大值
            decimal max = client.Max<UserESModel, decimal>(c => c.Money);
            //最小值
            decimal min = client.Min<UserESModel, decimal>(c => c.Money, t => t.Money > 0);
            //平均值
            decimal average = client.Average<UserESModel, decimal>(c => c.Money);
        }