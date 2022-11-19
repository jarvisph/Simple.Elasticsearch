using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Simple.Elasticsearch.Linq
{
    public abstract class ESSet<TDocument> : IESRepository<TDocument> where TDocument : class, IDocument
    {
        public ESSet()
        {
        
        }
        public abstract bool Any();

        public abstract bool Any(Expression<Func<TDocument, bool>> expression);
        public abstract int Count();
        public abstract int Count(Expression<Func<TDocument, bool>> expression);
        public abstract bool Delete(Expression<Func<TDocument, bool>> expression);
        public abstract TDocument FirstOrDefault();
        public abstract TDocument FirstOrDefault(Expression<Func<TDocument, bool>> expression);
        public abstract IQueryable<TDocument> GetAll();
        public abstract IQueryable<TDocument> GetAll(Expression<Func<TDocument, bool>> expression);

        public abstract bool Insert(TDocument entity);
        public abstract bool Insert(IEnumerable<TDocument> entities);
        public abstract bool Update(TDocument entity, Expression<Func<TDocument, bool>> expression);
        public abstract bool Update(TDocument entity, Expression<Func<TDocument, bool>> expression, Expression<Func<TDocument, bool>> fields);
        public abstract bool Update<TValue>(TValue value, Expression<Func<TDocument, TValue>> field, Expression<Func<TDocument, bool>> expression);
    }
}
