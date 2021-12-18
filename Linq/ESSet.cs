using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    public abstract class ESSet<TEntity> : IQueryable<TEntity>, IESRepository<TEntity>
    {
        public Type ElementType => throw new NotImplementedException();

        public Expression Expression => throw new NotImplementedException();

        public IQueryProvider Provider => throw new NotImplementedException();

        public abstract bool Any();
        public abstract bool Any(Expression<Func<TEntity, bool>> expression);
        public abstract int Count();
        public abstract int Count(Expression<Func<TEntity, bool>> expression);
        public abstract bool Delete(TEntity entity);
        public abstract bool Delete(Expression<Func<TEntity, bool>> expression);
        public abstract TEntity FirstOrDefault();
        public abstract TEntity FirstOrDefault(Expression<Func<TEntity, bool>> expression);
        public abstract IQueryable<TEntity> GetAll();
        public abstract IQueryable<TEntity> GetAll(Expression<Func<TEntity, bool>> expression);

        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public abstract bool Insert(TEntity entity);
        public abstract bool Insert(IEnumerable<TEntity> entities);
        public abstract bool Update(TEntity entity, Expression<Func<TEntity, bool>> expression);
        public abstract bool Update(TEntity entity, Expression<Func<TEntity, bool>> expression, Expression<Func<TEntity, bool>> fields);
        public abstract bool Update<TValue>(TValue value, Expression<Func<TEntity, TValue>> field, Expression<Func<TEntity, bool>> expression);

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }


    }
}
