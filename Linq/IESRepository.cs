using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Simple.Elasticsearch.Linq
{
    /// <summary>
    /// ES仓储
    /// </summary>
    public interface IESRepository<TEntity>
    {
        #region Insert
        public bool Insert(TEntity entity);
        public bool Insert(IEnumerable<TEntity> entities);
        #endregion

        #region Update
        public bool Update(TEntity entity, Expression<Func<TEntity, bool>> predicate);
        public bool Update(TEntity entity, Expression<Func<TEntity, bool>> predicate, Expression<Func<TEntity, bool>> fields);
        public bool Update<TValue>(TValue value, Expression<Func<TEntity, TValue>> field, Expression<Func<TEntity, bool>> predicate);
        #endregion

        #region Delete

        public bool Delete(TEntity entity);
        public bool Delete(Expression<Func<TEntity, bool>> predicate);

        #endregion
        #region Select

        public IQueryable<TEntity> GetAll();
        public IQueryable<TEntity> GetAll(Expression<Func<TEntity, bool>> predicate);

        public TEntity FirstOrDefault();
        public TEntity FirstOrDefault(Expression<Func<TEntity, bool>> predicate);

        public int Count();
        public int Count(Expression<Func<TEntity, bool>> predicate);

        public bool Any();
        public bool Any(Expression<Func<TEntity, bool>> predicate);
        #endregion

    }
}
