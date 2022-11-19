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
    public interface IESRepository<TDocument>
    {
        #region Insert
        public bool Insert(TDocument document);
        public bool Insert(IEnumerable<TDocument> documents);
        #endregion

        #region Update
        public bool Update(TDocument entity, Expression<Func<TDocument, bool>> predicate);
        public bool Update(TDocument entity, Expression<Func<TDocument, bool>> predicate, Expression<Func<TDocument, bool>> fields);
        public bool Update<TValue>(TValue value, Expression<Func<TDocument, TValue>> field, Expression<Func<TDocument, bool>> predicate);
        #endregion

        #region Delete

        public bool Delete(Expression<Func<TDocument, bool>> predicate);

        #endregion
        #region Select

        public IQueryable<TDocument> GetAll();
        public IQueryable<TDocument> GetAll(Expression<Func<TDocument, bool>> predicate);

        public TDocument FirstOrDefault();
        public TDocument FirstOrDefault(Expression<Func<TDocument, bool>> predicate);

        public int Count();
        public int Count(Expression<Func<TDocument, bool>> predicate);

        public bool Any();
        public bool Any(Expression<Func<TDocument, bool>> predicate);
        #endregion

    }
}
