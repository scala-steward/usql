package usql.dao

import usql.{ConnectionProvider, Optionalize, Query, RowDecoder, Sql}

/** A Query Builder based upon filter, map and join methods. */
trait QueryBuilder[T] extends Query[T] {

  /** Convert this query to SQL. */
  def sql: Sql

  override def rowDecoder: RowDecoder[T] = structure.rowDecoder

  /** Structure of the result. */
  def structure: Structure[T]

  /** Map one element. */
  def map[P](f: ColumnRootPath[T] => ColumnPath[T, P]): QueryBuilder[P] = {
    project(f(columnRootPath))
  }

  protected def columnRootPath: ColumnRootPath[T] = ColumnPath.make(using structure)

  /** Project using a Column Path */
  def project[P](p: ColumnPath[T, P]): QueryBuilder[P]

  /** Filter step. */
  def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilder[T]

  /** Join two queries. */
  def join[R](right: QueryBuilder[R])(on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]): QueryBuilder[(T, R)]

  /** Left Join two Queries */
  def leftJoin[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, Optionalize[R])]

  /** Preferred readable base name for joining this query as a source. */
  private[usql] def preferredAliasBase: String = "q"

  /** Converts into a From Item which has an alias. */
  private[usql] def asAliasedFromItem(using scope: SqlNaming.AliasScope): FromItem[T] = {
    val aliasName = scope.allocate(preferredAliasBase)
    FromItem.Aliased(FromItem.SubSelect(this), aliasName)
  }

  /** Count elements. */
  def count()(using cp: ConnectionProvider): Int
}

/** A Query Builder which somehow still presents a projected table. Supports update call. */
trait QueryBuilderForProjectedTable[T] extends QueryBuilder[T] {

  /** Update elements. */
  def update(in: T)(using cp: ConnectionProvider): Int

  override def map[P](f: ColumnPath[T, T] => ColumnPath[T, P]): QueryBuilderForProjectedTable[P] = {
    project(f(columnRootPath))
  }

  override def project[P](p: ColumnPath[T, P]): QueryBuilderForProjectedTable[P]
}

/** A Query builder which somehow still presents a table. Supports delete call */
trait QueryBuilderForTable[T] extends QueryBuilderForProjectedTable[T] {

  /** Delete selected elements. */
  def delete()(using cp: ConnectionProvider): Int

  override def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilderForTable[T]
}

object QueryBuilder {

  def make[T](using tabular: SqlTabular[T]): QueryBuilderForTable[T] = {
    SimpleTableSelect(tabular)
  }
}
