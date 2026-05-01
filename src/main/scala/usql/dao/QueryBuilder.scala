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

  private[usql] def columnRootPath: ColumnRootPath[T] = ColumnPath.make(using structure)

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

  extension [A, B](qb: QueryBuilder[(A, B)])
    def map[P](
        f: (ColumnPath[(A, B), A], ColumnPath[(A, B), B]) => ColumnPath[(A, B), P]
    ): QueryBuilder[P] = {
      val root = qb.columnRootPath
      val a    = root.selectDynamic("_1").asInstanceOf[ColumnPath[(A, B), A]]
      val b    = root.selectDynamic("_2").asInstanceOf[ColumnPath[(A, B), B]]
      qb.project(f(a, b))
    }

  extension [A, B, C](qb: QueryBuilder[(A, B, C)])
    def map[P](
        f: (
            ColumnPath[(A, B, C), A],
            ColumnPath[(A, B, C), B],
            ColumnPath[(A, B, C), C]
        ) => ColumnPath[(A, B, C), P]
    ): QueryBuilder[P] = {
      val root = qb.columnRootPath
      val a    = root.selectDynamic("_1").asInstanceOf[ColumnPath[(A, B, C), A]]
      val b    = root.selectDynamic("_2").asInstanceOf[ColumnPath[(A, B, C), B]]
      val c    = root.selectDynamic("_3").asInstanceOf[ColumnPath[(A, B, C), C]]
      qb.project(f(a, b, c))
    }

  extension [A, B, C, D](qb: QueryBuilder[(A, B, C, D)])
    def map[P](
        f: (
            ColumnPath[(A, B, C, D), A],
            ColumnPath[(A, B, C, D), B],
            ColumnPath[(A, B, C, D), C],
            ColumnPath[(A, B, C, D), D]
        ) => ColumnPath[(A, B, C, D), P]
    ): QueryBuilder[P] = {
      val root = qb.columnRootPath
      val a    = root.selectDynamic("_1").asInstanceOf[ColumnPath[(A, B, C, D), A]]
      val b    = root.selectDynamic("_2").asInstanceOf[ColumnPath[(A, B, C, D), B]]
      val c    = root.selectDynamic("_3").asInstanceOf[ColumnPath[(A, B, C, D), C]]
      val d    = root.selectDynamic("_4").asInstanceOf[ColumnPath[(A, B, C, D), D]]
      qb.project(f(a, b, c, d))
    }

  extension [A, B, C, D, E](qb: QueryBuilder[(A, B, C, D, E)])
    def map[P](
        f: (
            ColumnPath[(A, B, C, D, E), A],
            ColumnPath[(A, B, C, D, E), B],
            ColumnPath[(A, B, C, D, E), C],
            ColumnPath[(A, B, C, D, E), D],
            ColumnPath[(A, B, C, D, E), E]
        ) => ColumnPath[(A, B, C, D, E), P]
    ): QueryBuilder[P] = {
      val root = qb.columnRootPath
      val a    = root.selectDynamic("_1").asInstanceOf[ColumnPath[(A, B, C, D, E), A]]
      val b    = root.selectDynamic("_2").asInstanceOf[ColumnPath[(A, B, C, D, E), B]]
      val c    = root.selectDynamic("_3").asInstanceOf[ColumnPath[(A, B, C, D, E), C]]
      val d    = root.selectDynamic("_4").asInstanceOf[ColumnPath[(A, B, C, D, E), D]]
      val e    = root.selectDynamic("_5").asInstanceOf[ColumnPath[(A, B, C, D, E), E]]
      qb.project(f(a, b, c, d, e))
    }
}
