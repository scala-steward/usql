package usql.dao

import usql.{Optionalize, Sql, SqlInterpolationParameter, sql}

/** Defines a DataSource within [[QueryBuilder]] */
private[usql] sealed trait FromItem[T] {

  /** The structure of this item. */
  def structure: Structure[T]

  /** Convert into pre SQL */
  def toPreSql: Sql

  lazy val rootPath: ColumnRootPath[T] = ColumnPath.make(using structure)
}

private[usql] object FromItem {
  case class Aliased[T](fromItem: FromItem[T], aliasName: String) extends FromItem[T] {
    override def structure: Structure[T] = fromItem.structure.withAlias(aliasName)

    override def toPreSql: Sql = sql"${fromItem.toPreSql} AS ${SqlInterpolationParameter.AliasParameter(aliasName)}"
  }

  case class FromTable[T](tabular: SqlTabular[T]) extends FromItem[T] {
    override def structure: Structure[T] = tabular

    override def toPreSql: Sql = sql"${tabular.table}"
  }

  case class SubSelect[T](query2: QueryBuilder[T]) extends FromItem[T] {
    override def structure: Structure[T] = query2.structure.dropAlias

    override def toPreSql: Sql = sql"(${query2.toPreSql})"
  }

  case class InnerJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
      extends FromItem[(L, R)] {
    override def structure: Structure[(L, R)] = {
      given leftStructure: Structure[L]  = left.structure
      given rightStructure: Structure[R] = right.structure
      summon
    }

    override def toPreSql: Sql = {
      sql"${left.toPreSql} JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"
    }
  }

  case class LeftJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
      extends FromItem[(L, Optionalize[R])] {
    override def structure: Structure[(L, Optionalize[R])] = {
      given leftStructure: Structure[L]               = left.structure
      given rightStructure: Structure[Optionalize[R]] = right.structure.optionalize
      summon
    }

    override def toPreSql: Sql = {
      sql"${left.toPreSql} LEFT JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"
    }
  }
}
