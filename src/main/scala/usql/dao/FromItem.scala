package usql.dao

import usql.{Optionalize, Sql, SqlInterpolationParameter, sql}

/** Defines a DataSource within [[QueryBuilder]] */
private[usql] sealed trait FromItem[T] {

  /** The structure of this item. */
  def structure: Structure[T]

  /** Convert into SQL */
  def sql: Sql

  /** Preferred readable alias base for this source. */
  def aliasHint: String

  /** Root column path visible from this source. */
  lazy val rootPath: ColumnRootPath[T] = ColumnPath.make(using structure)
}

private[usql] object FromItem {
  case class Aliased[T](fromItem: FromItem[T], aliasName: String) extends FromItem[T] {
    override def structure: Structure[T] = fromItem.structure.withAlias(aliasName)

    override def sql: Sql = sql"${fromItem.sql} AS ${SqlInterpolationParameter.AliasParameter(aliasName)}"

    override def aliasHint: String = aliasName
  }

  case class FromTable[T](tabular: SqlTabular[T]) extends FromItem[T] {
    override def structure: Structure[T] = tabular

    override def sql: Sql = sql"${tabular.table}"

    override def aliasHint: String = SqlNaming.sanitizeAliasBase(tabular.table.name)
  }

  case class SubSelect[T](query2: QueryBuilder[T]) extends FromItem[T] {
    override def structure: Structure[T] = query2.structure.dropAlias

    override def sql: Sql = sql"(${query2.sql})"

    override def aliasHint: String = query2.preferredAliasBase
  }

  case class InnerJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
      extends FromItem[(L, R)] {
    override def structure: Structure[(L, R)] = {
      given leftStructure: Structure[L]  = left.structure
      given rightStructure: Structure[R] = right.structure
      summon
    }

    override def sql: Sql = {
      sql"${left.sql} JOIN ${right.sql} ON ${onExpression.toInterpolationParameter}"
    }

    override def aliasHint: String = "q"
  }

  case class LeftJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
      extends FromItem[(L, Optionalize[R])] {
    override def structure: Structure[(L, Optionalize[R])] = {
      given leftStructure: Structure[L]               = left.structure
      given rightStructure: Structure[Optionalize[R]] = right.structure.optionalize
      summon
    }

    override def sql: Sql = {
      sql"${left.sql} LEFT JOIN ${right.sql} ON ${onExpression.toInterpolationParameter}"
    }

    override def aliasHint: String = "q"
  }
}
