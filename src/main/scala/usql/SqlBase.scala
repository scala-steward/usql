package usql

import java.sql.{Connection, PreparedStatement, Statement}

/** Something which can create prepared statements. */
trait SqlBase {

  /** Prepares a statement which can then be further filled or executed. */
  def withPreparedStatement[T](
      f: PreparedStatement => T
  )(using cp: ConnectionProvider, prep: StatementPreparator = StatementPreparator.default): T

  /** Turns into a query */
  def query[T](using rowDecoder: RowDecoder[T]): Query[T] = Query.Simple(this, rowDecoder)

  /** Shortcut for query all. */
  def queryAll[T]()(using rowDecoder: RowDecoder[T], cp: ConnectionProvider): Vector[T] = query[T].all()

  /** Shortcut for query one. */
  def queryOne[T]()(using rowDecoder: RowDecoder[T], cp: ConnectionProvider): Option[T] = query[T].one()

  /** Turns into an update. */
  def update: Update = {
    Update(this)
  }

  /** Turns into a update on one value set. */
  def one[T](value: T)(using p: RowEncoder[T]): AppliedSql[T] = {
    AppliedSql(this, value, p)
  }

  /** Turns into a batch operation */
  def batch[T](values: Iterable[T])(using p: RowEncoder[T]): Batch[T] = {
    Batch(this, values, p)
  }

  /** Raw Executes this statement. */
  def execute()(using ConnectionProvider): Boolean = {
    withPreparedStatement(_.execute())
  }
}

/** Hook for changing the preparation of SQL. */
trait StatementPreparator {
  def prepare(connection: Connection, sql: String): PreparedStatement
}

object StatementPreparator {

  /** Default Implementation */
  object default extends StatementPreparator {
    override def prepare(connection: Connection, sql: String): PreparedStatement = {
      connection.prepareStatement(sql)
    }
  }

  /** Statement should return generated keys */
  object withGeneratedKeys extends StatementPreparator {
    override def prepare(connection: Connection, sql: String): PreparedStatement = {
      connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    }
  }
}

/** With supplied arguments */
case class AppliedSql[T](base: SqlBase, parameter: T, rowEncoder: RowEncoder[T]) extends SqlBase {
  override def withPreparedStatement[T](
      f: PreparedStatement => T
  )(using cp: ConnectionProvider, sp: StatementPreparator): T = {
    base.withPreparedStatement { ps =>
      rowEncoder.fill(ps, parameter)
      f(ps)
    }
  }
}
