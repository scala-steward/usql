package usql.dao

import usql.dao.Rep.{IsNumber, IsString, SqlRep}
import usql.{DataType, SqlInterpolationParameter, SqlParameters, Sql, UnOption, sql}

import scala.annotation.unused
import scala.language.implicitConversions

/** Typed representation for a typed value (either a [[ColumnPath]] or some raw SQL) */
trait Rep[T] {
  def toInterpolationParameter: SqlInterpolationParameter

  def ===(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} = ${rep.toInterpolationParameter}")
  }

  def !==(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} <> ${rep.toInterpolationParameter}")
  }

  def <(using IsNumber[T])(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} < ${rep.toInterpolationParameter}")
  }

  def >(using IsNumber[T])(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} > ${rep.toInterpolationParameter}")
  }

  def <=(using IsNumber[T])(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} <= ${rep.toInterpolationParameter}")
  }

  def >=(using IsNumber[T])(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} >= ${rep.toInterpolationParameter}")
  }

  def &&(using T =:= Boolean)(rep: Rep[Boolean]): Rep[Boolean] = {
    SqlRep(sql"(${toInterpolationParameter}) AND (${rep.toInterpolationParameter})")
  }

  def ||(using T =:= Boolean)(rep: Rep[Boolean]): Rep[Boolean] = {
    SqlRep(sql"(${toInterpolationParameter}) OR (${rep.toInterpolationParameter})")
  }

  def isNull(using @unused optCheck: T => Option[?]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} IS NULL")
  }

  def isNotNull(using @unused optCheck: T => Option[?]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} IS NOT NULL")
  }

  def unary_!(using T =:= Boolean): Rep[Boolean] = {
    SqlRep(sql"NOT (${toInterpolationParameter})")
  }

  // Arithmetic operations

  def +(using IsNumber[T])(rep: Rep[T]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) + (${rep.toInterpolationParameter})")
  }

  def -(using IsNumber[T])(rep: Rep[T]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) - (${rep.toInterpolationParameter})")
  }

  def *(using IsNumber[T])(rep: Rep[T]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) * (${rep.toInterpolationParameter})")
  }

  def /(using IsNumber[T])(rep: Rep[T]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) / (${rep.toInterpolationParameter})")
  }

  def %(using IsNumber[T])(rep: Rep[T]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) % (${rep.toInterpolationParameter})")
  }

  def unary_-(using IsNumber[T]): Rep[T] = {
    SqlRep(sql"-(${toInterpolationParameter})")
  }

  // String operations

  def like(using IsString[T])(pattern: Rep[String]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} LIKE ${pattern.toInterpolationParameter}")
  }

  def ++(using IsString[T])(rep: Rep[String]): Rep[T] = {
    SqlRep(sql"(${toInterpolationParameter}) || (${rep.toInterpolationParameter})")
  }

  // Set and range operations

  def in(values: Seq[UnOption[T]])(using DataType[UnOption[T]]): Rep[Boolean] = {
    if values.isEmpty then {
      SqlRep(sql"FALSE")
    } else {
      SqlRep(sql"${toInterpolationParameter} IN (${SqlParameters(values)})")
    }
  }

  def between(low: Rep[T], high: Rep[T]): Rep[Boolean] = {
    SqlRep(
      sql"${toInterpolationParameter} BETWEEN ${low.toInterpolationParameter} AND ${high.toInterpolationParameter}"
    )
  }
}

object Rep {
  case class SqlRep[T](rep: Sql) extends Rep[T] {
    override def toInterpolationParameter: SqlInterpolationParameter = rep
  }

  case class RawValue[T: DataType](value: T) extends Rep[T] {
    override def toInterpolationParameter: SqlInterpolationParameter =
      SqlInterpolationParameter.SqlParameter(value)
  }

  case class SomeRep[T](underlying: Rep[T]) extends Rep[Option[T]] {
    override def toInterpolationParameter: SqlInterpolationParameter = underlying.toInterpolationParameter
  }

  implicit def raw[T: DataType](value: T): Rep[T]                         = RawValue(value)
  implicit def rawOpt[T](value: T)(using dt: DataType[T]): Rep[Option[T]] = opt(raw(value))
  implicit def opt[T](rep: Rep[T]): Rep[Option[T]]                        = SomeRep(rep)

  /** T is a numeric SQL type. */
  trait IsNumber[T]
  object IsNumber {
    given int: IsNumber[Int] with               {}
    given long: IsNumber[Long] with             {}
    given short: IsNumber[Short] with           {}
    given byte: IsNumber[Byte] with             {}
    given float: IsNumber[Float] with           {}
    given double: IsNumber[Double] with         {}
    given bigDecimal: IsNumber[BigDecimal] with {}

    given opt[T](using IsNumber[T]): IsNumber[Option[T]] with {}
  }

  /** T is a string SQL type. */
  trait IsString[T]
  object IsString {
    given string: IsString[String] with            {}
    given stringOpt: IsString[Option[String]] with {}
  }
}
