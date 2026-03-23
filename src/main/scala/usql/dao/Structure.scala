package usql.dao

import usql.{Optionalize, RowDecoder, RowEncoder}

/** Base trait for Structures. */
trait Structure[T] {

  /** Returns the columns of this structure. */
  def columns: Seq[SqlColumn[?]]

  /** Count of columns */
  def cardinality: Int = columns.size

  /** Decoder for a full row. */
  def rowDecoder: RowDecoder[T]

  /** Encoder for a full row. */
  def rowEncoder: RowEncoder[T]

  /** Returns true if T is an optional type */
  def isOptional: Boolean

  /** Returns the structure of this value optionalized. */
  def optionalize: Structure[Optionalize[T]]

  /** Select a field, maps columns accordingly */
  def selectField(name: String): Option[(index: Int, structure: Structure[?])]

  /** Returns available field names. */
  def fieldNames: Seq[String]

  /** Converts into a field of given name (maybe with anonymous column mapping) */
  def toField(fieldName: String): Field[T]

  /** Split an instance into its fields, columns are just one */
  protected[dao] def split(value: T): Seq[Any]

  /** Build from field values, columns are just one */
  protected[dao] def build(fieldValues: Seq[Any]): T

  /** Cardinality for split and build. */
  protected[dao] def fieldCardinality: Int

  /** Ensure all column ids are unique. */
  protected[dao] def ensureUniqueColumnIds(keepAlias: Boolean): Structure[T] = this

  /** Convert to SqlFielded, wrapping single columns in PseudoFielded. */
  private[usql] def toFielded: SqlFielded[T]
}

object Structure {

  /** Bridge so that given SqlFielded[T] instances are found when searching for Structure[T]. */
  given fromFielded[T](using f: SqlFielded[T]): Structure[T] = f
}
