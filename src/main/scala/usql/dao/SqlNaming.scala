package usql.dao

import usql.SqlReservedWords

import scala.collection.mutable

private[usql] object SqlNaming {

  /** Normalizes an arbitrary alias base into a valid readable SQL identifier. */
  def sanitizeAliasBase(raw: String): String = {
    val normalized  = raw.map { c =>
      if c.isLetterOrDigit || c == '_' then { c }
      else { '_' }
    }
    val nonEmpty    = if normalized.nonEmpty then { normalized }
    else { "q" }
    val startsValid = nonEmpty.head.isLetter || nonEmpty.head == '_'
    val candidate   = if startsValid then { nonEmpty }
    else { s"q_$nonEmpty" }
    if SqlReservedWords.isReserved(candidate) then { s"${candidate}_" }
    else { candidate }
  }

  /** Deduplicates projected column names deterministically. */
  def deduplicateColumnNames(names: Seq[String]): Seq[String] = {
    val used = mutable.Set.empty[String]
    names.map { name =>
      val result = findUnique(sanitizeAliasBase(name), used, separator = "", startAt = 1)
      used += result
      result
    }
  }

  private def findUnique(
      base: String,
      used: mutable.Set[String],
      separator: String,
      startAt: Int
  ): String = {
    if !used.contains(base) then { base }
    else {
      Iterator
        .from(startAt)
        .map(idx => s"$base$separator$idx")
        .find(candidate => !used.contains(candidate))
        .get
    }
  }

  final class AliasScope {
    private val used = mutable.Set.empty[String]

    /** Allocates a unique readable alias for a source within this scope. */
    def allocate(base: String): String = {
      val sanitized = sanitizeAliasBase(base)
      val short     = sanitized.take(1).toLowerCase

      val result = if !used.contains(short) then { short }
      else {
        // Short alias taken — try full name, then suffixed
        findUnique(sanitized, used, separator = "_", startAt = 1)
      }
      used += result
      result
    }
  }
}
