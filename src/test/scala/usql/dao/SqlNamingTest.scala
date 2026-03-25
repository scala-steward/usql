package usql.dao

import usql.util.TestBase

class SqlNamingTest extends TestBase {

  "sanitizeAliasBase" should "keep normal names unchanged" in {
    SqlNaming.sanitizeAliasBase("person") shouldBe "person"
    SqlNaming.sanitizeAliasBase("user_role") shouldBe "user_role"
  }

  it should "replace special characters with underscores" in {
    SqlNaming.sanitizeAliasBase("my-table") shouldBe "my_table"
    SqlNaming.sanitizeAliasBase("a.b") shouldBe "a_b"
  }

  it should "handle names starting with digits" in {
    SqlNaming.sanitizeAliasBase("123abc") shouldBe "q_123abc"
  }

  it should "handle empty input" in {
    SqlNaming.sanitizeAliasBase("") shouldBe "q"
  }

  it should "append underscore for SQL reserved words" in {
    SqlNaming.sanitizeAliasBase("select") shouldBe "select_"
    SqlNaming.sanitizeAliasBase("from") shouldBe "from_"
    SqlNaming.sanitizeAliasBase("WHERE") shouldBe "WHERE_"
  }

  "AliasScope.allocate" should "use single-letter aliases for distinct tables" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("level") shouldBe "l"
  }

  it should "escalate to full names on first-letter collision" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("permission") shouldBe "permission"
  }

  it should "use full name when short collides with different table" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("permission") shouldBe "permission"
    // "person" full name is still available since first allocation used "p"
    scope.allocate("person") shouldBe "person"
    // Now both "p" and "person" are taken
    scope.allocate("person") shouldBe "person_1"
  }

  it should "use full name for self-joins" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("person") shouldBe "person"
  }

  it should "add suffixes when full name also collides" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("person") shouldBe "person"
    scope.allocate("person") shouldBe "person_1"
  }

  it should "work with three or more sources" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("level") shouldBe "l"
    scope.allocate("role") shouldBe "r"
  }

  it should "handle three-way first-letter collision with full names" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("person") shouldBe "p"
    scope.allocate("permission") shouldBe "permission"
    scope.allocate("project") shouldBe "project"
  }

  it should "handle reserved words in alias bases" in {
    val scope = SqlNaming.AliasScope()
    scope.allocate("select") shouldBe "s"
  }

  "deduplicateColumnNames" should "leave unique names unchanged" in {
    SqlNaming.deduplicateColumnNames(Seq("id", "name", "age")) shouldBe Seq("id", "name", "age")
  }

  it should "add numeric suffixes for duplicates" in {
    SqlNaming.deduplicateColumnNames(Seq("id", "id", "name")) shouldBe Seq("id", "id1", "name")
  }

  it should "handle multiple duplicates" in {
    SqlNaming.deduplicateColumnNames(Seq("id", "id", "id")) shouldBe Seq("id", "id1", "id2")
  }
}
