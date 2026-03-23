package usql.dao

import usql.*
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class SqlTabularTest extends TestBase {
  case class Sample(
      name: String,
      age: Int
  )

  "Tabular" should "be derivable" in {
    val tabular = SqlTabular.derived[Sample]
    tabular.columns.map(_.id) shouldBe Seq(SqlColumnId.fromString("name"), SqlColumnId.fromString("age"))
    tabular.table shouldBe SqlTableId.fromString("sample")
  }

  @TableName("samplename")
  case class SampleWithAnnotations(
      @ColumnName("my_name") name: String,
      age: Int
  )

  it should "work with annotations" in {
    val tabular = SqlTabular.derived[SampleWithAnnotations]
    tabular.table shouldBe SqlTableId.fromString("samplename")
    tabular.columns.map(_.id) shouldBe Seq(SqlColumnId.fromString("my_name"), SqlColumnId.fromString("age"))
  }

  case class Nested(
      x: Double,
      y: Double
  ) derives SqlFielded

  case class WithNested(
      @ColumnGroup(ColumnGroupMapping.Pattern(pattern = "a_%c"))
      a: Nested,
      @ColumnGroup(ColumnGroupMapping.Pattern(pattern = "%c_s"))
      b: Nested,
      c: Nested
  )

  it should "work for nested" in {
    val tabular = SqlTabular.derived[WithNested]
    tabular.rowEncoder.cardinality shouldBe 6
    tabular.rowDecoder.cardinality shouldBe 6
    tabular.columns.map(_.id) shouldBe Seq("a_x", "a_y", "x_s", "y_s", "c_x", "c_y").map(SqlColumnId.fromString)
    tabular.table.name shouldBe "with_nested"
  }
}
