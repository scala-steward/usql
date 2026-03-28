package usql.dao

import usql.util.TestBaseWithH2

class RepTest extends TestBaseWithH2 {
  override protected def baseSql: String =
    """
      |CREATE TABLE item (
      |  id INT PRIMARY KEY,
      |  name VARCHAR NOT NULL,
      |  price INT NOT NULL,
      |  discount INT,
      |  description VARCHAR
      |);
      |""".stripMargin

  case class Item(
      id: Int,
      name: String,
      price: Int,
      discount: Option[Int] = None,
      description: Option[String] = None
  ) derives SqlTabular

  object Item extends KeyedCrudBase[Int, Item] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Item] = summon
  }

  trait Env {
    val widget      = Item(1, "Widget", 100, Some(10), Some("A basic widget"))
    val gadget      = Item(2, "Gadget", 200, None, Some("A fancy gadget"))
    val thingamajig = Item(3, "Thingamajig", 50, Some(5), None)

    Item.insert(widget, gadget, thingamajig)
  }

  case class FilterCase(
      name: String,
      filter: ColumnBasePath[Item] => Rep[Boolean],
      expected: Seq[String]
  )

  val cases: Seq[FilterCase] = Seq(
    FilterCase("addition", i => i.price + Rep.raw(50) > Rep.raw(150), Seq("Gadget")),
    FilterCase("subtraction", i => i.price - i.price === Rep.raw(0), Seq("Widget", "Gadget", "Thingamajig")),
    FilterCase("multiplication", i => i.price * Rep.raw(2) > Rep.raw(150), Seq("Widget", "Gadget")),
    FilterCase("division", i => i.price / Rep.raw(10) >= Rep.raw(10), Seq("Widget", "Gadget")),
    FilterCase("modulo", i => i.price % Rep.raw(100) === Rep.raw(0), Seq("Widget", "Gadget")),
    FilterCase("unary negation", i => -i.price < Rep.raw(-50), Seq("Widget", "Gadget")),
    FilterCase("LIKE on string", _.name.like("W%"), Seq("Widget")),
    FilterCase("LIKE on optional string", _.description.like("%gadget%"), Seq("Gadget")),
    FilterCase("IN clause", _.price.in(Seq(100, 200)), Seq("Widget", "Gadget")),
    FilterCase("IN clause empty", _.price.in(Seq.empty[Int]), Seq.empty),
    FilterCase("IN on optional column", _.discount.in(Seq(10)), Seq("Widget")),
    FilterCase("BETWEEN", _.price.between(Rep.raw(50), Rep.raw(150)), Seq("Widget", "Thingamajig")),
    FilterCase(
      "combined arithmetic and LIKE",
      i => i.price * Rep.raw(2) > Rep.raw(100) && i.name.like("G%"),
      Seq("Gadget")
    ),
    FilterCase(
      "precedence of chained arithmetic",
      i => (i.price + Rep.raw(50)) * Rep.raw(2) > Rep.raw(150),
      Seq("Widget", "Gadget", "Thingamajig")
    ),
    FilterCase(
      "precedence of multiply before add",
      i => i.price + Rep.raw(50) * Rep.raw(2) > Rep.raw(150),
      Seq("Widget", "Gadget")
    )
  )

  cases.foreach { c =>
    it should s"support ${c.name}" in new Env {
      val results = Item.query.filter(c.filter).map(_.name).all()
      results should contain theSameElementsAs c.expected
    }
  }

  it should "support string concatenation" in new Env {
    val results = Item.query
      .filter(i => (i.name ++ Rep.raw(" item")).like("%Widget item%"))
      .map(_.name)
      .all()
    results shouldBe Seq("Widget")
  }

  it should "support arithmetic on optional columns" in new Env {
    val results = Item.query
      .filter(_.discount.isNotNull)
      .filter(i => i.discount + i.discount > Rep.rawOpt(12))
      .map(_.name)
      .all()
    results should contain theSameElementsAs Seq("Widget")
  }
}
