package csdfs.mysql

import csdfs.mysql.MySQLSchema.{ColumnDef, ForeignKey}
import org.specs2.mutable.Specification

class MySQLSchemaParserSpec extends Specification {

  "#parseAll" should {

    "return MySQLSchema" in {
      val expected =
        MySQLSchema(
          "table",
          List(
            ColumnDef("id",DataType.Int,true,true,false),
            ColumnDef("column1",DataType.Mediumint,true,false,false),
            ColumnDef("column2",DataType.MEnum(Set("value1", "value2"), None),false,false,true),
            ForeignKey("foreign_table",Map("column2" -> "fr_column"))))
      val parsed = MySQLSchemaParser.parse(
        """
          |create table table (
          |  id int not null auto_increment,
          |  column1 mediumint not null,
          |  column2 enum('value1', 'value2') null unique,
          |
          |  foreign key (column2)
          |  references foreign_table (fr_column)
          |)
        """.stripMargin
      )
      parsed must beRight(expected)
    }

  }

}
