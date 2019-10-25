package csdfs.mysql

import csdfs.mysql.MySQLSchema.{ColumnDef, ForeignKey}
import org.specs2.mutable.Specification

class MySQLSchemaParserSpec extends Specification {

  val parser = new MySQLSchemaParser {}

  "#parseAll" should {

    "return MySQLSchema" in {
      val expected =
        MySQLSchema(
          Table("table"),
          List(
            ColumnDef(Column("id"),MySQLDataType.Int,true,true,false),
            ColumnDef(Column("column1"),MySQLDataType.Mediumint,true,false,false),
            ColumnDef(Column("column2"),MySQLDataType.MEnum(Set("value1", "value2"), None),false,false,true),
            ForeignKey(Table("foreign_table"),Map(Column("column2") -> Column("fr_column")))))
      val parsed = parser.parsing(
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
