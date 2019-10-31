package csdfs.mysql

import csdfs.mysql.MySQLSchema.{ColumnDef, ForeignKey, Index, PrimaryKey}
import org.specs2.mutable.Specification

class MySQLSchemaParserSpec extends Specification {

  val parser = new MySQLSchemaParser {}

  "#parseAll" should {

    "return MySQLSchema" in {
      val expected =
        MySQLSchema(
          Table("table"),
          List(
            ColumnDef(Column("id"),MySQLDataType.MySQLInt(false),true,true,false),
            ColumnDef(Column("column1"),MySQLDataType.Mediumint(true),true,false,false),
            ColumnDef(Column("column2"),MySQLDataType.MEnum(Set("value1", "value2"), None),false,false,true),
            ColumnDef(Column("column3"),MySQLDataType.Varchar(Some(64), None), true, false, false),
            ColumnDef(Column("column4"),MySQLDataType.Decimal(Some(2, Some(1)), false), false, false, false),
            ColumnDef(Column("column5"),MySQLDataType.Double(None, true), true, false, false),
            ColumnDef(Column("column6"),MySQLDataType.Time, false, false, false),
            ColumnDef(Column("column7"),MySQLDataType.Integer(true), true, false, false),
            PrimaryKey(Seq(Column("id"), Column("column1"))),
            Index,
            ForeignKey(Table("foreign_table"),Map(Column("column2") -> Column("fr_column"))),
            ForeignKey(Table("foreign_table2"),Map(Column("column7") -> Column("fr_column")))))
      val parsed = parser.parsing(
        """
          |create table table (
          |  id int not null auto_increment,
          |  column1 mediumint unsigned not null,
          |  column2 enum('value1', 'value2') null unique,
          |  column3 VARCHAR(64) NOT NULL,
          |  column4 decimal(2,1),
          |  column5 double unsigned not null,
          |  column6 TIME(4),
          |  column7 integer(8) unsigned not null,
          |
          |  primary key (id, column1),
          |  index idx_column3 (column3),
          |
          |  foreign key (column2)
          |  references foreign_table (fr_column),
          |
          |  constraint fk_column7 foreign key (column7)
          |  references foreign_table2 (fr_column)
          |)
        """.stripMargin
      )
      parsed must beRight(expected)
    }

  }

}
