package csdfs.mysql

import org.specs2.mutable.Specification

class MySQLCsdfsSpec extends Specification {

  val csdfs = new MySQLCsdfs(
    new MySQLSchemaParser {},
    new DependencyResolver
  )

  "#generateInsertStatements" should {

    "return insert statements" in {
      val r = csdfs.generateInsertStatements(
        Seq(
          """
            |create table table1 (
            |  id int not null auto_increment,
            |  column1 mediumint not null unique,
            |  column2 enum('value1', 'value2') null,
            |  column3 varchar(64) not null,
            |
            |  foreign key (column3)
            |  references table2 (column2)
            |)
          """.stripMargin,
          """
            |create table table2 (
            |  id int not null auto_increment,
            |  column1 char not null,
            |  column2 varchar(64) not null,
            |
            |  primary key (id, column1)
            |)
          """.stripMargin
        ),
        GenConf(
          Map((Table("table1"),
            GenConf.GenTableConf(Table("table1"), 10,
              Map((Column("column2"),
                GenConf.GenColumnConf(Column("column2"), cardinality = 2))))))
        )
      )
      println(r)
      r must beRight
    }

  }

}
