package csdfs.mysql

import org.specs2.mutable.Specification

class MySQLCsdfsSpec extends Specification {

  val csdfs = new MySQLCsdfs(
    new MySQLSchemaParser {},
    new DependencyResolver
  )

  "#generateInsertStatements" should {

    "return insert statements" in {
      csdfs.generateInsertStatements(
        Seq(
          """
            |create table table1 (
            |  id int not null auto_increment,
            |  column1 mediumint not null,
            |  column2 enum('value1', 'value2') null unique,
            |  column3 varchar not null,
            |
            |  foreign key (column3)
            |  references table2 (column2)
            |)
          """.stripMargin,
          """
            |create table table2 (
            |  id int not null auto_increment,
            |  column1 char not null,
            |  column2 varchar not null
            |)
          """.stripMargin
        )
      ) must beRight
    }

  }

}
