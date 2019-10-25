package csdfs.mysql

import csdfs.mysql.MySQLSchema.{ColumnDef, ForeignKey}
import org.specs2.mutable.Specification

class DependencyResolverSpec extends Specification {

  val resolver = new DependencyResolver

  "#resolveSchemaDependency" should {

    "return resolve dependency" in {
      val independent =
        MySQLSchema(
          Table("independent"),
          Seq(
            ColumnDef(Column("col1"), MySQLDataType.Int, true, false, false),
            ForeignKey(Table("dependent"), Map((Column("col1"), Column("col1"))))))
      val dependent =
        MySQLSchema(
          Table("dependent"),
          Seq(
            ColumnDef(Column("col1"), MySQLDataType.Int, true, false, false)))
      val schemas =
        Seq(independent, dependent)

      resolver.resolveSchemaDependency(schemas) must beRight
    }

    "return IllegalTblDependency" +
      "when there is a circular dependency in the schema of the argument" in {
      val cyclic =
        MySQLSchema(
          Table("cyclic"),
          Seq(
            ColumnDef(Column("col1"), MySQLDataType.Int, true, false, false),
            ForeignKey(Table("cyclic"), Map((Column("col1"), Column("col1"))))))

      resolver.resolveSchemaDependency(Seq(cyclic)) must beLeft
    }

  }

}
