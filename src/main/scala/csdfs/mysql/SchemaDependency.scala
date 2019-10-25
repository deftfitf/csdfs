package csdfs.mysql

class SchemaDependency(
                        topologicalSorted: Seq[Table],
                        tables: Map[Table, MySQLSchema]) {

  def generateSampleDataInsertStatement(): Seq[String] = {
    val (_, r) = topologicalSorted.foldLeft((Map.empty[(Table, Column), Seq[String]], List.empty[String])) { case (b, table) =>
      val (m, n) = tables(table).generateSampleData(b._1)
      println(n)
      (n, m :: b._2)
    }
    r
  }

}