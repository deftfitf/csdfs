package csdfs.mysql

class SchemaDependency(
                        topologicalSorted: Seq[Table],
                        tables: Map[Table, MySQLSchema]) {

  def generateSampleDataInsertStatement(genConf: GenConf): Either[CsdfsError, Seq[String]] = {
    val empty: Either[CsdfsError, (Map[ForeignRef, Set[String]], List[String])] =
      Right(Map.empty[ForeignRef, Set[String]], List.empty[String])

    topologicalSorted.foldLeft(empty) { case (b, table) =>
      b match {
        case Right(b) =>
          tables(table).generateSampleData(genConf, b._1). map { r =>
            (r._2, (r._1 :: b._2))
          }
        case l => l
      }
    } map (_._2)
  }

}