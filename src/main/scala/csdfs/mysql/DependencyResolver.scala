package csdfs.mysql

import csdfs.mysql.CsdfsError.IllegalTblDependency

private[mysql] class DependencyResolver {

  def resolveSchemaDependency(schemas: Seq[MySQLSchema]): Either[CsdfsError, SchemaDependency] = {
    val dependencyGraph: Map[Table, Seq[Table]] =
      schemas.map(s => (s.tbl, s.dependentTables)).toMap

    val dim =
      schemas.foldRight(Map.from(schemas.map(s => (s.tbl, 0))))((schema, m) =>
        schema.dependentTables.foldRight(m)((table, m) =>
          m.updated(table, m(table) + 1)))

    def topologicalSort(dim: Map[Table, Int], sorted: List[Table]): Either[CsdfsError, List[Table]] = {
      dim.find(d => d._2 == 0) match {
        case Some((t, _)) =>
          val updatedDim =
            dependencyGraph(t).foldRight(dim)((dependent, d) =>
              d.updated(dependent, d(dependent) - 1)).removed(t)
          val newSorted = t :: sorted
          topologicalSort(updatedDim, newSorted)

        case None =>
          if (dim.isEmpty) Right(sorted)
          else Left(IllegalTblDependency(s"circular dependency detected.: $dim"))
      }
    }

    topologicalSort(dim, Nil) map { sorted =>
      new SchemaDependency(sorted, schemas.map(s => (s.tbl, s)).toMap)
    }
  }

}