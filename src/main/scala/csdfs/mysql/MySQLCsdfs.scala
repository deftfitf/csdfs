package csdfs.mysql

class MySQLCsdfs(
    schemaParser: MySQLSchemaParser,
    dependencyResolver: DependencyResolver) {
  
  private def parseAllSchemas(schemas: Seq[String]): Either[CsdfsError, Seq[MySQLSchema]] =
    schemas.foldRight(Right(List.empty[MySQLSchema]): Either[CsdfsError, List[MySQLSchema]])((s, r) =>
      r match {
        case Right(rr) =>
          schemaParser.parsing(s) match {
            case Right(nr) => Right(nr :: rr)
            case Left(e) => Left(e)
          }
        case e @ Left(_) => e
      })

  def generateInsertStatements(schemas: Seq[String], genConf: GenConf): Either[CsdfsError, Seq[String]] = {
    for {
      parsedSchemas <- parseAllSchemas(schemas)
      resolvedDependency <- dependencyResolver.resolveSchemaDependency(parsedSchemas)
      generatedInsertStatements <- resolvedDependency.generateSampleDataInsertStatement(genConf)
    } yield generatedInsertStatements
    
  }

}

