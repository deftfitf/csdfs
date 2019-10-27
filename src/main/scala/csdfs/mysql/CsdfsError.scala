package csdfs.mysql

sealed abstract class CsdfsError(val message: String) extends Exception

object CsdfsError {

  case class IllegalTblDependency(override val message: String) extends CsdfsError(message)

  case class SchemaParseError(override val message: String) extends CsdfsError(message)

  case class UniqueConstraintError(override val message: String) extends CsdfsError(message)

  case class GenerateConfigError(override val message: String) extends CsdfsError(message)

}