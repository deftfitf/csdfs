package csdfs.mysql

import csdfs.mysql.MySQLSchema.CreateDefinition

case class MySQLSchema(tblName: String, createDefinitions: Seq[CreateDefinition]) extends csdfs.Schema

object MySQLSchema {

  sealed trait CreateDefinition

  case class ColumnDef(
      columnName: String,
      dataType: DataType,
      notNull: Boolean,
      autoIncrement: Boolean,
      unique: Boolean
  ) extends CreateDefinition

  case class PrimaryKey(indexColNames: Seq[String]) extends CreateDefinition

  case class UniqueKey(indexColNames: Seq[String]) extends CreateDefinition

  case class ForeignKey(referenceTblName: String, indexColNames: Map[String, String]) extends CreateDefinition

}