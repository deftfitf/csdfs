package csdfs.mysql

import java.nio.charset.Charset

sealed trait DataType

object DataType {

  case object Bit extends DataType

  case object Tinyint extends DataType

  case object Smallint extends DataType

  case object Mediumint extends DataType

  case object Int extends DataType

  case object Integer extends DataType

  case object Bigint extends DataType

  case object Double extends DataType

  case object Float extends DataType

  case object Decimal extends DataType

  case object Numeric extends DataType

  case object Date extends DataType

  case object Time extends DataType

  case object Timestamp extends DataType

  case object Datetime extends DataType

  case object Year extends DataType

  case class Char(charset: Option[Charset]) extends DataType

  case class Varchar(charset: Option[Charset]) extends DataType

  case object Binary extends DataType

  case object Varbinary extends DataType

  case object Tinyblob extends DataType

  case object Blob extends DataType

  case object Mediumblob extends DataType

  case object Longblob extends DataType

  case class Tinytext(charset: Option[Charset]) extends DataType

  case class Text(charset: Option[Charset]) extends DataType

  case class Mediumtext(charset: Option[Charset]) extends DataType

  case class Longtext(charset: Option[Charset]) extends DataType

  case class MEnum(values: Set[String], charset: Option[Charset]) extends DataType

  case class MSet(values: Set[String], charset: Option[Charset]) extends DataType

}