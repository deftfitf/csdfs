package csdfs.mysql

import java.nio.charset.Charset

import csdfs.mysql.{MySQLDataType => dt}

import scala.util.Random

sealed trait MySQLDataType

object MySQLDataType {

  case object Bit extends MySQLDataType

  case object Tinyint extends MySQLDataType

  case object Smallint extends MySQLDataType

  case object Mediumint extends MySQLDataType

  case object Int extends MySQLDataType

  case object Integer extends MySQLDataType

  case object Bigint extends MySQLDataType

  case object Double extends MySQLDataType

  case object Float extends MySQLDataType

  case object Decimal extends MySQLDataType

  case object Numeric extends MySQLDataType

  case object Date extends MySQLDataType

  case object Time extends MySQLDataType

  case object Timestamp extends MySQLDataType

  case object Datetime extends MySQLDataType

  case object Year extends MySQLDataType

  case class Char(charset: Option[Charset]) extends MySQLDataType

  case class Varchar(charset: Option[Charset]) extends MySQLDataType

  case object Binary extends MySQLDataType

  case object Varbinary extends MySQLDataType

  case object Tinyblob extends MySQLDataType

  case object Blob extends MySQLDataType

  case object Mediumblob extends MySQLDataType

  case object Longblob extends MySQLDataType

  case class Tinytext(charset: Option[Charset]) extends MySQLDataType

  case class Text(charset: Option[Charset]) extends MySQLDataType

  case class Mediumtext(charset: Option[Charset]) extends MySQLDataType

  case class Longtext(charset: Option[Charset]) extends MySQLDataType

  case class MEnum(values: Set[String], charset: Option[Charset]) extends MySQLDataType

  case class MSet(values: Set[String], charset: Option[Charset]) extends MySQLDataType

  implicit def generatorOf[T <: MySQLDataType: ColGenerator](dataType: T) = implicitly[ColGenerator[T]]

  implicit def colGenerator(dataType: MySQLDataType): ColGenerator[_] =
    dataType match {
      case d: dt.Bit.type => generatorOf(d)
      case d: dt.Tinyint.type => generatorOf(d)
      case d: dt.Smallint.type => generatorOf(d)
      case d: dt.Mediumint.type => generatorOf(d)
      case d: dt.Int.type => generatorOf(d)
      case d: dt.Integer.type => generatorOf(d)
      case d: dt.Bigint.type => generatorOf(d)
      case d: dt.Double.type => generatorOf(d)
      case d: dt.Float.type => generatorOf(d)
      case d: dt.Decimal.type => generatorOf(d)
      case d: dt.Numeric.type => generatorOf(d)
      case d: dt.Date.type => generatorOf(d)
      case d: dt.Time.type => generatorOf(d)
      case d: dt.Timestamp.type => generatorOf(d)
      case d: dt.Datetime.type => generatorOf(d)
      case d: dt.Year.type => generatorOf(d)
      case d: dt.Char => generatorOf(d)
      case d: dt.Varchar => generatorOf(d)
      case d: dt.Binary.type => generatorOf(d)
      case d: dt.Varbinary.type => generatorOf(d)
      case d: dt.Tinyblob.type => generatorOf(d)
      case d: dt.Blob.type => generatorOf(d)
      case d: dt.Mediumblob.type => generatorOf(d)
      case d: dt.Longblob.type => generatorOf(d)
      case d: dt.Tinytext => generatorOf(d)
      case d: dt.Text => generatorOf(d)
      case d: dt.Mediumtext => generatorOf(d)
      case d: dt.Longtext => generatorOf(d)
      case d: dt.MEnum => enumColGenerator(d)
      case d: dt.MSet => setColGenerator(d)
    }

  implicit val intColGenerator: ColGenerator[dt.Int.type] = new ColGenerator[dt.Int.type] {
    def next: String = Random.nextInt(2147483647).toString
  }
  implicit val bitColGenerator: ColGenerator[dt.Bit.type] = new ColGenerator[dt.Bit.type] {
    def next: String = s"b'${Random.nextInt().toBinaryString}'"
  }
  implicit val tinyintColGenerator: ColGenerator[dt.Tinyint.type] = new ColGenerator[dt.Tinyint.type] {
    def next: String = Random.nextInt(128).toString
  }
  implicit val smallintColGenerator: ColGenerator[dt.Smallint.type] = new ColGenerator[dt.Smallint.type] {
    def next: String = Random.nextInt(32767).toString
  }
  implicit val mediumintColGenerator: ColGenerator[dt.Mediumint.type] = new ColGenerator[dt.Mediumint.type] {
    def next: String = Random.nextInt(16777215).toString
  }
  implicit val integerColGenerator: ColGenerator[dt.Integer.type] = new ColGenerator[dt.Integer.type] {
    def next: String = Random.nextInt(2147483647).toString
  }
  implicit val bigintColGenerator: ColGenerator[dt.Bigint.type] = new ColGenerator[dt.Bigint.type] {
    def next: String = Random.nextLong(9223372036854775807L).toString
  }
  implicit val doubleColGenerator: ColGenerator[dt.Double.type] = new ColGenerator[dt.Double.type] {
    def next: String = Random.nextDouble().toString
  }
  implicit val floatColGenerator: ColGenerator[dt.Float.type] = new ColGenerator[dt.Float.type] {
    def next: String = Random.nextFloat().toString
  }
  implicit val decimalColGenerator: ColGenerator[dt.Decimal.type] = new ColGenerator[dt.Decimal.type] {
    def next: String = Random.nextFloat().toString
  }
  implicit val numericColGenerator: ColGenerator[dt.Numeric.type] = new ColGenerator[dt.Numeric.type] {
    def next: String = Random.nextFloat().toString
  }
  implicit val dateColGenerator: ColGenerator[dt.Date.type] = new ColGenerator[dt.Date.type] {
    def next: String = s"'${java.sql.Date.valueOf(java.time.LocalDate.now()).toString}'"
  }
  implicit val timeColGenerator: ColGenerator[dt.Time.type] = new ColGenerator[dt.Time.type] {
    def next: String = s"'${java.sql.Time.valueOf(java.time.LocalTime.now()).toString}'"
  }
  implicit val timestampColGenerator: ColGenerator[dt.Timestamp.type] = new ColGenerator[dt.Timestamp.type] {
    def next: String = s"'${java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()).toString}'"
  }
  implicit val datetimeColGenerator: ColGenerator[dt.Datetime.type] = new ColGenerator[dt.Datetime.type] {
    def next: String = s"'${java.time.LocalDateTime.now().formatted("YYYY-MM-DD HH:MM:SS")}'"
  }
  implicit val yearColGenerator: ColGenerator[dt.Year.type] = new ColGenerator[dt.Year.type] {
    def next: String = s"'${java.time.LocalDate.now().formatted("YYYY")}'"
  }
  implicit val charColGenerator: ColGenerator[dt.Char] = new ColGenerator[dt.Char] {
    def next: String = s"'${Random.alphanumeric.head}'"
  }
  implicit val varcharColGenerator: ColGenerator[dt.Varchar] = new ColGenerator[dt.Varchar] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val binaryColGenerator: ColGenerator[dt.Binary.type] = new ColGenerator[dt.Binary.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val varbinaryColGenerator: ColGenerator[dt.Varbinary.type] = new ColGenerator[dt.Varbinary.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val tinyblobColGenerator: ColGenerator[dt.Tinyblob.type] = new ColGenerator[dt.Tinyblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val blobColGenerator: ColGenerator[dt.Blob.type] = new ColGenerator[dt.Blob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val mediumblobColGenerator: ColGenerator[dt.Mediumblob.type] = new ColGenerator[dt.Mediumblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val longblobColGenerator: ColGenerator[dt.Longblob.type] = new ColGenerator[dt.Longblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val tinytextColGenerator: ColGenerator[dt.Tinytext] = new ColGenerator[dt.Tinytext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val textColGenerator: ColGenerator[dt.Text] = new ColGenerator[dt.Text] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val mediumtextColGenerator: ColGenerator[dt.Mediumtext] = new ColGenerator[dt.Mediumtext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit val longtextColGenerator: ColGenerator[dt.Longtext] = new ColGenerator[dt.Longtext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def enumColGenerator(mEnum: MEnum): ColGenerator[dt.MEnum] = new ColGenerator[dt.MEnum] {
    override def next: String = Random.shuffle(mEnum.values).head
  }
  implicit def setColGenerator(mSet: MSet): ColGenerator[dt.MSet] = new ColGenerator[dt.MSet] {
    override def next: String = Random.shuffle(mSet.values).head
  }

}