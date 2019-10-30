package csdfs.mysql

import java.nio.charset.Charset

import csdfs.mysql.{MySQLDataType => dt}

import scala.util.Random

sealed trait MySQLDataType

object MySQLDataType {

  case class Bit(unsigned: Boolean) extends MySQLDataType

  case class Tinyint(unsigned: Boolean) extends MySQLDataType

  case class Smallint(unsigned: Boolean) extends MySQLDataType

  case class Mediumint(unsigned: Boolean) extends MySQLDataType

  case class MySQLInt(unsigned: Boolean) extends MySQLDataType

  case class Integer(unsigned: Boolean) extends MySQLDataType

  case class Bigint(unsigned: Boolean) extends MySQLDataType

  case class Double(lenDec: Option[(scala.Int, scala.Int)], unsigned: Boolean) extends MySQLDataType

  case class Float(lenDec: Option[(scala.Int, scala.Int)], unsigned: Boolean) extends MySQLDataType

  case class Decimal(lenDec: Option[(scala.Int,Option[scala.Int])], unsigned: Boolean) extends MySQLDataType

  case class Numeric(lenDec: Option[(scala.Int,Option[scala.Int])], unsigned: Boolean) extends MySQLDataType

  case object Date extends MySQLDataType

  case object Time extends MySQLDataType

  case object Timestamp extends MySQLDataType

  case object Datetime extends MySQLDataType

  case object Year extends MySQLDataType

  case class Char(length: Option[scala.Int] = Some(1), charset: Option[Charset]) extends MySQLDataType

  case class Varchar(length: Option[scala.Int] = Some(255), charset: Option[Charset]) extends MySQLDataType

  case class Binary(length: Option[scala.Int] = Some(1)) extends MySQLDataType

  case class Varbinary(length: Option[scala.Int] = Some(255)) extends MySQLDataType

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

  implicit def colGenerator(dataType: MySQLDataType): ColGenerator[_] =
    dataType match {
      case d: dt.Bit => d
      case d: dt.Tinyint => d
      case d: dt.Smallint => d
      case d: dt.Mediumint => d
      case d: dt.MySQLInt => d
      case d: dt.Integer => d
      case d: dt.Bigint => d
      case d: dt.Double => d
      case d: dt.Float => d
      case d: dt.Decimal => d
      case d: dt.Numeric => d
      case d: dt.Date.type => d
      case d: dt.Time.type => d
      case d: dt.Timestamp.type => d
      case d: dt.Datetime.type => d
      case d: dt.Year.type => d
      case d: dt.Char => d
      case d: dt.Varchar => d
      case d: dt.Binary => d
      case d: dt.Varbinary => d
      case d: dt.Tinyblob.type => d
      case d: dt.Blob.type => d
      case d: dt.Mediumblob.type => d
      case d: dt.Longblob.type => d
      case d: dt.Tinytext => d
      case d: dt.Text => d
      case d: dt.Mediumtext => d
      case d: dt.Longtext => d
      case d: dt.MEnum => d
      case d: dt.MSet => d
    }

  implicit def intColGenerator(int: dt.MySQLInt): ColGenerator[dt.MySQLInt] = new ColGenerator[dt.MySQLInt] {
    def next: String = {
      val n = if (int.unsigned)
        Math.abs(Random.nextInt(2147483647))
      else
        Random.nextInt(2147483647)
      n.toString
    }
  }
  implicit def bitColGenerator(bit: dt.Bit): ColGenerator[dt.Bit] = new ColGenerator[dt.Bit] {
    def next: String = {
      val n = if (bit.unsigned)
        Math.abs(Random.nextInt())
      else
        Random.nextInt()
      s"b'${n.toBinaryString}'"
    }
  }
  implicit def tinyintColGenerator(tinyint: dt.Tinyint): ColGenerator[dt.Tinyint] = new ColGenerator[dt.Tinyint] {
    def next: String = {
      val n = if (tinyint.unsigned)
        Math.abs(Random.nextInt(128))
      else
        Random.nextInt(128)
      n.toString
    }
  }
  implicit def smallintColGenerator(smallint: dt.Smallint): ColGenerator[dt.Smallint] = new ColGenerator[dt.Smallint] {
    def next: String = {
      val n = if (smallint.unsigned)
        Math.abs(Random.nextInt(32767))
      else
        Random.nextInt(32767)
      n.toString
    }
  }
  implicit def mediumintColGenerator(mediumint: dt.Mediumint): ColGenerator[dt.Mediumint] = new ColGenerator[dt.Mediumint] {
    def next: String = {
      val n = if (mediumint.unsigned)
        Math.abs(Random.nextInt(16777215))
      else
        Random.nextInt(16777215)
      n.toString
    }
  }
  implicit def integerColGenerator(integer: dt.Integer): ColGenerator[dt.Integer] = new ColGenerator[dt.Integer] {
    def next: String = {
      val n = if (integer.unsigned)
        Math.abs(Random.nextInt(2147483647))
      else
        Random.nextInt(2147483647)
      n.toString
    }
  }
  implicit def bigintColGenerator(bigint: dt.Bigint): ColGenerator[dt.Bigint] = new ColGenerator[dt.Bigint] {
    def next: String = {
      val n = if (bigint.unsigned)
        Math.abs(Random.nextLong(9223372036854775807L))
      else
        Random.nextLong(9223372036854775807L)
      n.toString
    }
  }
  implicit def doubleColGenerator(double: dt.Double): ColGenerator[dt.Double] = new ColGenerator[dt.Double] {
    def next: String = {
      val n = if (double.unsigned)
        Math.abs(Random.nextDouble())
      else
        Random.nextDouble()
      n.toString
    }
  }
  implicit def floatColGenerator(float: dt.Float): ColGenerator[dt.Float] = new ColGenerator[dt.Float] {
    def next: String = {
      val n = if (float.unsigned)
        Math.abs(Random.nextFloat())
      else
        Random.nextFloat()
      n.toString
    }
  }
  implicit def decimalColGenerator(decimal: dt.Decimal): ColGenerator[dt.Decimal] = new ColGenerator[dt.Decimal] {
    def next: String = {
      val n = if (decimal.unsigned)
        Math.abs(Random.nextFloat())
      else
        Random.nextFloat()
      n.toString
    }
  }
  implicit def numericColGenerator(numeric: dt.Numeric): ColGenerator[dt.Numeric] = new ColGenerator[dt.Numeric] {
    def next: String = {
      val n = if (numeric.unsigned)
        Math.abs(Random.nextFloat())
      else
        Random.nextFloat()
      n.toString
    }
  }
  implicit def dateColGenerator(date: dt.Date.type): ColGenerator[dt.Date.type] = new ColGenerator[dt.Date.type] {
    def next: String = s"'${java.sql.Date.valueOf(java.time.LocalDate.now()).toString}'"
  }
  implicit def timeColGenerator(time: dt.Time.type): ColGenerator[dt.Time.type] = new ColGenerator[dt.Time.type] {
    def next: String = s"'${java.sql.Time.valueOf(java.time.LocalTime.now()).toString}'"
  }
  implicit def timestampColGenerator(timestamp: dt.Timestamp.type): ColGenerator[dt.Timestamp.type] = new ColGenerator[dt.Timestamp.type] {
    def next: String = s"'${java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()).toString}'"
  }
  implicit def datetimeColGenerator(datetime: dt.Datetime.type): ColGenerator[dt.Datetime.type] = new ColGenerator[dt.Datetime.type] {
    def next: String = s"'${java.time.LocalDateTime.now().formatted("YYYY-MM-DD HH:MM:SS")}'"
  }
  implicit def yearColGenerator(year: dt.Year.type): ColGenerator[dt.Year.type] = new ColGenerator[dt.Year.type] {
    def next: String = s"'${java.time.LocalDate.now().formatted("YYYY")}'"
  }
  implicit def charColGenerator(char: dt.Char): ColGenerator[dt.Char] = new ColGenerator[dt.Char] {
    def next: String = {
      val len = char.length.getOrElse(1)
      s"'${Random.alphanumeric.take(len).mkString}'"
    }
  }
  implicit def varcharColGenerator(varchar: dt.Varchar): ColGenerator[dt.Varchar] = new ColGenerator[dt.Varchar] {
    def next: String = {
      val len = varchar.length.getOrElse(255)
      s"'${Random.alphanumeric.take(len).mkString}'"
    }
  }
  implicit def binaryColGenerator(binary: dt.Binary): ColGenerator[dt.Binary] = new ColGenerator[dt.Binary] {
    def next: String = {
      val len = binary.length.getOrElse(1)
      s"'${Random.alphanumeric.take(len).mkString}'"
    }
  }
  implicit def varbinaryColGenerator(varbinary: dt.Varbinary): ColGenerator[dt.Varbinary] = new ColGenerator[dt.Varbinary] {
    def next: String = {
      val len = varbinary.length.getOrElse(255)
      s"'${Random.alphanumeric.take(len).mkString}'"
    }
  }
  implicit def tinyblobColGenerator(tinyblob: dt.Tinyblob.type): ColGenerator[dt.Tinyblob.type] = new ColGenerator[dt.Tinyblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def blobColGenerator(blob: dt.Blob.type): ColGenerator[dt.Blob.type] = new ColGenerator[dt.Blob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def mediumblobColGenerator(mediumblob: dt.Mediumblob.type): ColGenerator[dt.Mediumblob.type] = new ColGenerator[dt.Mediumblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def longblobColGenerator(longblob: dt.Longblob.type): ColGenerator[dt.Longblob.type] = new ColGenerator[dt.Longblob.type] {
    def next: String = s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def tinytextColGenerator(tinytext: dt.Tinytext): ColGenerator[dt.Tinytext] = new ColGenerator[dt.Tinytext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def textColGenerator(text: dt.Text): ColGenerator[dt.Text] = new ColGenerator[dt.Text] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def mediumtextColGenerator(mediumtext: dt.Mediumtext): ColGenerator[dt.Mediumtext] = new ColGenerator[dt.Mediumtext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def longtextColGenerator(longtext: dt.Longtext): ColGenerator[dt.Longtext] = new ColGenerator[dt.Longtext] {
    def next: String =  s"'${Random.alphanumeric.take(20).mkString}'"
  }
  implicit def enumColGenerator(mEnum: MEnum): ColGenerator[dt.MEnum] = new ColGenerator[dt.MEnum] {
    override def next: String = Random.shuffle(mEnum.values).head
  }
  implicit def setColGenerator(mSet: MSet): ColGenerator[dt.MSet] = new ColGenerator[dt.MSet] {
    override def next: String = Random.shuffle(mSet.values).head
  }

}