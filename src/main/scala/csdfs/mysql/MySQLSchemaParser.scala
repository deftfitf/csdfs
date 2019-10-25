package csdfs.mysql

import java.nio.charset.Charset

import csdfs.SchemaParser
import csdfs.mysql.MySQLSchema._
import csdfs.mysql.{DataType => dt}

import scala.util.parsing.combinator.JavaTokenParsers

protected[csdfs] object MySQLSchemaParser
    extends JavaTokenParsers
    with SchemaParser[MySQLSchema] {

  def parse(schema: String): Either[Throwable, MySQLSchema] =
    parseAll(expr, schema) match {
      case Success(s, _) => Right(s)
      case Failure(e, _) => Left(new Throwable(e))
    }

  private def expr: Parser[MySQLSchema] =
    "CREATE" ~> "TEMPORARY".? ~> "TABLE" ~> ("IF" ~> "NOT" ~> "EXISTS").? ~> stringLiteral ~
      ("(" ~> rep1sep(createDefinition, ",") <~ ")") ^^ { case tblName ~ createDefinitions =>
        MySQLSchema(tblName, createDefinitions)
    }

  private def createDefinition: Parser[CreateDefinition] =
    columnDefinition | primaryKeyConstraint | uniqueKeyConstraint | foreighKeyConstraint

  private def columnDefinition: Parser[ColumnDef] =
    stringLiteral ~ dataType ~ notNullConstraint ~ autoIncrement ~ uniqueConstraint ^^
      { case columnName ~ dataType ~ notNullConstraint ~ autoIncrement ~ uniqueConstraint =>
        ColumnDef(
          columnName = columnName,
          dataType = dataType,
          notNull = notNullConstraint,
          autoIncrement = autoIncrement,
          unique = uniqueConstraint)
    }

  private def dataType: Parser[DataType] =
    bit | tinyint | smallint | mediumint | int | integer |
      bigint | double | float | decimal | numeric | date |
      time | timestamp | datetime | year | char | varchar | binary |
      varbinary | tinyblob | blob | mediumblob | longblob | tinytext |
      text | mediumtext | longtext | enum | set

  private def bit: Parser[dt.Bit.type] = "BIT" ^^ (_ => dt.Bit)
  private def tinyint: Parser[dt.Tinyint.type] = "TINYINT" ^^ (_ => dt.Tinyint)
  private def smallint: Parser[dt.Smallint.type] = "SMALLINT" ^^ (_ => dt.Smallint)
  private def mediumint: Parser[dt.Mediumint.type] = "MEDIUMINT" ^^ (_ => dt.Mediumint)
  private def int: Parser[dt.Int.type] = "INT" ^^ (_ => dt.Int)
  private def integer: Parser[dt.Integer.type] = "INTEGER" ^^ (_ => dt.Integer)
  private def bigint: Parser[dt.Bigint.type] = "BIGINT" ^^ (_ => dt.Bigint)
  private def double: Parser[dt.Double.type] = "DOUBLE" ^^ (_ => dt.Double)
  private def float: Parser[dt.Float.type] = "FLOAT" ^^ (_ => dt.Float)
  private def decimal: Parser[dt.Decimal.type] = "DECIMAL" ^^ (_ => dt.Decimal)
  private def numeric: Parser[dt.Numeric.type] = "NUMERIC" ^^ (_ => dt.Numeric)
  private def date: Parser[dt.Date.type] = "DATE" ^^ (_ => dt.Date)
  private def time: Parser[dt.Time.type] = "TIME" ^^ (_ => dt.Time)
  private def timestamp: Parser[dt.Timestamp.type] = "TIMESTAMP" ^^ (_ => dt.Timestamp)
  private def datetime: Parser[dt.Datetime.type] = "DATETIME" ^^ (_ => dt.Datetime)
  private def year: Parser[dt.Year.type] = "YEAR" ^^ (_ => dt.Year)
  private def char: Parser[dt.Char] = "CHAR" ~> charset.? ^^ (charset => dt.Char(charset.flatten))
  private def varchar: Parser[dt.Varchar] = "VARCHAR" ~> charset.? ^^ (charset => dt.Varchar(charset.flatten))
  private def binary: Parser[dt.Binary.type] = "BINARY" ^^ (_ => dt.Binary)
  private def varbinary: Parser[dt.Varbinary.type] = "VARBINARY" ^^ (_ => dt.Varbinary)
  private def tinyblob: Parser[dt.Tinyblob.type] = "TINYBLOB" ^^ (_ => dt.Tinyblob)
  private def blob: Parser[dt.Blob.type] = "BLOB" ^^ (_ => dt.Blob)
  private def mediumblob: Parser[dt.Mediumblob.type] = "MEDIUMBLOB" ^^ (_ => dt.Mediumblob)
  private def longblob : Parser[dt.Longblob.type] = "LONGBLOB " ^^ (_ => dt.Longblob)
  private def tinytext: Parser[dt.Tinytext] = "TINYTEXT" ~> charset.? ^^ (charset => dt.Tinytext(charset.flatten))
  private def text: Parser[dt.Text] = "TEXT" ~> charset.? ^^ (charset => dt.Text(charset.flatten))
  private def mediumtext: Parser[dt.Mediumtext] = "MEDIUMTEXT" ~> charset.? ^^ (charset => dt.Mediumtext(charset.flatten))
  private def longtext: Parser[dt.Longtext] = "LONGTEXT" ~> charset.? ^^ (charset => dt.Longtext(charset.flatten))
  private def enum: Parser[dt.MEnum] = "ENUM" ~> ("(" ~> rep1sep(mySqlString, ",") <~ ")") ~ charset.? ^^
    { case values ~ charset => dt.MEnum(values.toSet, charset.flatten) }
  private def set: Parser[dt.MSet] = "SET" ~> ("(" ~> rep1sep(mySqlString, ",") <~ ")") ~ charset.? ^^
    { case values ~ charset => dt.MSet(values.toSet, charset.flatten) }

  private def quoto: Parser[String] = "'" | "\""

  private def mySqlString: Parser[String] = quoto ~> stringLiteral <~ quoto

  override def stringLiteral: Parser[String] = """\w+""".r

  private def charset: Parser[Option[Charset]] =
    "CHARACTER" ~> "SET" ~> stringLiteral ^^ { charset =>
      if (Charset.isSupported(charset)) Some(Charset.forName(charset))
      else None
    }

  private def notNullConstraint: Parser[Boolean] =
    (("NOT" ~> "NULL" ).^^^(true) | "NULL".^^^(false)).?.^^(r => r.getOrElse(false))

  private def autoIncrement: Parser[Boolean] =
    "AUTO_INCREMENT".? ^^ ifSomeTrueElseFalse

  private def uniqueConstraint: Parser[Boolean] =
    "UNIQUE".? ^^ ifSomeTrueElseFalse

  private def primaryKeyConstraint: Parser[PrimaryKey] =
    ("CONSTRAINT" ~> stringLiteral).? ~> "PRIMARY" ~> "KEY" ~> stringLiteral.? ~> ("(" ~> rep1sep(stringLiteral, ",") <~ ")") ^^ { keys =>
      PrimaryKey(keys)
    }

  private def uniqueKeyConstraint: Parser[UniqueKey] =
    ("CONSTRAINT" ~> stringLiteral).? ~> "UNIQUE" ~> "KEY" ~>
      ("INDEX" | "KEY").? ~> stringLiteral.? ~> stringLiteral.? ~>
      ("(" ~> rep1sep(stringLiteral, ",") <~ ")") ^^ { keys =>
      UniqueKey(keys)
    }

  private def foreighKeyConstraint: Parser[ForeignKey] =
    ("CONSTRAINT" ~> stringLiteral).? ~> "FOREIGN" ~> "KEY" ~> stringLiteral.? ~>
      ("(" ~> rep1sep(stringLiteral, ",") <~ ")") ~ ("REFERENCES" ~> stringLiteral ~
        ("(" ~> rep1sep(stringLiteral, ",") <~ ")") <~
      (("ON" ~> "DELETE" ~> stringLiteral).? ~ ("ON" ~> "UPDATE" ~> stringLiteral).?)) ^^ {
      case thisTblKeys ~ (refTblName ~ refTblKeys) =>
        ForeignKey(refTblName, thisTblKeys.zip(refTblKeys).toMap)
    }

  private val ifSomeTrueElseFalse: Option[_] => Boolean = _.fold(false)(_ => true)

  private def comma = ","

  override val whiteSpace = """\s+|\r|\n|\r\n""".r

  implicit def ignoreUpperLowerLiteral(s: String): Parser[String] = new Parser[String] {
    override def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = handleWhiteSpace(source, offset)
      var i = 0
      var j = start
      while (i < s.length && j < source.length && s.charAt(i).toUpper == source.charAt(j).toUpper) {
        i += 1
        j += 1
      }
      if (i == s.length)
        Success(source.subSequence(start, j).toString, in.drop(j - offset))
      else  {
        val found = if (start == source.length()) "end of source" else "'"+source.charAt(start)+"'"
        Failure("'"+s+"' expected but "+found+" found", in.drop(start - offset))
      }
    }
  }

}
