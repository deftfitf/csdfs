package csdfs.mysql

import java.nio.charset.Charset

import csdfs.mysql.CsdfsError.SchemaParseError
import csdfs.mysql.MySQLSchema._
import csdfs.mysql.{MySQLDataType => dt}

import scala.util.parsing.combinator.JavaTokenParsers

protected[csdfs] trait MySQLSchemaParser
    extends JavaTokenParsers {

  def parsing(schema: String): Either[CsdfsError, MySQLSchema] =
    parseAll(expr, schema) match {
      case Success(s, _) => Right(s)
      case Failure(e, _) => Left(SchemaParseError(e))
      case Error(e, _) => Left(SchemaParseError(e))
    }

  private def expr: Parser[MySQLSchema] =
    "CREATE" ~> "TEMPORARY".? ~> "TABLE" ~> ("IF" ~> "NOT" ~> "EXISTS").? ~> stringLiteral ~
      ("(" ~> rep1sep(createDefinition, ",") <~ ")") ^^ { case tblName ~ createDefinitions =>
        MySQLSchema(Table(tblName), createDefinitions)
    }

  private def createDefinition: Parser[CreateDefinition] =
    columnDefinition | primaryKeyConstraint | indexDefinition | uniqueKeyConstraint | foreighKeyConstraint

  private def columnDefinition: Parser[ColumnDef] =
    stringLiteral ~ dataType ~ notNullConstraint ~ autoIncrement ~ uniqueConstraint ^^
      { case columnName ~ dataType ~ notNullConstraint ~ autoIncrement ~ uniqueConstraint =>
        ColumnDef(
          column = Column(columnName),
          dataType = dataType,
          notNull = notNullConstraint,
          autoIncrement = autoIncrement,
          unique = uniqueConstraint)
    }

  private def dataType: Parser[MySQLDataType] =
    bit | tinyint | smallint | mediumint | integer | int |
      bigint | double | float | decimal | numeric | date |
      time | timestamp | datetime | year | char | varchar | binary |
      varbinary | tinyblob | blob | mediumblob | longblob | tinytext |
      text | mediumtext | longtext | enum | set

  private def bit: Parser[dt.Bit] = "BIT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Bit(unsigned))
  private def tinyint: Parser[dt.Tinyint] = "TINYINT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Tinyint(unsigned))
  private def smallint: Parser[dt.Smallint] = "SMALLINT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Smallint(unsigned))
  private def mediumint: Parser[dt.Mediumint] = "MEDIUMINT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Mediumint(unsigned))
  private def int: Parser[dt.MySQLInt] = "INT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.MySQLInt(unsigned))
  private def integer: Parser[dt.Integer] = "INTEGER" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Integer(unsigned))
  private def bigint: Parser[dt.Bigint] = "BIGINT" ~> dataTypeLength.? ~> unsigned <~ zeroFill ^^ (unsigned => dt.Bigint(unsigned))
  private def double: Parser[dt.Double] = "DOUBLE" ~> lengthDecimal.? ~ unsigned <~ zeroFill ^^ { case ld ~ unsigned => dt.Double(ld, unsigned) }
  private def float: Parser[dt.Float] = "FLOAT" ~> lengthDecimal.? ~ unsigned <~ zeroFill ^^ { case ld ~ unsigned => dt.Float(ld, unsigned) }
  private def decimal: Parser[dt.Decimal] = "DECIMAL" ~> lengthMaybeDecimal.? ~ unsigned <~ zeroFill ^^ { case ld ~ unsigned => dt.Decimal(ld, unsigned) }
  private def numeric: Parser[dt.Numeric] = "NUMERIC" ~> lengthMaybeDecimal.? ~ unsigned <~ zeroFill ^^ { case ld ~ unsigned => dt.Numeric(ld, unsigned) }
  private def date: Parser[dt.Date.type] = "DATE" ~> fsp.? ^^ (_ => dt.Date)
  private def time: Parser[dt.Time.type] = "TIME" ~> fsp.? ^^ (_ => dt.Time)
  private def timestamp: Parser[dt.Timestamp.type] = "TIMESTAMP" ~> fsp.? ^^ (_ => dt.Timestamp)
  private def datetime: Parser[dt.Datetime.type] = "DATETIME" ^^ (_ => dt.Datetime)
  private def year: Parser[dt.Year.type] = "YEAR" ^^ (_ => dt.Year)
  private def char: Parser[dt.Char] = "CHAR" ~> dataTypeLength.? ~ charset.? ^^ { case len ~ charset => dt.Char(len, charset.flatten) }
  private def varchar: Parser[dt.Varchar] = "VARCHAR" ~> dataTypeLength.? ~ charset.? ^^ { case len ~ charset => dt.Varchar(len, charset.flatten) }
  private def binary: Parser[dt.Binary] = "BINARY" ~> dataTypeLength.? ^^ (len => dt.Binary(len))
  private def varbinary: Parser[dt.Varbinary] = "VARBINARY" ~> dataTypeLength.? ^^ (len => dt.Varbinary(len))
  private def tinyblob: Parser[dt.Tinyblob.type] = "TINYBLOB" ^^ (_ => dt.Tinyblob)
  private def blob: Parser[dt.Blob.type] = "BLOB" ^^ (_ => dt.Blob)
  private def mediumblob: Parser[dt.Mediumblob.type] = "MEDIUMBLOB" ^^ (_ => dt.Mediumblob)
  private def longblob : Parser[dt.Longblob.type] = "LONGBLOB " ^^ (_ => dt.Longblob)
  private def tinytext: Parser[dt.Tinytext] = "TINYTEXT" ~> binaryFlag ~> charset.? ^^ (charset => dt.Tinytext(charset.flatten))
  private def text: Parser[dt.Text] = "TEXT" ~> binaryFlag ~> charset.? ^^ (charset => dt.Text(charset.flatten))
  private def mediumtext: Parser[dt.Mediumtext] = "MEDIUMTEXT" ~> binaryFlag ~> charset.? ^^ (charset => dt.Mediumtext(charset.flatten))
  private def longtext: Parser[dt.Longtext] = "LONGTEXT" ~> binaryFlag ~> charset.? ^^ (charset => dt.Longtext(charset.flatten))
  private def enum: Parser[dt.MEnum] = "ENUM" ~> ("(" ~> rep1sep(mySqlString, ",") <~ ")") ~ charset.? ^^
    { case values ~ charset => dt.MEnum(values.toSet, charset.flatten) }
  private def set: Parser[dt.MSet] = "SET" ~> ("(" ~> rep1sep(mySqlString, ",") <~ ")") ~ charset.? ^^
    { case values ~ charset => dt.MSet(values.toSet, charset.flatten) }
  private def dataTypeLength: Parser[Int] = "(" ~> wholeNumber <~ ")" ^^ (_.toInt)
  private def fsp: Parser[Int] = "(" ~> wholeNumber <~ ")" ^^ (_.toInt)
  private def unsigned: Parser[Boolean] = "UNSIGNED".? ^^ (unsigned => unsigned.fold(false)(_ => true))
  private def zeroFill: Parser[Boolean] = "ZEROFILL".? ^^ (zeroFill => zeroFill.fold(false)(_ => true))
  private def lengthDecimal: Parser[(Int, Int)] = "(" ~> wholeNumber ~ ("," ~> wholeNumber) <~ ")" ^^ (n => (n._1.toInt, n._2.toInt))
  private def lengthMaybeDecimal: Parser[(Int, Option[Int])] = "(" ~> wholeNumber ~ ("," ~> wholeNumber).? <~ ")" ^^ (rr => (rr._1.toInt, rr._2.map(_.toInt)))
  private def binaryFlag: Parser[Boolean] = "BINARY".? ^^ (zeroFill => zeroFill.fold(false)(_ => true))

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
    ("CONSTRAINT" ~> stringLiteral.?).? ~> "PRIMARY" ~> "KEY" ~> indexType.? ~> "(" ~> rep1sep(stringLiteral, ",") <~ ")" ^^ { keys =>
      PrimaryKey(keys.map(Column))
    }

  private def indexDefinition: Parser[CreateDefinition] = ("INDEX" | "KEY") ~> stringLiteral.? ~> indexType.? ~> "(" ~> rep1sep(stringLiteral, ",") ~> ")" ^^ (_ => Index)

  private def uniqueKeyConstraint: Parser[UniqueKey] =
    ("CONSTRAINT" ~> stringLiteral.?).? ~> "UNIQUE" ~> "KEY" ~>
      ("INDEX" | "KEY").? ~> stringLiteral.? ~> indexType.? ~>
      "(" ~> rep1sep(stringLiteral, ",") <~ ")" ^^ { keys =>
      UniqueKey(keys.map(Column))
    }

  private def foreighKeyConstraint: Parser[ForeignKey] =
    ("CONSTRAINT" ~> stringLiteral.?).? ~> "FOREIGN" ~> "KEY" ~> stringLiteral.? ~>
      ("(" ~> rep1sep(stringLiteral, ",") <~ ")") ~ ("REFERENCES" ~> stringLiteral ~
        ("(" ~> rep1sep(stringLiteral, ",") <~ ")") <~
      (("ON" ~> "DELETE" ~> stringLiteral).? ~ ("ON" ~> "UPDATE" ~> stringLiteral).?)) ^^ {
      case thisTblKeys ~ (refTblName ~ refTblKeys) =>
        ForeignKey(
          Table(refTblName),
          thisTblKeys.map(Column)
            .zip(refTblKeys.map(Column)).toMap)
    }

  private def indexType: Parser[String] = "USING" ~> ("BTREE" | "HASH")

  private val ifSomeTrueElseFalse: Option[_] => Boolean = _.fold(false)(_ => true)

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
