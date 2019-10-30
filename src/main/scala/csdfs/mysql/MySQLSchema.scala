package csdfs.mysql

import csdfs.mysql.CsdfsError.{GenerateConfigError, UniqueConstraintError}
import csdfs.mysql.MySQLSchema.{ColumnDef, CreateDefinition, ForeignKey, PrimaryKey, UniqueGroupConstraint, UniqueKey}
import csdfs.mysql.MySQLDataType._

// TODO: オートインクリメントの考慮, ユニーク制約の考慮, 実装リファクタ
case class MySQLSchema(tbl: Table, createDefinitions: Seq[CreateDefinition]) {

  val dependentTables: Seq[Table] =
    createDefinitions.collect {
      case ForeignKey(dependent, _) => dependent
    }

  private val columnDef: Seq[ColumnDef] =
    createDefinitions. collect {
      case d: ColumnDef => d
    }

  private val foreignKey: Seq[ForeignKey] =
    createDefinitions. collect {
      case fk: ForeignKey => fk
    }

  private val uniqueConstraintKey: Seq[UniqueGroupConstraint] =
    createDefinitions. collect {
      case uk: UniqueKey => uk: UniqueGroupConstraint
      case pk: PrimaryKey => pk: UniqueGroupConstraint
    }

  private val notUniqueConstraintKey: Seq[ColumnDef] =
    createDefinitions. collect {
      case columnDef: ColumnDef
        if !uniqueConstraintKey.exists(k => k.indexColNames.contains(columnDef.column)) =>
          columnDef
    }

  private def dependentColumnDefOrNot: (Seq[ColumnDef], Seq[ColumnDef]) = {
    val colFk = foreignKey.flatMap(_.indexColNames).toMap

    columnDef.partition { d =>
      colFk.contains(d.column)
    }
  }

  private val columnRefResolveMap: Map[Column, ForeignRef] =
    foreignKey.flatMap { fk =>
      fk.indexColNames.toSeq.map { case ((col, refCol)) =>
        (col, (fk.referenceTbl, refCol))
      }
    }. toMap

  private def resolveForeignRefOf(column: Column): ForeignRef =
    columnRefResolveMap(column)

  private def uniqueGenerator(cardinality: Int, dataType: MySQLDataType): Option[Set[String]] = {
    val maxTry = cardinality * 2

    def generate(turn: Int, acm: Set[String]): Option[Set[String]] =
      if (turn < maxTry) {
        val maybeNew = dataType.next
        if (acm.contains(maybeNew)) generate(turn+1, acm)
        else {
          val newSet = acm + maybeNew
          if (newSet.size >= cardinality) Some(newSet)
          else generate(turn+1, newSet)
        }
      } else None

    generate(0, Set.empty)
  }

  private def widing(size: Int, data: List[String]): List[String] =
    if (data.size < size)
      widing(size, data.last :: data)
    else data

  def generateSampleData(
      genConf: GenConf,
      alreadyGenerated: Map[ForeignRef, Set[String]]
  ): Either[CsdfsError, (String, Map[ForeignRef, Set[String]])] = {
    val conf = genConf.of(tbl)
    val (dependentCol, independentCol) = dependentColumnDefOrNot

    val independentData: Map[Column, Set[String]] =
      independentCol.map(colDef => {
        val cardinality = conf.of(colDef.column).cardinality
        val elems = uniqueGenerator(cardinality, colDef.dataType) match {
          case Some(e) => e
          case None =>
            return Left(UniqueConstraintError(
              "The required number of unique elements could not be generated. :\n" +
              s"requested cardinality: $cardinality\n"))
        }
        if (colDef.unique && elems.size < conf.rowSize) {
          return Left(GenerateConfigError(
            s"There are not enough unique key types of ${tbl.tblName}.${colDef.column.colName} " +
            "in the number of rows to be generated. :\n" +
            s"requested cardinality: $cardinality\n" +
            s"generated column cardinality: ${elems.size}\n" +
            s"requested generate row size: ${conf.rowSize}"))
        }
        (colDef.column, elems)
      }).toMap

    val dependentData: Map[Column, Set[String]] =
      dependentCol.map(colDef => {
        val ref = resolveForeignRefOf(colDef.column)
        (colDef.column, alreadyGenerated(ref))
      }). toMap

    val inputData = (independentData.toSeq ++ dependentData.toSeq).toMap

    val newGenerated: Map[ForeignRef, Set[String]] =
      (independentData ++ dependentData).map(d =>
        ((tbl, d._1), d._2))

    val (columns, rowsConstraint) = uniqueConstraintKey.foldRight((Seq.empty[Column], Set.empty[Seq[String]]))((next, r) => {
      val (colmns, rows) = r
      val (thisColumns, thisRows) = next.genColSatisfyConstraint(conf.rowSize, inputData) match {
        case Right(r) => r
        case Left(l) => return Left(l)
      }
      val combined =
        if (rows.nonEmpty)
          rows.zip(thisRows).map(zipped => zipped._1 ++ zipped._2)
        else thisRows
      (colmns ++ thisColumns, combined)
    })

    val rowsNotConstraint = notUniqueConstraintKey.map(k => {
      widing(conf.rowSize, inputData(k.column).toList)
    })

    val colNames = notUniqueConstraintKey.map(_.column.colName) ++ columns.map(_.colName)
    val rows = if (rowsConstraint.nonEmpty) {
      rowsConstraint.zipWithIndex.map { case (rowConstraint, idx) =>
        rowsNotConstraint.foldRight(Nil: List[String])(
          (lst, data) => lst(idx) :: data) ++ rowConstraint
      }
    } else {
      (0 until conf.rowSize).map(idx =>
        rowsNotConstraint.foldRight(Nil: List[String])(
          (lst, data) => lst(idx) :: data))
    }

    val inserts = s"INSERT INTO ${tbl.tblName} " +
      s"${colNames.mkString("(", ", ", ")")} VALUES \n" +
      (rows map { row =>
        row.mkString("(", ", ", ")")
      }).mkString(",\n") + ";"

    Right((inserts, alreadyGenerated ++ newGenerated))
  }


}

object MySQLSchema {

  sealed trait CreateDefinition

  case class ColumnDef(
                        column: Column,
                        dataType: MySQLDataType,
                        notNull: Boolean,
                        autoIncrement: Boolean,
                        unique: Boolean
  ) extends CreateDefinition

  case class PrimaryKey(indexColNames: Seq[Column]) extends CreateDefinition with UniqueGroupConstraint

  case class UniqueKey(indexColNames: Seq[Column]) extends CreateDefinition with UniqueGroupConstraint

  trait UniqueGroupConstraint {

    val indexColNames: Seq[Column]
    
    def genColSatisfyConstraint(size: Int, dataElem: Map[Column, Set[String]]): Either[CsdfsError, (Seq[Column], Set[Seq[String]])] = {
      val maxSize =
        indexColNames.foldLeft(1)((size, col) => {
          val colCardinality = dataElem(col).size
          size * colCardinality
        })

      if (maxSize < size)
        return Left(UniqueConstraintError("The required number of elements that" +
          "satisfy the unique constraint was not reached."))

      def enumerate(cols: List[Column]): Set[Seq[String]] = {
        val resultSet = scala.collection.mutable.Set.empty[Seq[String]]

        def rec(stack: List[(List[Column], List[String])], generated: Int): Unit = {
          if (generated < size && stack.nonEmpty) {
            val (restColumns, selected) = stack.head
            restColumns match {
              case Nil =>
                resultSet.add(selected.reverse)
                rec(stack.tail, generated + 1)

              case h :: t =>
                val newCallStacks = dataElem(h)
                  .foldRight(Nil: List[(List[Column], List[String])])(
                    (col, newCallStacks) => (t, col :: selected) :: newCallStacks)
                rec(newCallStacks ++ stack.tail, generated)
            }
          } else ()
        }
        rec(List((cols, Nil)), 0)

        resultSet.toSet
      }

      Right((indexColNames, enumerate(indexColNames.toList)))
    }
      
  }

  case class ForeignKey(referenceTbl: Table, indexColNames: Map[Column, Column]) extends CreateDefinition

}