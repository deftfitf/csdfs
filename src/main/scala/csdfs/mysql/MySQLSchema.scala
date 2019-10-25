package csdfs.mysql

import csdfs.mysql.MySQLSchema.{ColumnDef, CreateDefinition, ForeignKey, UniqueKey}
import csdfs.mysql.MySQLDataType._

import scala.util.Random

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

  private val uniqueKey: Seq[UniqueKey] =
    createDefinitions. collect {
      case uk: UniqueKey => uk
    }

  private def dependentColumnDefOrNot: (Seq[ColumnDef], Seq[ColumnDef]) = {
    val colFk = foreignKey.flatMap(_.indexColNames).toMap

    columnDef.partition { d =>
      colFk.contains(d.column)
    }
  }

  private val columnRef: Map[Column, (Table, Column)] =
    foreignKey.flatMap { fk =>
      fk.indexColNames.toSeq.map { case ((col, refCol)) =>
        (col, (fk.referenceTbl, refCol))
      }
    }. toMap

  // TODO: 外部キー制約で, 依存テーブルで'使用'した値のみを受け渡す.
  def generateSampleData(
      alreadyGenerated: Map[(Table, Column), Seq[String]],
      genColumnN: Int = 10
  ): (String, Map[(Table, Column), Seq[String]]) = {
    println(tbl.tblName)
    val (dependentCol, independentCol) = dependentColumnDefOrNot

    // TODO: maybe throw exception, handle this.
    val independentData: Map[ColumnDef, Seq[String]] =
      independentCol.map(colDef =>
        (colDef, (0 to 100).map(_ => colDef.dataType.next))).toMap

    val dependentData: Map[ColumnDef, Seq[String]] =
      dependentCol.map(colDef => {
        val dataKey = columnRef(colDef.column)
        (colDef, alreadyGenerated(dataKey))
      }). toMap

    val newGenerated: Map[(Table, Column), Seq[String]] =
      (independentData ++ dependentData).map(d => ((tbl, d._1.column), d._2))

    val rows = (0 to genColumnN). map { _ =>
      independentCol. map { colDef =>
        Random.shuffle(independentData(colDef)).head
      } ++
      dependentCol. map { colDef =>
        Random.shuffle(dependentData(colDef)).head
      }
    }

    val inserts = s"INSERT INTO ${tbl.tblName} " +
      s"${(independentCol.map(_.column.colName) ++
        dependentCol.map(_.column.colName)).mkString("(", ", ", ")")} VALUES \n" +
      (rows map { row =>
        row.mkString("(", ", ", ")")
      }).mkString(",\n") + ";"

    (inserts, alreadyGenerated ++ newGenerated)
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

  case class PrimaryKey(indexColNames: Seq[String]) extends CreateDefinition

  case class UniqueKey(indexColNames: Seq[String]) extends CreateDefinition

  case class ForeignKey(referenceTbl: Table, indexColNames: Map[Column, Column]) extends CreateDefinition

}