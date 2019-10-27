package csdfs.mysql

import csdfs.mysql.GenConf.GenTableConf

case class GenConf(private val genTableConfMap: Map[Table, GenTableConf]) {

  def of(table: Table): GenTableConf =
    genTableConfMap.getOrElse(table, GenTableConf.defaultConfOf(table))

}

object GenConf {

  val default = GenConf(Map.empty)

  case class GenTableConf(
      tbl: Table,
      rowSize: Int = 10,
      private val columnConfMap: Map[Column, GenColumnConf] = Map()) {

    def of(column: Column): GenColumnConf =
      columnConfMap.getOrElse(column, GenColumnConf.defaultConfOf(column))

  }

  object GenTableConf {

    def defaultConfOf(tbl: Table): GenTableConf = GenTableConf(tbl = tbl)

  }

  case class GenColumnConf(
      column: Column,
      cardinality: Int = 10)

  object GenColumnConf {

    def defaultConfOf(column: Column): GenColumnConf = GenColumnConf(column = column)

  }

}