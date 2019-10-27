package csdfs

import scala.util.Random

package object mysql {

  type ForeignRef = (Table, Column)

  implicit val defaultIntGen = new ColGenerator[MySQLDataType.Int.type] {
    override def next(): String = Random.nextInt().toString
  }

}
