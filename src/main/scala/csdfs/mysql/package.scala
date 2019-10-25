package csdfs

import scala.util.Random

package object mysql {

  implicit val defaultIntGen = new ColGenerator[MySQLDataType.Int.type] {
    override def next(): String = Random.nextInt().toString
  }

}
