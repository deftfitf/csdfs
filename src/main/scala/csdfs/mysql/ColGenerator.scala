package csdfs.mysql

trait ColGenerator[T] {

  def next: String

}