package csdfs

trait SchemaParser[S <: Schema] {

  def parse(schema: String): Either[Throwable, S]

}
