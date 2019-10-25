package csdfs

// TODO: 外部キー制約のあるテーブルでも自動生成できるようにしたい.
// 例えば, テーブルAとテーブルBがあって, テーブルAのカラムに依存するテーブルBの外部キーがある時, A.column_i <- B.clunmn_jとなる.
// ２つのスキーマから、解決し、ジェネレータが生成する値が、A.column_iで使われた値がB.clunmn_jで使われるようになる.

// TODO: ユニークキー制約のあるテーブルでも自動生成できるようにしたい.

// TODO: PRIMARY KEY制約があるテーブルでも自動生成できるようにしたい.

// TODO: AUTO_INCREMENT制約を考慮した自動生成.
trait Csdfs