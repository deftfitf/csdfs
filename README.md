# csdfs 

csdfs (Create sample data from schema)

! Includes specification leaks and bugs.

# How to use

```scala
import csdfs.mysql.MySQLCsdfs

val csdfs =
  new MySQLCsdfs(
    new MySQLSchemaParser {},
    new DependencyResolver)

csdfs.generateInsertStatements(
    Seq(
      """
        |create table table1 (
        |  id int not null auto_increment,
        |  column1 mediumint not null unique,
        |  column2 enum('value1', 'value2') null,
        |  column3 varchar not null,
        |
        |  foreign key (column3)
        |  references table2 (column2)
        |)
      """.stripMargin,
      """
        |create table table2 (
        |  id int not null auto_increment,
        |  column1 char not null,
        |  column2 varchar not null,
        |
        |  primary key (id, column1)
        |)
      """.stripMargin
    ),
    GenConf(
      Map((Table("table1"),
        GenConf.GenTableConf(Table("table1"), 10,
          Map((Column("column2"),
            GenConf.GenColumnConf(Column("column2"), cardinality = 2))))))
    )
)
```

returns

```scala
Right(List(
"""INSERT INTO table1 (id, column1, column2, column3) VALUES 
(-586993562, 12241194, value1, 'PXnjdffykXeP5zG5HwMh'),
(-823798655, 8893848, value1, 'D9bLAAJiV8fKRcDOUwvX'),
(642949237, 6419621, value1, 'hBXEBLVgFtfjM4jfgu69'),
(535174522, 2873838, value1, '8Bw5rdriOE0U2Ne6dxtm'),
(457588828, 2323780, value1, 'eKDZsQKwCqrWKPU3FoTh'),
(-1401895932, 14158572, value1, '6jGeJ1czPQBr4U1AHTdJ'),
(-1432855235, 6371477, value1, 'BlcO7JDOTOA1ca133W7F'),
(-1626369446, 15265092, value1, 'WGzOzrvTu2BTaeAIFVgj'),
(-1028050537, 808524, value2, '3FXGA3QYM3jNKMMcMwsg'),
(1130449006, 8866004, value1, 'YpalAsknwCQzoGdg9QCK');""",
"""INSERT INTO table2 (column2, id, column1) VALUES 
('8Bw5rdriOE0U2Ne6dxtm', 1945521320, 'z'),
('BlcO7JDOTOA1ca133W7F', 1945521320, 'B'),
('D9bLAAJiV8fKRcDOUwvX', 1945521320, 'K'),
('WGzOzrvTu2BTaeAIFVgj', 1945521320, 'a'),
('eKDZsQKwCqrWKPU3FoTh', 1945521320, '2'),
('hBXEBLVgFtfjM4jfgu69', 1945521320, 'd'),
('3FXGA3QYM3jNKMMcMwsg', 1945521320, 'k'),
('PXnjdffykXeP5zG5HwMh', 1945521320, 'N'),
('YpalAsknwCQzoGdg9QCK', 1945521320, 'v'),
('6jGeJ1czPQBr4U1AHTdJ', 1945521320, '1');"""))
```