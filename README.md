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
        |  column1 mediumint not null,
        |  column2 enum('value1', 'value2') null unique,
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
        |  column2 varchar not null
        |)
      """.stripMargin
    ))
```

returns

```scala
Right(List(
"""INSERT INTO table1 (id, column1, column2, column3) VALUES 
(-715691413, 3348064, value1, 'WgJx5ebjyFrvciiLJ4vY'),
(1426974093, 5337510, value2, 'EznCNpPOPEDTYMM6Xssp'),
(1446523668, 2167930, value1, 'LqiztGacLkFJQT3udmvH'),
(614036656, 1952271, value1, 'gFlSTBiD6N2ck0AgvHbt'),
(56317086, 8481338, value1, '6ghQH019L80aXlgUifY6'),
(-538994130, 5268792, value2, 'GAN53PEffkdfIQ2kjAqY'),
(-1014239790, 14417965, value1, 'U6f4NWUAU40aKbgCmQI7'),
(-603291771, 3940782, value1, 'CYF4Eqwe9RCxubwd91oT'),
(898091868, 4953800, value2, 'WgJx5ebjyFrvciiLJ4vY'),
(714183211, 14181945, value1, 'bB19NWssz0mnNfj6eH7n'),
(-719618547, 15652892, value2, 'VDYAJAtOuROSp0vfCa4a');""",
"""INSERT INTO table2 (id, column1, column2) VALUES 
(-173270041, 'e', 'Vyz9YHFo6JRwUvIq7Rq1'),
(-972631142, 'p', 'mVtxeGGaGTTsCOcos58t'),
(-1772900352, 'f', 'Qu5qMUNKjHxSBK0nYQi9'),
(-2072802790, '7', 'Vyz9YHFo6JRwUvIq7Rq1'),
(1924561849, 'V', 'KzkohjtZl0WKJbW8HB2h'),
(906269495, 'H', '3wNIjq2FfdSrfmlfqHIs'),
(1777892755, 'S', 'NEOXj68Pgd7i6Xgaq7jG'),
(-1194832663, 'X', 'yplEw0gcKZP3a7jKzDIf'),
(-1959988323, 'p', 'AOTdj7ly4DRChEYswgoD'),
(-664777541, 'p', '5YJCIcG2JHCpabJp8ngh'),
(333305104, 'E', 'QYSwzlpPnQWH2SFBfBUU');"""))
```