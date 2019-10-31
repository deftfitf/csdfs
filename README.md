# csdfs 

csdfs (Create sample data from schema)

! Includes specification leaks and bugs.

# Functions list

The following functions can be used.

* Support MySQL5.6 CREATE TABLE statement. (Some grammars are not supported) [13.1.17 CREATE TABLE 構文](https://dev.mysql.com/doc/refman/5.6/ja/create-table.html)
* Sample data generation considering foreign key constraints of multiple CREATE TABLE statements.
* Generation of sample data satisfying primary key and unique key constraints.
* Setting the number of sample data generation per table.
* The number of unique data generated for a particular column in a particular table. (However, it is not possible to generate more types of column data than the number of row sample data to be generated.)

# How to use

```scala
import csdfs.mysql.MySQLCsdfs

val csdfs = csdfs.mysql.MySQLCsdfs.instance

csdfs.generateInsertStatements(
    Seq(
      """
        |create table table1 (
        |  id int not null auto_increment,
        |  column1 mediumint not null unique,
        |  column2 enum('value1', 'value2') null,
        |  column3 varchar(64) not null,
        |
        |  foreign key (column3)
        |  references table2 (column2)
        |)
      """.stripMargin,
      """
        |create table table2 (
        |  id int not null auto_increment,
        |  column1 char not null,
        |  column2 varchar(64) not null,
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
(628505154, 6367780, value1, 'twpLBNpv0TYukg4vYQHZ8GTCKrZxc37ZIQvOziPNIffaJ2g4SBI8FvjCg8OYNxTQ'),
(1802938845, 16516874, value1, 'QeNg2mt0XiXUjK1US4sAaDvlO5J8ccQLe3MpqBfd19jIJRhmWCDXYVGxqJ0UJneH'),
(122688015, 7907605, value1, 'KNwEHrgqKPg9wDkY6Ix7XAgO1WVEywI2BgyrU4Kj0uEkw3hnBm2iMqlDmWCWb1MV'),
(738062263, 6200986, value1, 'tG1v1w5A03Pz4RKZUvg6lbrS29v9C66uCGqNk8iGl32YLjoRdkQwMdHPHvuDT58l'),
(887723347, 5461001, value1, 'ZcrDCsMD8aNVQi2ma35cmwOuKcDbijxCENUtZlIQvT9qPheCbnCAxZ8uL58etFzn'),
(76465175, 13984979, value1, 'VEtdhE8RlfxQ2F4tqv0oJ1eflJzfSV25hO6hU5hBjbcmOn3k6z8amS9M4IB376j6'),
(217732770, 14715916, value1, '2GdCrDXINvBD7NWvKmeXFsPqAsmGg9hd21X2nB6KTDykaA0iyFZLvOUPerJS1Hww'),
(227990809, 6879586, value1, 'hkEOdLYQ3ju0NH3lwMsYFVREtHuh84AZXzWdkOFNc3wjlTszuC19YtZ6v76APGEu'),
(1740430165, 13493854, value2, '7nLgoLxQoqu6B8B77y0ZI57XgMbXBv0uQ5nfkF0CyYhQJInvJDby0ZLocFRjQIzE'),
(296392555, 261530, value1, 'vqF3QStaVdS9xh9K7vQBgWkdLLrmvGCt1mMxqrmjrlUidm8FNY5riaLp8PskTO1e');""",
"""INSERT INTO table2 (column2, id, column1) VALUES 
('VEtdhE8RlfxQ2F4tqv0oJ1eflJzfSV25hO6hU5hBjbcmOn3k6z8amS9M4IB376j6', 1257347204, '6'),
('2GdCrDXINvBD7NWvKmeXFsPqAsmGg9hd21X2nB6KTDykaA0iyFZLvOUPerJS1Hww', 1257347204, 'l'),
('KNwEHrgqKPg9wDkY6Ix7XAgO1WVEywI2BgyrU4Kj0uEkw3hnBm2iMqlDmWCWb1MV', 1257347204, 'F'),
('tG1v1w5A03Pz4RKZUvg6lbrS29v9C66uCGqNk8iGl32YLjoRdkQwMdHPHvuDT58l', 1257347204, 'm'),
('hkEOdLYQ3ju0NH3lwMsYFVREtHuh84AZXzWdkOFNc3wjlTszuC19YtZ6v76APGEu', 1257347204, 'b'),
('vqF3QStaVdS9xh9K7vQBgWkdLLrmvGCt1mMxqrmjrlUidm8FNY5riaLp8PskTO1e', 1257347204, 'x'),
('QeNg2mt0XiXUjK1US4sAaDvlO5J8ccQLe3MpqBfd19jIJRhmWCDXYVGxqJ0UJneH', 1257347204, 'A'),
('7nLgoLxQoqu6B8B77y0ZI57XgMbXBv0uQ5nfkF0CyYhQJInvJDby0ZLocFRjQIzE', 1257347204, 'v'),
('ZcrDCsMD8aNVQi2ma35cmwOuKcDbijxCENUtZlIQvT9qPheCbnCAxZ8uL58etFzn', 1257347204, 'd'),
('twpLBNpv0TYukg4vYQHZ8GTCKrZxc37ZIQvOziPNIffaJ2g4SBI8FvjCg8OYNxTQ', 1257347204, 'U');"""))
```
