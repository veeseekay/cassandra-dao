Cassandra DAO
=============

Cassandra DAO is a framework for (you guessed it) implementing data access objects that are bound to
Cassandra CQLv3 tables.

An Example
----------

Suppose you have a CQLv3 table like so...
````cql
create table users (
  username text,
  fullName text,
  primary key(username)
);
````

Your model would probably look something like this...
````java
class User extends com.womply.cassandradao.Model {
  private String username;
  private String fullName;

  // Getters and setters
}
```

By default, cassandradao converts User to "users" as the table name.
To override the table name, simply add the @Table annotation.
Use the @Column annotation to override your column name.

````java
@Table(name = "table_name")
class User extends com.womply.cassandradao.Model {
  @Column(name = "user_name")
  private String username;
  private String fullName;

  // Getters and setters
}
````

To create a "adhoc" DAO for this model/table you would...
````java
// Build a Cassandra Java Driver Session
com.datastax.driver.core.Session session = com.datastax.driver.core.Cluster.Builder.builder()
  .addContactPoint("localhost").build().connect();

com.womply.cassandradao.CassandraDAO<User> userDao =
  new com.womply.cassandradao.CassandraDAO<>(User.class, "my_keyspace", session);
````

To get a specific user record...
````java
User user = userDao.first("username = ?", new Object[] { "johndoe" },
  com.datastax.driver.core.ConsistencyLevel.ONE);
````

To save a change to the user record...
````java
user.setFullName("John Doe")
userDao.save(user, com.datastax.driver.core.ConsistencyLevel.ONE);
````

To iterate over all user records...
````java
com.womply.cassandradao.ResultSet<User> users = userDao.findAll(com.datastax.driver.core.ConsistencyLevel.ONE);
for(User user : users) {
  // Do something with each user
}
````

Maven Dependency
----------------

````xml
<dependency>
		<groupId>com.womply.cassandradao</groupId>
		<artifactId>cassandra-dao</artifactId>
		<version>0.2.0</version>
</dependency>
````

