# orPgAdmin
pgAdmin extension

This application orPgAdmin is a standard servlet that can perform various operations on the Postgresql base such as synchronizing bases, creating scripts for them, and so on.
It is intended for developers who are part of the development in base. It does not replace the well-known PgAdmin but extends it.

To start:
1. If you do not have a Java servlet runner, install Tomcat on your own. I use Tomcat8, which has also been tested on the application.
2. Add the Tomcat lib to the PostgreSQL JDBC driver (mine is postgresql-42.2.5.jre7.jar).
3. Create an application catalog somewhere and put the orPgAdmin.war and config.xml
4. The configuration file contains server descriptions, but it also contains the temp ( work) directory location and the debug level that controls logging.
Values ​​are 0 ... 99, where 0 is minimal, meaning only error situations are logged. I usually have a working directory in the application directory.
5. Change the application starter orPgAdmin.xml so that the docBase attribute refers to the application and the path parameter to the application catalog.
6. Get Tomcat up and run application with http://localhost:8080/orPgAdmin/Show
 Anyone who wants to improve things must have the source code of the application and everything else is in war.
