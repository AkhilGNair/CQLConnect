sparkhello: Scala to Spark - Hello World
================

**sparkhello** demonstrates how to build a [sparklyr](http://github.com/rstudio/sparklyr) extension package that uses custom Scala code which is compiled and deployed to Apache Spark.

For example, suppose that you want to deploy the following Scala code to Spark as part of your extension:

``` scala
object HelloWorld {
  def hello() : String = {
    "Hello, world! - From Scala"
  }
}
```

The R wrapper for this Scala code might look like this:

``` r
spark_hello <- function(sc) {
  sparklyr::invoke_static(sc, "SparkHello.HelloWorld", "hello")
}
```

To package and deploy this Scala code as part of your extension, store the Scala code within the `inst/scala` directory of your package and then compile it using the following command, which will build the JARs required for various versions of Spark under the `inst/java` directory of your package:

``` r
sparklyr::compile_package_jars()
```

There are a couple of conditions required for `sparklyr::compile_package_jars` to work correctly:

1.  Your current working directory should be the root directory of your package.

2.  You should install the Scala 2.10 and 2.11 compilers to one of the following paths:
    -   /opt/scala
    -   /opt/local/scala
    -   /usr/local/scala
    -   ~/scala (Windows-only)

You then need to implement the `spark_dependencies` function (which tells sparklyr that your JARs are required dependencies) as well as an `.onLoad` function which registers your extension. For example:

``` r
spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/sparkhello-%s-%s.jar", spark_version, scala_version),
        package = "sparkhello"
      )
    ),
    packages = c()
  )
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
```

Assuming the **sparkhello** package was loaded prior to connecting to Spark, you can now call the `spark_hello` function which in turn executes the Scala code in your custom JAR:

``` r
library(sparklyr)
library(sparkhello)

sc <- spark_connect(master = "local")
spark_hello(sc)
```

    ## [1] "Hello, world! - From Scala"

``` r
spark_disconnect(sc)
```

You can learn more about sparklyr extensions at <http://spark.rstudio.com/extensions.html>.
