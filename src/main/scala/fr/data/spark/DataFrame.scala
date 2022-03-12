package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("postalCodes")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/codesPostaux.csv")

    case class Commune(
      Code_commune_INSEE: String,
      Nom_commune: String,
      Code_postal: String,
      Ligne_5: String,
      Libellé_d_acheminement: String,
      coordonnees_gps: String
    )

    // val ds = df.as[Commune]

    // 1) Show schema
    df.printSchema()

    // Notice the missing values!
    // df.show()

    // Number of columns : 6
    println("Number of columns : " + df.columns.size)

    // 2) Display the number of communes : 35100
    println(df.select("Code_commune_INSEE")
      .distinct()
      .count)

    // 3) Display the number of communes that have the Line_5 attribute : 2191
    // println(df.select("Nom_commune")
    //   .where($"Ligne_5" =!= null)
    //   .distinct()
    //   .count())
    println(df.select("Code_commune_INSEE")
      .filter("Ligne_5 is not null")
      .distinct()
      .count())

    // 4) Add a column to the data containing the department number of the commune.
    // df.withColumn("Numero_departement", $"Code_postal".substr(1,2))
    val df2 = df.withColumn("Numero_departement", column("Code_postal")
                .substr(1,2))
    df2.show()

    // Write the result in a new CSV file named "commune_et_departement.csv", 
    // having for column "Code_commune_INSEE", "Nom_commune", "Code_postal", "Numero_departement", ordered by postal code.
    df2.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "Numero_departement")
      .sort("Code_postal") // .orderBy("Code_postal".desc)
      .write.options("header",true)
      .csv("src/main/resources")

    // Display the communes of the Aisne department.
    df2.filter("Numero_departement = 02")
      .show()
 
    // Which department has the most communes ?
    df2.groupBy("Numero_departement")
      .count()
      .show()

  }

}




