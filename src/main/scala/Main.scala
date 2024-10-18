import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Main {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
          .appName("Experian Spark Entrevista Tecnica")
          .master("local[*]")
          .getOrCreate()

        import spark.implicits._

        // Creando dataframes
        // Dataframe 1 tiene 4000 registros su columna será id y df1_value y contendra valores value_df1
        // Dataframe 2 tiene 1000 registros su columna será id y df2_value y contendra valores value_df2
        // Dataframe 3 tiene 10 registros y contendra la columna id y df3_value y los valores value_df3
        val df1: DataFrame = (1 to 4000).toDF("id").withColumn("df1_value", lit("value_df1"))
        val df2: DataFrame = (1 to 1000).toDF("id").withColumn("df2_value", lit("value_df2"))
        val df3: DataFrame = (1 to 10).toDF("id").withColumn("df3_value", lit("value_df3"))

        // Realizar un Left Join entre df1 y df2  para obtener el dataframe 4
        val df4 = df1.join(df2, Seq("id"), "left")
        println("Resultado de df4 (left join entre df1 y df2):")
        df4.show(false)
        df4.count()
        // Realizar un Inner Join entre df4 y df3 para obtener df 5
        // Como optimizamos el dataframe 5: Utilizamos broadcast join debido a que el dataframe 3 es muy pequeño,
        // Con Broadcast join Spark replica el Df en los nodos, y no hay repartición acelerando la operación del join
        // adicionalmente el df4 se mantiene en cache evitando que vuelva a recalcular las operaciones
        // Ponemos .coalesce debido a que sólo son 10 registros no tenemos que mantenerlos reparticionados en diferentes executors
        df4.cache()
        val df5 = df4.join(broadcast(df3), Seq("id"), "inner").coalesce(1)
        println("Resultado de df5 (inner join entre df4 y df3):")
        df5.show(false)

        // Cerrar la sesión de Spark
        spark.stop()
    }
}
