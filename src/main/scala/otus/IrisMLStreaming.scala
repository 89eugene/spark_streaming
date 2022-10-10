package otus

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, MultilayerPerceptronClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object IrisMLStreaming {

  case class Iris(sepalLength: Double,
                  sepalWidth: Double,
                  petalLength: Double,
                  petalWidth: Double,
                  species: String
                 )


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Iris")
      .config("spark.master", "local")
      .getOrCreate()

    val irisDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/IRIS.csv");


    val vectorAssembler = new  VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")

    var vectorModel = vectorAssembler.transform(irisDF)

    vectorModel = vectorModel.drop("sepal_length", "sepal_width", "petal_length", "petal_width")

    val indexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("labelIndex")

    val indexFit = indexer.fit(vectorModel)

    val indexModel = indexFit.transform(vectorModel)


    val decisionTreeClassifier = new DecisionTreeClassifier()
        .setLabelCol("labelIndex")
        .setFeaturesCol("features")

    val model = decisionTreeClassifier.fit(indexModel)

    model.write.overwrite().save("./src/main/resources/model")

    // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:19092")

    spark.close()


    // Создаём Streaming Context и получаем Spark Context
    val sparkConf        = new SparkConf().setMaster("local").setAppName("MLStreaming")

    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    val sparkContext     = streamingContext.sparkContext
    // Создаём Kafka Sink (Producer)
    val kafkaSink = sparkContext.broadcast(KafkaSink(props))

    // Параметры подключения к Kafka для чтения
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> "localhost:19092",
      ConsumerConfig.GROUP_ID_CONFIG                 -> "1",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // Подписываемся на входную тему Kafka (тема с данными)
    val inputTopicSet = Set("input")
    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
    )

    // Разбиваем входную строку на элементы
    val lines = messages
      .map(_.value)
      .map(_.replace("\"", "").split(","))


    val inputVectorAssembler = new  VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")

//    val query = input.writeStream
    lines.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Преобразовываем RDD в DataFrame
      val data = rdd
        .toDF("input")
        .withColumn("sepal_length", $"input" (0).cast(DoubleType))
        .withColumn("sepal_width", $"input" (1).cast(DoubleType))
        .withColumn("petal_length", $"input" (2).cast(DoubleType))
        .withColumn("petal_width", $"input" (3).cast(DoubleType))
        .drop("input")

      println("Data show")
      data.show(50)

      // Если получили непустой набор данных, передаем входные данные в модель, вычисляем и выводим ID клиента и результат
      if (data.count > 0) {

        val inputVector = inputVectorAssembler.transform(data)
        val result = model.transform(inputVector)
        val converter = new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("classification")
          .setLabels(indexFit.labelsArray.flatten)

        val convertedClass = converter.transform(result)

        convertedClass
          .select("sepal_length", "sepal_width", "petal_length", "petal_width", "classification")
          .foreach { row => kafkaSink.value.send("prediction", s"${row(0)},${row(1)}, ${row(2)}, ${row(3)},  ${row(4)}") }
      }
    }

    streamingContext.start()

    streamingContext.awaitTermination()
  }

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
