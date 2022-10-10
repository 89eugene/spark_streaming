package otus

case class Data(
                 sepalLength: Double,
                 sepalWidth: Double,
                 petalLength: Double,
                 petalWidth: Double
               )

object Data {
  def apply(a: Array[String]): Data =
    Data(
      a(0).toDouble,
      a(1).toDouble,
      a(2).toDouble,
      a(3).toDouble
    )
}
