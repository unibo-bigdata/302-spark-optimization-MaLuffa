import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.SparkSession


// spark2-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
// spark-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
object Exercise extends App {

  override def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    if(args.length >= 1){
      args(0) match {
        case "1" => exercise1(sc)
        case "2" => exercise2(sc)
        case "3" => exercise3(sc)
        case "4" => exercise4(sc)
        case "5" => exercise5(sc)
      }
    }
  }

  /**
   * Creates the SparkContent; comment/uncomment code depending on Spark's version!
   * @return
   */
  def getSparkContext(): SparkContext = {
    // Spark 1
    // val conf = new SparkConf().setAppName("Exercise 302 - Spark1")
    // new SparkContext(conf)

    // Spark 2
    val spark = SparkSession.builder.appName("Exercise 302 - Spark2").getOrCreate()
    spark.sparkContext
  }

  /**
   * Optimize the two jobs (avg temperature and max temperature)
   * by avoiding the repetition of the same computations
   * and by defining a good number of partitions.
   *
   * Hints:
   * - Verify your persisted data in the web UI
   * - Use either repartition() or coalesce() to define the number of partitions
   *   - repartition() shuffles all the data
   *   - coalesce() minimizes data shuffling by exploiting the existing partitioning
   * - Verify the execution plan of your RDDs with rdd.toDebugString (shell) or on the web UI
   *
   * @param sc
   */
  def exercise1(sc: SparkContext): Unit = {
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract) //extract mi serve x parsare i dati e costruire l'rdd

    /*//per mettere questo codice sulla shell devo mettere inline i diversi step
    // Average temperature for every month
    rddWeather
      .filter(_.temperature<999) //considero solo le temperature con un valore <999, xk se non c'è valore x la temp viene inserito 999
      .map(x => (x.month, x.temperature))//organizzo l'rdd in una coppia chiave-valore (mese-temperatura)
      .aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2)) //aggregazione k mantiene separate somma e conteggio
      .map({case(k,v)=>(k,v._1/v._2)}) //calcolo il valore finale della temperatura media
      .collect()*/

    //Average temperature for every month optimized
    // (x evitare le ripetizioni uso cache nel momento opportuno,
    // per un buon numero di partizioni dipende da quanti core ho (2-4 partizioni per core)
    //coalesce e repartition creano tot partizioni ma non definiscono un criterio di partizionamento (partitioner = none)
    //che può essere impostato nel momento in cui il mio rdd è un rdd chiave-valore
    val cachedRdd = rddWeather.coalesce(8).filter(_.temperature<999).map(x => (x.month, x.temperature)).cache()
    cachedRdd
        .aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
        .map({case(k,v)=>(k,v._1/v._2)})
        .collect()

    /*// Maximum temperature for every month
    rddWeather
      .filter(_.temperature<999)
      .map(x => (x.month, x.temperature))
      .reduceByKey((x,y)=>{if(x<y) y else x})//invece di calcolare la media calcolo il massimo
      .collect()*/

    //Maximum temperature for every month
    cachedRdd.reduceByKey((x,y) => if (x<y) y else x).collect()
  }

  /**
   * Find the best option
   * @param sc
   */
  def exercise2(sc: SparkContext): Unit = {
    import org.apache.spark.HashPartitioner
    val p = new HashPartitioner(8)

    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)
    //sapendo che lou seremo + volte qual è il modo migliore x organizzarlo?
    // Alcune non potrebbero nemmeno essere eseguite (le prime 2 in quanto partitionBy imposta
    // un criterio di partizionamento e lo può fare solo su un rdd chiave-valore, cosa che fa il keyBy).
    //Tra la 3 e la 4 va scelta la 3 in quanto fare il cache prima del partizionamento keyBy non ha senso
    // val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
    // val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban)
    val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
    // val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)

  }

  /**
   * Define the join between rddWeather and rddStation and compute:
   * - The maximum temperature for every city
   * - The maximum temperature for every city in Italy
   *   - StationData.country == "IT"
   * - Sort the results by descending temperature
   *   - map({case(k,v)=>(v,k)}) to invert key with value and vice versa
   *
   * Hints & considerations:
   * - Keep only temperature values <999
   * - Join syntax: rdd1.join(rdd2)
   * - Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
   * - Consider partitioning and caching to optimize the join
   * - Careful: it is not enough for the two RDDs to have the same number of partitions;
   *   they must have the same partitioner!
   * - Verify the execution plan of the join in the web UI (con .toDebugString in cui ogni passo di indentazione
    * mi identifica uno stage diverso, separati da shuffling)
   *
   * @param sc
   */
  def exercise3(sc: SparkContext): Unit = {
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  //li trasformo in rdd chiave-valore e gli applico lo stesso criterio di partizionamento
    val p = new HashPartitioner(8)
    val rddW = rddWeather
      .filter(_.temperature < 999)
      .keyBy(x => x.usaf + x.wban)
      .partitionBy(p)
    val rddS = rddStation
      .keyBy(x => x.usaf + x.wban)
      .partitionBy(p)
    val joinedRdd = rddS.join(rddW).cache() //posso anche fare join nell'altro verso

    val maxTempPerCity = joinedRdd
      .map({case(_,v) => (v._1.name, v._2.temperature)})
      .reduceByKey((x,y) => if(x<y) y else x)
      .collect()

    val maxTempPerITCity = joinedRdd
      .filter(_._2._1.country == "IT")
      .map({case(_,v) => (v._1.name, v._2.temperature)})
      .reduceByKey((x,y) => if(x<y) y else x)
      .map({case(k,v) => (v,k)})
      .sortByKey(false)
      .collect()


  }

  /**
   * Use Spark's web UI to verify the space occupied by the following RDDs
   * @param sc
   */
  def exercise4(sc: SparkContext): Unit = {
    import org.apache.spark.storage.StorageLevel._
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)

    //richiama lo spark context x svuotare tutto il contenuto che c'era in memoria
    sc.getPersistentRDDs.foreach(_._2.unpersist())


    val memRdd = rddWeather.sample(false,0.1).repartition(8).cache()
    val memSerRdd = memRdd.map(x=>x).persist(MEMORY_ONLY_SER)//la metto in memoria serializzata, così occupa meno spazio
    val diskRdd = memRdd.map(x=>x).persist(DISK_ONLY)//la metto su disco
    //prima di questi collect non c'è ancora niente in memoria
    memRdd.collect()
    memSerRdd.collect()//anche se eseguo solo qst senza il precedente, siccome memSer parte da mem, verrà eseguito anche il codice di mem (riga 149)
    diskRdd.collect()
  }

  /**
   * Consider the following scenario:
   * - We have a disposable RDD of Weather data (i.e., it is used only once): rddW
   * - And we have an RDD of Station data that is used many times: rddS
   * - Both RDDs are cached (collect() is called to enforce caching)
   *
   * We want to join the two RDDS. Which option is best?
   * - Simply join the two RDDs
   * - Enforce on rddW1 the same partitioner of rddS (and then join)
   * - Exploit broadcast variables
   * @param sc
   */
  def exercise5(sc: SparkContext): Unit = {
    import org.apache.spark.HashPartitioner

    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

    val rddW = rddWeather
      .sample(false,0.1) //fatto x ridurre i dati e semplificare l'esecuzione
      .filter(_.temperature<999)
      .keyBy(x => x.usaf + x.wban)
      .cache() //se lo utilizzo una volta sola non avrebbe senso metterlo in cache, qui serve x fare le prove
    val rddS = rddStation
      .keyBy(x => x.usaf + x.wban)
      .partitionBy(new HashPartitioner(8)) //gli imposto il criterio di partizione, cosa faccio a qst punto?
      .cache()

    // Collect to enforce caching
    rddW.collect
    rddS.collect

    // Is it better to simply join the two RDDs..
    //rddS è organizzato secondo un criterio e rddW no
    rddW
      .join(rddS)
      .filter(_._2._2.country=="IT")
      .map({case(k,v)=>(v._2.name,v._1.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()

    // ..to enforce on rddW1 the same partitioner of rddS..
    //semplifico il join xk ho già le coppie di partizione organizzate
    rddW
      .partitionBy(new HashPartitioner(8))
      .join(rddS)
      .filter(_._2._2.country=="IT")
      .map({case(k,v)=>(v._2.name,v._1.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()

    // ..or to exploit broadcast variables?
    val bRddS = sc.broadcast(rddS.collectAsMap())//esco dalla struttura dell rddS e lo riporto in memoria centrale, duplicandolo poi in ogni executor
    val rddJ = rddW
      .map({case (k,v) => (bRddS.value.get(k),v)})
      .filter(_._1!=None)
      .map({case(k,v)=>(k.get.asInstanceOf[StationData],v)})
    rddJ
      .filter(_._1.country=="IT")
      .map({case (k,v) => (k.name,v.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()
  }

  /* La scelta migliore è la 3. perché non richiede un trasferimento di dati come fa il join. In realtà
  l'ho fatto prima, ho trasferito rddS a tutti gli executor (che un po' mi è costato ma poco). Il join poi
  viene fatto localmente perché x ogni partizione di rddW il contenuto di rddS ce l'ho direttamente in
  memoria quindi vi accedo per recuperarlo. Poi la reduce la faccio lo stesso, ma il join iniziale lo salto.
  Throwback: posso ricorrere a questo stratagemma solo se l'rddS non è troppo grande e nn supera la disponibilità
  di memoria di un executor e inoltre c'è una duplicazione per ogni executor
   */

}