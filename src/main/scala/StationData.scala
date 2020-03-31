/*questo, insieme a WeatherData.scala contengono il codice per fare le letture
* dei dati del meteo, per farne il parsing (nel dataset weather-sample contiene i dati sulle rilevazioni
* meteo in una giornata in una centralina, nel dataset station ho l'elenco
* di tutte le stazioni meteo che hanno collezionato le rilevazioni, dentro
* le stazioni ci sono riferimenti geografici (città, stato)).
* Inoltre creano un rdd con una struttura: all'interno dell'rdd vengono messi degli
* oggetti in modo da consentire di fare riferimento ai singoli campi tramite il nome,
* altrimenti non potremmo visto che l'rdd non ha un concetto di struttura (non c'è
* il nome delle colonne). Un altro modo di aggiungere struttura agli rdd è tramite
* un'altra struttura dati: il dataframe*/
object StationData {
  def extract(row:String) = {
    def getDouble(str:String) : Double = {
      if (str.isEmpty)
        return 0
      else
        return str.toDouble
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val latitude = getDouble(columns(6))
    val longitude = getDouble(columns(7))
    val elevation = getDouble(columns(8))
    StationData(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),latitude,longitude,elevation,columns(9),columns(10))
  }
}

case class StationData(
  usaf:String, //codice usato per il join, insieme a wban, infatti sono anche in WeatherData
  wban:String, //codice usato per il join, insieme a usaf, infatti sono anche in WeatherData
  name:String,
  country:String,
  state:String,
  call:String,
  latitude:Double,
  longitude:Double,
  elevation:Double,
  date_begin:String,
  date_end:String
)

