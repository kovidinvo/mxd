package com.kovidinvo.mxd.feeds

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import javax.annotation.PostConstruct
import org.springframework.web.reactive.function.client.WebClient.builder as WebCltBuilder

data class MxdResponse(
    val metadata : Map<String,String>,
    val columns: List<String>,
    val data: List<Map<String,Any>>
)

@Component
class TradesFeed(@Autowired val elClt: RestHighLevelClient) {

    private val logger = LoggerFactory.getLogger(TradesFeed::class.java)
    private val SECID = "SECID"
    private val TRADEID = "TRADENO"

    private val mxClt = WebCltBuilder().baseUrl("https://iss.moex.com/iss/")
        .defaultHeader("Content-Type","application/json")
        .exchangeStrategies(ExchangeStrategies.builder()
            .codecs{ cod -> cod.defaultCodecs().maxInMemorySize(16*1024*1024)}.build())
        .build()

    private val mapper = ObjectMapper()

    private var start : Int = 0

    @Scheduled(cron="0 0 3 * * MON-FRI")
    fun dailyAtNight() {
        start=0
    }

    @PostConstruct
    fun initializeIndexes() {
        val bodyNode = mxClt.get().uri("engines/stock/markets/shares/securities.json")
            .retrieve().bodyToMono(String::class.java).map { b -> mapper.readTree(b) }.block() ?: throw ExceptionInInitializerError("securities loading error")
        val dataNode = bodyNode.path("securities").path("data")
        val data = mapper.convertValue(dataNode,object: TypeReference<List<List<String>>>(){})
        val securites = data.map { rec -> rec[0].lowercase() }.toSet().toList()
        //create indexes
        securites.forEach { sec ->
            val indname ="shares-trades-${sec}"
            val mappingStr = """
            {
            "properties" : {
                "TRADENO" : { "type" : "long" },
                "TRADETIME" : { "type" : "date", "format":"strict_date_optional_time||epoch_second" } ,
                "BOARDID"  : { "type" : "keyword" },
                "SECID"  : { "type" : "keyword"},
                "PRICE"  : { "type" : "double" },
                "QUANTITY"  : { "type" : "long" },
                "VALUE"  : { "type" : "double" },
                "PERIOD"  : { "type" : "text" },
                "TRADETIME_GRP" : { "type" : "long" },
                "SYSTIME"  : { "type" : "text" },
                "BUYSELL"  : { "type" : "keyword" },
                "DECIMALS"  : { "type" : "integer" },
                "TRADINGSESSION"  : { "type" : "text" }
                }
            }
        """.trimIndent()

            if(elClt.indices().exists(GetIndexRequest(indname), RequestOptions.DEFAULT)) {
              //update mapping and return
                val req = PutMappingRequest(indname)
                req.source(mappingStr,XContentType.JSON)
                elClt.indices().putMapping(req, RequestOptions.DEFAULT)
                logger.info("Updated mappings: $indname")
                return@forEach
            }
            val req = CreateIndexRequest(indname)
            req.mapping(mappingStr,XContentType.JSON)
            val resp =elClt.indices().create(req, RequestOptions.DEFAULT)
            logger.info("Created index: $indname = ${resp.isAcknowledged}")
        }
    }

    @Scheduled(cron="0 */2 9-23 * * MON-FRI")
    fun fetchTrades() {
        mxClt.get().uri("engines/stock/markets/shares/trades.json?start=${start}")
            .retrieve().bodyToMono(String::class.java)
            .map(::processData)
            .doOnNext { mxd ->
                logger.info("Fetched ${mxd.data.size} records")
                start+=mxd.data.size
                if(mxd.data.size>0) fetchTrades()
            }.subscribe(::processTrades)
    }

    fun processData(jsonStr : String) : MxdResponse {
        val node = mapper.readTree(jsonStr)
        val data = node.path("trades").path("data")
        val metadata = node.path("trades").path("metadata")
        val columns = node.path("trades").path("columns")
        val colList = mapper.convertValue(columns,object: TypeReference<List<String>>(){})
        val metaMap = (metadata as ObjectNode).fields()
            .asSequence().map { field ->
                val fTypeTxt = (field.value as ObjectNode).get("type").asText()
                Pair(field.key,fTypeTxt)
            }.toMap()
        val pdata = data.map { rec ->
            rec.toList().mapIndexed { ind, el ->
                val value = when (metaMap[colList[ind]]) {
                    "string" -> el.asText()
                    "int64" -> el.asLong()
                    "int32" -> el.asInt()
                    "double" -> el.asDouble()
                    "time" -> LocalTime.parse(el.asText())
                        .atDate(LocalDate.now())
                        .atZone(ZoneId.of("Europe/Moscow"))
                        .toEpochSecond()
                    "datetime" -> el.asText()
                    else -> el.asText()
                }
                Pair(colList[ind],value)
            }.toMap()
        }
        return MxdResponse(metaMap,colList,pdata)
    }

    fun processTrades(resp : MxdResponse) {
        if(resp.data.size==0) return
        val data2Index = filterTradesData(resp)
        val breq = BulkRequest()
        data2Index.forEach { tr ->
            val secName = tr[SECID].toString().lowercase()
            val indexName = "shares-trades-${secName}"
            val indReq = IndexRequest(indexName).id(tr[TRADEID].toString())
            indReq.source(tr)
            breq.add(indReq)
        }
        val resp= elClt.bulk(breq, RequestOptions.DEFAULT)
        if(resp.hasFailures())
            logger.warn("Bulk response: "+resp.buildFailureMessage())
        else logger.info("Stored ${data2Index.size} records")
    }

   private inline fun <T> get2Slices(src: List<T>, sliceSize: Int) : List<T> {
       if(sliceSize>src.size) return src
       val bottom = src.size - sliceSize
       if(bottom<sliceSize) return src
       val retList = src.take(sliceSize).toMutableList()
       retList.addAll(src.takeLast(sliceSize))
       return retList
   }

    fun filterTradesData(resp: MxdResponse) : List<Map<String,Any>> {
        val dataBySec = mutableMapOf<String,MutableList<Map<String,Any>>>()
        resp.data.forEach { rec ->
            if(dataBySec[rec["SECID"].toString()] == null)
                dataBySec[rec["SECID"].toString()]=mutableListOf(rec)
            else dataBySec[rec["SECID"].toString()]!!.add(rec)
        }
        val dataPriceSorted= dataBySec.mapValues { pair -> pair.value.sortedBy { (it["PRICE"] as Double) } }
        val dataVolumeSorted= dataBySec.mapValues { pair -> pair.value.sortedBy { (it["QUANTITY"] as Int) } }
        val trades = emptySet<String>().toMutableSet()
        val dataFiltered = dataPriceSorted
            .mapValues { val res=get2Slices(it.value,1); res.forEach { e -> trades.add(e["TRADENO"].toString()) } ; res }
        val dataFiltered2 = dataVolumeSorted
            .mapValues { it.value.filter { e -> !trades.contains(e["TRADENO"].toString())}.takeLast(1) }
        val dataRet = dataFiltered.flatMap { it.value }.toMutableList()
        dataRet.addAll(dataFiltered2.flatMap { it.value })
        return dataRet
    }
}