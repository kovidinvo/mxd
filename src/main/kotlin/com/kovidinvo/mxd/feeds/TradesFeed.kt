package com.kovidinvo.mxd.feeds

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import org.springframework.web.reactive.function.client.WebClient.builder as WebCltBuilder

data class MxdResponse(
    val metadata : Map<String,String>,
    val columns: List<String>,
    val data: List<List<Any>>
)

@Component
class TradesFeed(@Autowired val elClt: RestHighLevelClient) {

    private val mxClt = WebCltBuilder().baseUrl("https://iss.moex.com/iss/engines/stock/markets/shares/")
        .defaultHeader("Content-Type","application/json")
        .exchangeStrategies(ExchangeStrategies.builder()
            .codecs{ cod -> cod.defaultCodecs().maxInMemorySize(16*1024*1024)}.build())
        .build()

    private val mapper = ObjectMapper()

    private var start : Int = 0

    @Scheduled(fixedDelay = 60*1000)
    fun fetchTrades() {
        mxClt.get().uri("trades.json?start=${start}")
            .retrieve().bodyToMono(String::class.java)
            .map(::processData)
            .doOnNext { mxd ->
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
                when (metaMap[colList[ind]]) {
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
            }
        }
        return MxdResponse(metaMap,colList,pdata)
    }

    fun processTrades(resp : MxdResponse) {
        val breq = BulkRequest()
        val secInd = resp.columns.indexOf("SECID")
        val dataSorted = resp.data.sortedBy { it[secInd].toString() }
        dataSorted.forEach { tr ->
            val indReq = IndexRequest("trades-shares-${tr[secInd]}").id(tr[0].toString())
            val source = indReq.sourceAsMap()
            tr.forEachIndexed{ index,fld  -> source[resp.metadata[resp.columns[index]]]=fld }
            breq.add(indReq)
        }
        elClt.bulk(breq, RequestOptions.DEFAULT)
    }
}