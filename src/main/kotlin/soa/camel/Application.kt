package soa.camel

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.apache.camel.Exchange
import org.apache.camel.Pattern
import org.apache.camel.Processor
import org.apache.camel.ProducerTemplate
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.expression.spel.standard.SpelCompiler.compile
import org.springframework.stereotype.Component
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseBody
import java.util.regex.Pattern.compile


@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}

const val DIRECT_ROUTE = "direct:twitter"
const val COUNT_ROUTE = "direct:extractor"
const val LOG_ROUTE = "direct:log"

@Controller
class SearchController(private val producerTemplate: ProducerTemplate) {
    @RequestMapping("/")
    fun index() = "index"

    @RequestMapping(value = ["/search"])
    @ResponseBody
    fun search(@RequestParam("q") q: String?) {
        producerTemplate.requestBodyAndHeader(DIRECT_ROUTE, "mandalorian", "keywords", q)
    }

}

@Component
class Router(meterRegistry: MeterRegistry) : RouteBuilder() {

    private val perKeywordMessages = TaggedCounter("per-keyword-messages", "keyword", meterRegistry)

    override fun configure() {
        from(DIRECT_ROUTE)

            .toD("twitter-search:\${header.keywords}")
                .process(KeywordProccesor())
            .wireTap(LOG_ROUTE)
            .wireTap(COUNT_ROUTE)

        from(LOG_ROUTE)
            .marshal().json(JsonLibrary.Gson)
            .to("file://log?fileName=\${date:now:yyyy/MM/dd/HH-mm-ss.SSS}.json")

        from(COUNT_ROUTE)
            .split(body())
            .process { exchange ->
                val keyword = exchange.getIn().getHeader("keywords")
                if (keyword is String) {
                    keyword.split(" ").map {
                        perKeywordMessages.increment(it)
                    }
                }
            }
        }
    }

class KeywordProccesor  : Processor {
    override fun process(exchange: Exchange) {

        val msg = (exchange.getIn().getHeader("keywords") as String)
        val tokens = msg.split(" ")
        val resultado = StringBuilder()

        for (splitmsg in tokens) {
            //max:{number} en este caso 10 y endline
            if (splitmsg.matches("max:[0-9]+".toRegex())) {
                val numberAttached = splitmsg.split(":").get(1) //[max,numero] nos quedamos numero
                resultado.append("?count=").append(numberAttached)
            }
            else {
                // añadimos el mensaje
                resultado.append(splitmsg)
            }
        }

        val answer = resultado.toString()
        exchange.getIn().setHeader("keywords",answer)
    }

}

class TaggedCounter(private val name: String, private val tagName: String, private val registry: MeterRegistry) {
    private val counters: MutableMap<String, Counter> = HashMap()
    fun increment(tagValue: String) {
        counters.getOrPut(tagValue) {
            Counter.builder(name).tags(tagName, tagValue).register(registry)
        }.increment()
    }
}

