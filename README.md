# Web Engineering 2021-2022 / SOA
**In this assignment your PR must only modify the `README.md` file**.
Please, go to the [Wiki](https://github.com/UNIZAR-30246-WebEngineering/lab5-soa/wiki) in order to get the instructions for this assignment.

## Apache Camel 

Camel is an Open Source integration framework that empowers you to quickly and easily integrate various systems 
consuming or producing data. This is a common SOA scenario. The snippet below:

* Search tweets in Twitter and returns to the web client
* Dump to disk at least one of tweets retrieved encoded in JSON in the folder `log/{year}/{month}/{day}`
* Compute metrics (available at `/actuator/metrics/per-keyword-messages?tag=keyword:${value}`, values available at `http://localhost:8080/actuator/metrics/per-keyword-messages`)

```kotlin
from(DIRECT_ROUTE)
    .toD("twitter-search:\${header.keywords}") // Twitter search, returns a list of Tweets
    .wireTap(LOG_ROUTE) // Copy the list of tweets to the LOG_ROUTE, which is processed by its own thread  
    .wireTap(COUNT_ROUTE) // Copy the list of tweets to the COUNT_ROUTE, which is processed by its own thread
    // returns a list of tweets to the DIRECT_ROUTE caller

from(LOG_ROUTE)
    .marshal().json(JsonLibrary.Gson) // Encode the list of tweets as JSON
    .to("file://log?fileName=\${date:now:yyyy/MM/dd/HH-mm-ss.SSS}.json") // Write the JSON to a file

from(COUNT_ROUTE)
    .split(body()) // Split the list in single Tweets
    .process { exchange -> // Process a single Tweet
        val keyword = exchange.getIn().getHeader("keywords") 
        if (keyword is String) {
            keyword.split(" ").map {
                perKeywordMessages.increment(it)
            }
        }
    }
```

## Primary goal

The objective is to extend the query interface to support the command `max:n`, where _n_ is a number.
`max` sets a limit in the number of retrieved tweets.
That is `cool max:10` must return at most 10 tweets.
There are several ways to do it.
You can use the parameter `q` or create additional parameters.
There is a restriction.
You must explicitly tell the API Twitter to enforce such a limit.
Read the documentation of the [Twitter Search component](https://camel.apache.org/components/latest/twitter-search-component.html) of Apache Camel to discover how.

You need to [apply for a free Twitter developer account](https://developer.twitter.com/en/apply-for-access).

**Do not add `application.properties` with the Twitter tokens to your git!**

Note: the Twitter Search endpoint is configured using URI syntax `twitter-search:{string}[?param=value[&param=value]*]`

## Secondary goals (:gift:)

Proposed:

- [Use Camel for providing a WebSocket endpoint for queries](https://camel.apache.org/components/latest/websocket-jsr356-component.html); a websocket client is required.
- [Use Camel for keeping a copy of responses in a database](https://camel.apache.org/components/latest/jdbc-component.html)
- [Enrich the response with moustache](https://camel.apache.org/components/latest/eips/content-enricher.html); remove moustache from the client.
- [Create an operator "-" to filter out tweets that contain a keyword](https://camel.apache.org/components/latest/eips/filter-eip.html)

The link provides one of the possible approaches. You can use a different one.

The possibilities of Camel are endless; you can propose your idea for a :gift:.

Manifest your intention first by a PR updating this `README.md` with your goal.
If you desist of your goal, release it by a PR so other fellow can try it.

|User name | NIA | Status |  Improvement | Score  |
|-------|-----------|------|-------------|--------|
|[UNIZAR-30246-WebEngineering](https://github.com/UNIZAR-30246-WebEngineering/lab5-soa) | 30246 | [![Build Status](https://github.com/UNIZAR-30246-WebEngineering/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/UNIZAR-30246-WebEngineering/lab5-soa/actions/workflows/ci.yml) | [instructions](https://github.com/UNIZAR-30246-WebEngineering/lab5-soa/wiki) | |
|[Diego Marco](https://github.com/dmarcob/lab5-soa/tree/work) | 755232 | [![Build Status](https://github.com/dmarcob/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/dmarcob/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/dmarcob/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |
|[Alejandro Magallón](https://github.com/alecron/lab5-soa/tree/work) | 779354 | [![Build Status](https://github.com/alecron/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/alecron/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/alecron/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |
|[Óscar Pueyo](https://github.com/iksopo/lab5-soa/tree/work) | 780378 | [![Build Status](https://github.com/iksopo/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/iksopo/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/iksopo/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |
|[María Peña](https://github.com/Keyleth8/lab5-soa/tree/work) | 780448 | [![Build Status](https://github.com/Keyleth8/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/Keyleth8/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/Keyleth8/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |        | 
|[Tomás Pelayo](https://github.com/Tomenos18/lab5-soa/tree/work) | 779691 | [![Build Status](https://github.com/Tomenos18/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/Tomenos18/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/Tomenos18/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |        | 
|[Jorge Laguna](https://github.com/topopelon/lab5-soa/tree/work) | 735550 | [![Build Status](https://github.com/topopelon/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/topopelon/lab5-soa/actions/workflows/ci.yml) | [Throttle the route to avoid be banned by Twitter](https://github.com/topopelon/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) | :gift:
|[María Peña](https://github.com/Keyleth8/lab5-soa/tree/work) | 780448 | [![Build Status](https://github.com/Keyleth8/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/Keyleth8/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/Keyleth8/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |
|[Andoni Salcedo](https://github.com/AndoniSalcedo/lab5-soa/tree/work) | 785649 | [![Build Status](https://github.com/AndoniSalcedo/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/AndoniSalcedo/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/AndoniSalcedo/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |
|[Alejandro Artal](https://github.com/Alejandro-Artal/lab5-soa/tree/work) | 775929 | [![Build Status](https://github.com/Alejandro-Artal/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/Alejandro-Artal/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/Alejandro-Artal/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt)
|[Jaime Conchello](https://github.com/jaimecb/lab5-soa/tree/work) | 776012 | [![Build Status](https://github.com/jaimecb/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/jaimecb/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/jaimecb/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt) |        | 
|[Fernando Serrano](https://github.com/Feer93/lab5-soa/tree/work) | 774840 | [![Build Status](https://github.com/Feer93/lab5-soa/actions/workflows/ci.yml/badge.svg)](https://github.com/Feer93/lab5-soa/actions/workflows/ci.yml) | [Solution](https://github.com/Feer93/lab5-soa/blob/work/src/main/kotlin/soa/camel/Application.kt)| [Rest EndPoint](https://github.com/Feer93/lab5-soa/tree/gift) :gift:   
