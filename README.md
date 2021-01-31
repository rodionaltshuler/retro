# Retro

##Purpose
The tool for retrospective analysis of trade strategies for financial instruments (shares, cryptocurrencies etc.)

[Instruments] examples: SPY stock, BTC/USD cryptocurrency)
[Strategy]: set of rules based on indicators which defines when to place a trading order, and it's params (price, position size, stoploss).
[Indicator]: some value computed value from information available (OHLC, volume etc.)
[Result]: difference in initial and end balance after following the strategy for some period of time.

##Input data
Trading data for some instrument containing Ticks with corresponding timestamps.

Minimal info:
`Timestamp,Open,High,Low,Close`
`1579492440,8635,8638.82,8635,8637`

Extended info could also include volume.

##Output
* Ending balance
* Max drawdown

Extended output to consider:
* Decisions made (orders placed) 
* Stop-losses executed
* Balance changes for each timestamp for further visualization

##Tech stack
* Java 8
* Apache Beam

##Requirements
* JDK 8
* Maven

##How to run
Provide input csv file name and path with ticks and timestamps, and output filename for results:
`mvn exec:java -Dexec.mainClass="com.ottamotta.retro.EnrichTicksWithIndicatorsPipeline --inputFile=./test.csv --output=averages"`


##Development notes

Initial project structure was created from Maven archetype:
`
mvn archetype:generate \
-DarchetypeGroupId=org.apache.beam \
-DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
-DarchetypeVersion=2.27.0 \
-DgroupId=com.ottamotta \
-DartifactId=retro \
-Dversion="0.1" \
-Dpackage=com.ottamotta.retro \
-DinteractiveMode=false
`