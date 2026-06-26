# Architettura del Sistema

## Componenti della Pipeline
- **Simulatore (Go)**: legge il dataset storico (formato Parquet), gestisce la riproduzione degli eventi applicando un fattore di accelerazione temporale e introduce ritardi controllati per simulare l'arrivo fuori ordine (*out-of-order*) prima dell'invio a Kafka.
- **Message Broker (Apache Kafka)**: backbone per il transito degli stream di input e dei risultati intermedi/finali.
- **Processing Engine (Apache Flink)**: consuma gli eventi da Kafka, gestisce l'assegnazione di Event Time e Watermark, esegue le aggregazioni delle tre query concorrenti e scrive i risultati sui topic di output.
- **Ingestion Agent (Telegraf)**: consuma i risultati di business analitici dai topic di output di Kafka e li scrive automaticamente in InfluxDB, mappando ciascun topic alla rispettiva tabella (*measurement*).
- **Storage e Visualizzazione (InfluxDB + Grafana)**: InfluxDB memorizza i risultati analitici e le metriche di sistema. Grafana mostra i dati tramite due dashboard distinte (una per le metriche prestazionali, una per i risultati di business).

```
[Dataset Parquet] ──> [Simulatore Go] ──> [Kafka Input Topic] ──> [Apache Flink]
                                                                        │
 ┌──────────────────────────────────────────────────────────────────────┘
 │
 ├──(Risultati)──> [Kafka Output Topics] ──> [Telegraf] ──> [InfluxDB] ──> [Grafana]
 │
 └──(Metriche)───> [InfluxDB Reporter] ──────────────────────────▲
```

## Scelte di Progettazione
- **Uso di Go per il simulatore**: garantisce prestazioni elevate e consumo minimo di memoria durante il parsing e il replay accelerato del dataset (circa 2.2 milioni di record), riducendo il rischio di colli di bottiglia locali.
- **Disaccoppiamento tramite Kafka**: l'uso di topic intermedi per l'output garantisce tolleranza ai guasti (fault tolerance). Se Telegraf, InfluxDB o Grafana dovessero andare offline, Flink continua a produrre risultati su Kafka senza perdite. Inoltre, semplifica l'esportazione dei file CSV finali tramite consumatori dedicati.

---

# Struttura del Job Flink (DAG Unica)

Tutte e tre le query sono implementate in un singolo Job Flink (DAG singola) con esecuzione concorrente:

1. **Inizializzazione**: setup di un unico `StreamExecutionEnvironment`.
2. **Sorgente**: definizione di una singola `KafkaSource` per lo stream dei voli.
3. **Watermarking**: assegnazione globale delle strategie di timestamp ed event time sul flusso principale.
4. **Biforcazione (Fork)**: partizione del flusso in tre rami indipendenti:
   - **Q1**: filtro compagnie aeree (AA, DL, UA, WN), partizionamento (`keyBy(airline)`), tumbling window di 1 ora e calcolo delle metriche operative.
   - **Q2**: partizionamento (`keyBy(originAirportId)`), calcolo parallelo su tre finestre temporali distinte (1 ora, 6 ore, globale) e classificazione delle Top-10 tramite operatori di stato.
   - **Q3**: filtro compagnie, estrazione della fascia oraria, partizionamento (`keyBy(airline, hour)`), stima dei percentili su tre finestre temporali tramite algoritmi di sketch (es. t-digest o DDSketch) per limitare l'uso di memoria.
5. **Sink**: scrittura di ciascun ramo sul rispettivo `KafkaSink`.

## Scelte di Progettazione
- **Efficienza di I/O**: evita di leggere e decodificare più volte lo stesso record da Kafka.
- **Coerenza temporale**: sincronizza l'avanzamento dei watermark su tutti gli operatori del job.
- **Gestione della Backpressure**: la sorgente unica adatta il proprio ritmo alla query più lenta. Questo comportamento consente di identificare colli di bottiglia nei test di carico e valutare l'efficacia del parallelismo o dell'isolamento dei gruppi di slot (`SlotSharingGroups`).

---

# Ingestione e Partizionamento

Configurazione del partizionamento per prevenire sbilanciamenti del carico (*data skew*):

- **Partizioni di Input**: il topic `flights-stream` è configurato con partizioni multiple (es. 4 o 8, allineate al parallelismo di Flink).
- **Scrittura (Simulatore -> Kafka)**: il simulatore distribuisce i record in modo uniforme (es. Round-Robin o hash casuale senza chiavi di business) per caricare equamente tutte le partizioni del broker.
- **Lettura (Kafka -> Flink)**: il parallelismo della sorgente Flink è configurato per corrispondere al numero di partizioni Kafka, garantendo che ogni thread consumi una singola partizione.

## Scelte di Progettazione
- **Prevenzione del Data Skew**: l'uso di chiavi logiche (es. aeroporto o compagnia) in fase di scrittura creerebbe colli di bottiglia sulle partizioni associate ad hub molto trafficati (es. Atlanta). La ridistribuzione logica avviene solo all'interno di Flink tramite `.keyBy()`.
- **Watermarking per partizione**: Flink calcola i watermark in modo indipendente su ogni partizione di Kafka, allineando l'Event Time globale al valore minimo per gestire correttamente i record fuori ordine inseriti dal simulatore.

---

# Configurazione dei Topic di Output

I risultati di Flink vengono scritti su tre topic separati:
- `flights-q1-results`
- `flights-q2-results`
- `flights-q3-results`

## Gestione delle Chiavi di Scrittura
- **Q1 e Q3**: i record vengono emessi solo alla chiusura delle finestre (volume ridotto) e scritti senza chiave logica (bilanciamento round-robin).
- **Q2 (Classifiche)**: i record vengono scritti usando `ORIGIN_AIRPORT_ID` come chiave di partizionamento.

## Scelte di Progettazione
- **Separazione delle responsabilità**: l'uso di topic distinti semplifica l'ingestione tramite Telegraf su InfluxDB, consentendo un mapping diretto a specifiche tabelle (measurement) senza logiche di smistamento complesse a livello software.
- **Ordinamento per Q2**: poiché la Query 2 produce aggiornamenti continui nel tempo, partizionare per `ORIGIN_AIRPORT_ID` garantisce che tutti gli aggiornamenti relativi allo stesso aeroporto mantengano l'ordine sequenziale corretto durante la scrittura, evitando sovrascritture errate su InfluxDB.
- **Garanzie di recapito**: i sink utilizzano la modalità `DeliveryGuarantee.AT_LEAST_ONCE` per prevenire perdite di dati. Eventuali rari duplicati vengono risolti implicitamente da InfluxDB tramite sovrascrittura sullo stesso timestamp logico di finestra.

---

# Monitoraggio delle Performance (Metriche)

Strategia per misurare latenza, throughput e comportamento del sistema durante i test di carico:

- **Approccio disaccoppiato**: non vengono utilizzati topic Kafka dedicati alle metriche per non introdurre overhead e alterare le prestazioni del sistema sotto carico.
- **Metric Reporter**: viene abilitato il metric reporter nativo di Flink per InfluxDB (tramite `flink-conf.yaml`), che scrive i dati di telemetria in modo asincrono su un database dedicato.

## Metriche Principali Monitorate
- **Throughput**: `numRecordsInPerSecond` e `numRecordsOutPerSecond` per valutare la scalabilità al variare del parallelismo.
- **Latenza**: calcolata tramite i *Latency Markers* di Flink (`latencyTrackingInterval`) dall'ingresso nel sistema fino al sink.
- **Stato**: `isBackPressured`, `outPoolUsage` e `inPoolUsage` per individuare l'origine di eventuali colli di bottiglia e documentare l'insorgenza della backpressure.

---

# Specifica Implementativa AirlinePerformanceQuery (Query 1)

La Query 1 effettua il monitoraggio in tempo reale dello stato operativo delle principali compagnie aeree: American Airlines (AA), Delta (DL), United (UA) e Southwest (WN). Gli eventi vengono aggregati usando finestre di tipo Tumbling basate sull'Event Time, con una durata di 1 ora logica.

## Topologia del Flusso
Per implementare questa query si utilizza la DataStream API di Flink. La topologia del flusso è definita come segue:

```java
DataStream<String> stream = targetAirlinesStream
        .keyBy(FlightRecord::getAirline)
        .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
        .allowedLateness(Duration.ofMinutes(5))
        .aggregate(new AirlinePerformanceAggregator(), new AirlinePerformanceWindowProcessor())
        .name("Q1: Performance (1h)");
```

### Scelte di Progettazione
- **Uso delle Window API**: si è preferito l'uso delle API nativamente fornite da Flink rispetto a una gestione manuale dello stato e dei timer tramite `KeyedProcessFunction` (basso livello), riducendo la complessità del codice e garantendo la corretta gestione integrata di dati tardivi (*late data*) e avanzamento dei watermark.
- **Partizionamento per Carrier**: la chiave di partizionamento (`keyBy`) basata sulla compagnia aerea (`airline`) distribuisce il carico di lavoro in modo bilanciato sui nodi del cluster, evitando i colli di bottiglia e l'overhead di serializzazione legati a un'aggregazione globale non chiavata.
- **Gestione dei ritardi e Short-Circuit**: in accordo con le specifiche, le statistiche sui ritardi devono escludere i voli cancellati o deviati. Invece di filtrare a monte l'intero flusso (il che impedirebbe di conteggiare il numero di voli cancellati o deviati richiesti per la metrica), tutti i record entrano nella finestra. Il filtro viene applicato all'interno dell'aggregatore con una logica a corto circuito.
- **Gestione dei valori null**: eventuali anomalie o valori nulli sul ritardo di partenza (`DEP_DELAY`) vengono convertiti a `0.0` prima dei calcoli per evitare eccezioni a runtime (`NullPointerException`).

## Ottimizzazione dello Stato e Aggregazione Incrementale
L'operatore `.aggregate()` combina due fasi per ottimizzare l'occupazione di memoria:

### A. Aggregazione Incrementale (`AirlinePerformanceAggregator`)
Utilizza un'implementazione di `AggregateFunction` per elaborare i record riga per riga man mano che arrivano, invece di accumularli in memoria fino alla scadenza della finestra.
Lo stato occupato per ogni chiave è costante $O(1)$ (rappresentato da un singolo accumulatore `AirlinePerformanceAccumulator` per compagnia aerea).

Logica di aggiornamento dell'accumulatore:
```java
public AirlinePerformanceAccumulator add(FlightRecord event, AirlinePerformanceAccumulator acc) {
    acc.numFlights++;

    if (event.isCancelled()) {
        acc.cancelled++;
        return acc; // Short-circuit: evita controlli sui ritardi per voli cancellati
    }
    
    if (event.isDiverted()) {
        acc.diverted++;
        return acc; // Short-circuit: i voli deviati sono esclusi dalle statistiche sui ritardi
    }

    // Voli completati con successo
    acc.completed++;
    double depDelay = event.getDepDelay();
    acc.sumDepDelay += depDelay;
    acc.countDelay++;

    if (depDelay > 15.0) {
        acc.lateDepartures++;
    }
    return acc;
}
```

### B. Processore di Finestra (`AirlinePerformanceWindowProcessor`)
La componente `ProcessWindowFunction` si attiva solo alla chiusura della finestra temporale (quando il Watermark supera la barriera dell'ora logica).
Ha il compito di estrarre i timestamp di inizio/fine finestra dal contesto della finestra e calcolare le metriche derivate:
* `dep_delay_mean = sum_dep_delay / count_dep_delay`
* `cancellation_rate = (cancelled / num_flights) * 100`
* `late_departure_rate = (late_departures / num_flights) * 100`

## Strategia di Sink ed Ingestion
- **Scrittura su Kafka**: i risultati finali vengono scritti sul topic `flights-q1-results`. Poiché vengono emessi solo 4 record ad ogni chiusura di finestra (uno per compagnia), la scrittura avviene senza chiave per distribuire in modo uniforme i messaggi sulle partizioni del topic di output.
- **Integrazione con Telegraf**: Telegraf consuma dal topic `flights-q1-results`, esegue il parsing dei messaggi e scrive i dati su InfluxDB nella tabella `flights_q1_results`.

---

# Specifica Implementativa RankAirportsQuery (Query 2)

La Query 2 individua in tempo reale i primi 10 aeroporti di partenza maggiormente interessati da ritardi significativi (> 30 minuti), calcolati su finestre di 1 ora, 6 ore e dall'inizio del dataset (globale).

## Topologia del Flusso
Il flusso si articola in due fasi principali: aggregazione locale in parallelo per ciascun aeroporto di partenza e classificazione globale delle Top-10 basata sul timestamp di fine finestra.
Per implementare questa query si utilizza la DataStream API di Flink. La topologia del flusso è definita come segue:

```java
// Partizionamento logico per aeroporto d'origine
KeyedStream<FlightRecord, Integer> keyedStream = mainStream.keyBy(FlightRecord::getOriginAirportId);

// Ramo 1h e 6h (Finestre Tumbling)
DataStream<String> w1 = keyedStream
        .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
        .aggregate(new RankAirportsAggregator(), new RankAirportsWindowProcessor("1h"))
        .name("Q2: Window (1h)")
        .keyBy(RankAirportsResult::getWindowEnd)
        .process(new RankAirportsRankProcessor())
        .name("Q2: Rank (1h)");

// Ramo Globale (Stato Storico Continuo)
DataStream<String> wGlobal = keyedStream
        .process(new RankAirportsGlobalStateProcessor())
        .name("Q2: Global Accum")
        .keyBy(RankAirportsResult::getWindowEnd)
        .process(new RankAirportsRankProcessor())
        .name("Q2: Global Rank");
```

### Scelte di Progettazione
- **Separazione delle fasi (Aggregazione vs. Ranking)**: l'ordinamento per determinare la Top-10 richiede una visione globale. Eseguire una partizione globale su tutti i record grezzi causerebbe colli di bottiglia e consumi di I/O insostenibili. La topologia prima aggrega i voli localmente per aeroporto (fase parallela), riducendo drasticamente il volume dei dati, e poi ri-partiziona i record pre-aggregati per timestamp di fine finestra (`keyBy(windowEnd)`) inviandoli ad un singolo operatore di ranking.
- **Filtro di significatività**: in conformità con i requisiti, vengono inclusi nel ranking solo gli aeroporti che registrano almeno 30 voli completati (non cancellati e non deviati) all'interno dell'intervallo temporale considerato.
- **Limitazione dello stato (Top-20)**: memorizzare tutti i voli in ritardo per ogni aeroporto saturerebbe la memoria di Flink. L'accumulatore mantiene una coda ordinata limitata al massimo a 20 voli peggiori.

## Ottimizzazione dello Stato e Aggregazione Incrementale
La pipeline della query 2 combina aggregatori incrementali e operatori di stato personalizzati per garantire un uso efficiente dello stato gestito di Flink.

### A. Aggregazione Incrementale (`RankAirportsAggregator`)
Utilizza `AggregateFunction` con un accumulatore compatto `RankAirportsAccumulator` per aggiornare i conteggi locali in tempo reale senza accumulare i singoli record in memoria.

Logica di inserimento dei voli e aggiornamento delle statistiche nell'accumulatore:
```java
public void add(String carrier, String dest, double depDelay) {
    this.numFlights++;
    this.sumDepDelay += depDelay;
    if (depDelay > this.maxDepDelay) {
        this.maxDepDelay = depDelay;
    }
    if (depDelay > 30.0) {
        this.severeDelays++;
        this.delayedFlights.add(new RankAirportsDelayedFlight(carrier, dest, depDelay));
        Collections.sort(this.delayedFlights);
        // Mantiene solo i 20 voli con ritardo maggiore
        if (this.delayedFlights.size() > 20) {
            this.delayedFlights.remove(this.delayedFlights.size() - 1);
        }
    }
}
```

### B. Processore del Rank (`RankAirportsRankProcessor`)
Questo operatore keyed memorizza temporaneamente le metriche pre-aggregate dei vari aeroporti all'interno di un `ListState`. Alla ricezione di un timer di fine finestra (Event Time), ordina gli aeroporti per numero di ritardi gravi decrescenti e assegna le posizioni da 1 a 10:

```java
public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    List<RankAirportsResult> list = new ArrayList<>();
    for (RankAirportsResult res : windowState.get()) {
        if (res.getNumFlights() >= 30) {
            list.add(res);
        }
    }
    windowState.clear();

    // Ordinamento principale per ritardi gravi, secondario opzionale
    list.sort((a, b) -> Integer.compare(b.getSevereDelays(), a.getSevereDelays()));

    int rank = 1;
    for (int i = 0; i < Math.min(10, list.size()); i++) {
        RankAirportsResult res = list.get(i);
        res.setRank(rank++);
        out.collect(MAPPER.writeValueAsString(res));
    }
}
```

### C. Gestione dello Stato Globale (`RankAirportsGlobalStateProcessor`)
Per l'orizzonte globale, non è possibile usare finestre temporali classiche. Si implementa un `KeyedProcessFunction` che mantiene lo stato progressivo dell'aeroporto tramite un `ValueState<RankAirportsAccumulator>`.
Ogni volta che arriva un record, lo stato viene aggiornato e viene registrato un timer per lo scoccare dell'ora logica successiva, al fine di produrre aggiornamenti periodici regolari:

```java
public void processElement(FlightRecord value, Context ctx, Collector<RankAirportsResult> out) throws Exception {
    if (value.isCancelled() || value.isDiverted()) {
        return;
    }
    RankAirportsAccumulator acc = state.value();
    if (acc == null) {
        acc = new RankAirportsAccumulator();
    }
    acc.add(value.getAirline(), value.getDest(), value.getDepDelay());
    state.update(acc);

    long timestamp = ctx.timestamp();
    long nextHour = ((timestamp / 3600000) + 1) * 3600000;
    ctx.timerService().registerEventTimeTimer(nextHour);
}
```

## Strategia di Sink ed Ingestion
- **Scrittura su Kafka con chiave**: i record vengono serializzati in JSON e inviati ai rispettivi topic `flights-q2-1h-results`, `flights-q2-6h-results` e `flights-q2-global-results`. Per garantire l'ordinamento sequenziale e prevenire inconsistenze dovute a corse critiche di rete, la chiave del record Kafka viene estratta dinamicamente come `origin_airport_id` in formato byte array.
- **Telegraf e InfluxDB**: Telegraf consuma dai tre topic e inserisce le righe all'interno della singola tabella `flights_q2_results` in InfluxDB, impostando `window_type` ("1h", "6h", "global") e `origin_airport_id` come tag di indicizzazione.

---

# Specifica Implementativa DelayDistributionQuery (Query 3)

La Query 3 calcola in tempo reale la distribuzione dei ritardi in partenza per ciascuna compagnia aerea e fascia oraria (24 ore giornaliere ricavate da `CRS_DEP_TIME`), stimando i percentili (25-esimo, 50-esimo/mediana, 75-esimo, 90-esimo), il minimo e il massimo.

## Topologia del Flusso
Il flusso raggruppa i voli tramite una chiave composta da compagnia aerea e ora del giorno (ottenuta dividendo per 100 il valore `CRS_DEP_TIME`), calcolando le statistiche sulle finestre temporali di 1 giorno, 7 giorni e globale:

```java
// Chiave composta: Tuple2<Airline, Hour>
KeyedStream<FlightRecord, Tuple2<String, Integer>> keyedStream = targetAirlinesStream
        .keyBy(
                event -> Tuple2.of(event.getAirline(), event.getCrsDepTime() / 100),
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Integer>>() {})
        );

// Finestre Tumbling 1 giorno e 7 giorni
DataStream<String> w1d = keyedStream
        .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
        .aggregate(new DelayDistributionAggregator(), new DelayDistributionWindowProcessor("1d"))
        .name("Q3: Window (1d)")
        .map(res -> MAPPER.writeValueAsString(res));

// Aggregazione Globale
DataStream<String> wGlobal = keyedStream
        .process(new DelayDistributionGlobalStateProcessor())
        .name("Q3: Global Accum")
        .map(res -> MAPPER.writeValueAsString(res));
```

### Scelte di Progettazione
- **Percentile Sketch Deterministico ad Alte Prestazioni**: il calcolo esatto dei percentili su finestre lunghe o globali richiede la memorizzazione in stato di tutti i singoli ritardi (milioni di elementi), provocando problemi di memoria ed enormi costi di serializzazione ad ogni checkpoint. Si è implementato un algoritmo di stima deterministico basato su un istogramma a secchielli (*bucketed distribution sketch*) incorporato nell'accumulatore.
- **Risoluzione a 1 minuto**: l'intervallo coperto dai bucket va da -100 a 2000 minuti con precisione di 1 minuto (2101 bucket totali). Tutti i valori al di fuori di questo intervallo vengono inseriti nei bucket estremi. Lo spazio di memoria occupato dallo stato è costante $O(1)$ (circa 8 KB) per chiave, a prescindere dal volume di eventi processati.
- **Nessuna libreria esterna**: la logica di stima è sviluppata a mano senza dipendenze per rispettare pienamente le linee guida accademiche del progetto.

## Ottimizzazione dello Stato e Aggregazione Incrementale
La query 3 sfrutta la struttura dell'istogramma compatto per l'aggregazione incrementale.

### A. Aggregazione Incrementale (`DelayDistributionAccumulator`)
L'accumulatore memorizza le frequenze dei minuti di ritardo all'interno di un array di dimensioni fisse, oltre a tracciare minimo, massimo e conteggio totale:

```java
public class DelayDistributionAccumulator implements Serializable {
    private static final int MIN_VAL = -100;
    private static final int MAX_VAL = 2000;
    private static final int NUM_BUCKETS = MAX_VAL - MIN_VAL + 1;

    private final long[] buckets = new long[NUM_BUCKETS];
    private long count = 0L;
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    public void add(double val) {
        this.count++;
        if (val < this.min) this.min = val;
        if (val > this.max) this.max = val;

        int bin = (int) Math.round(val);
        if (bin < MIN_VAL) {
            buckets[0]++;
        } else if (bin > MAX_VAL) {
            buckets[NUM_BUCKETS - 1]++;
        } else {
            buckets[bin - MIN_VAL]++;
        }
    }
}
```

### B. Calcolo e Stima dei Percentili
Durante la generazione dei risultati (alla chiusura di una finestra o al trigger di un timer globale), l'accumulatore esegue una scansione cumulativa dell'istogramma per determinare il minuto corrispondente alla frazione target $q$ dei dati totali:

```java
public double getPercentile(double q) {
    if (count == 0) return 0.0;

    double targetIndex = q * count;
    long cumulative = 0;

    for (int i = 0; i < NUM_BUCKETS; i++) {
        cumulative += buckets[i];
        if (cumulative >= targetIndex) {
            return MIN_VAL + i; // Estrapola il valore originario del ritardo
        }
    }
    return MAX_VAL;
}
```

### C. Gestione dello Stato Globale (`DelayDistributionGlobalStateProcessor`)
Analogamente alla query 2, lo stato cumulativo globale per ciascun gruppo (compagnia, fascia oraria) viene memorizzato tramite un `KeyedProcessFunction` all'interno di un `ValueState<DelayDistributionAccumulator>`.
L'operatore registra un timer giornaliero (Event Time) basato sul timestamp del record in ingresso per emettere periodicamente i percentili consolidati:

```java
public void processElement(FlightRecord value, Context ctx, Collector<DelayDistributionResult> out) throws Exception {
    if (value.isCancelled() || value.isDiverted()) {
        return;
    }
    DelayDistributionAccumulator acc = state.value();
    if (acc == null) {
        acc = new DelayDistributionAccumulator();
    }
    acc.add(value.getDepDelay());
    state.update(acc);

    long timestamp = ctx.timestamp();
    // Registra un timer giornaliero per emettere i risultati
    long nextDay = ((timestamp / 86400000L) + 1) * 86400000L;
    ctx.timerService().registerEventTimeTimer(nextDay);
}
```

## Strategia di Sink ed Ingestion
- **Scrittura su Kafka**: le statistiche sui percentili calcolate vengono prodotte come messaggi JSON sul topic `flights-q3-results`.
- **Telegraf e InfluxDB**: Telegraf intercetta i messaggi JSON e li archivia nella tabella `flights_q3_results` di InfluxDB. I campi memorizzati includono i percentili `p25`, `p50`, `p75`, `p90`, `min` e `max`, con `airline`, `hour` e `window_type` ("1d", "7d", "global") configurati come tag per consentire veloci interrogazioni analitiche in Grafana.

---

# Metriche per analisi del applicazione

monitoriamo:
- backpressure di ogni operatore
- throughut di ogni operatore
- latenza
- checkpointduration e checkpointSize
- garagecollection activity (spesso i picchi di latenza sono causati dallo stop-the-world)

Per ogni ec2 invece misuriamo:
- cpuutilizazione: capire lo sforzo computazionale dei taskmanafer e jobmanager
- networkIn/Out: misura il volume di byte che entrano ed escono dalla istanza EC2
- credit Balancing (t2 o t3): forse c'è credito
- disk read/write (se abbiamo finestre temporali di chiave rocksDB sposta i dati dalla RAM al disco perchè non entrano nella RAM)
- Status.JobManager.Status.NumberOfTaskSlotsTotal
- Status.JobManager.Status.NumberOfTaskSlotsAvailable
- Status.JobManager.Status.NumberOfRegisteredTaskManagers

# Tuning della pipeline

Pensiamo a questi argomenti:
- Stato/checkpointing
- outofordering
- parallelismo adattivo
- semantica garanzia di consegna

## Scenari da considerare

- stabilità del cluster: 
- out of ordering:
- multiplicatore: 

## Checkpointing

Impostazioni del checkponinting:
- checkpointing interval: 
- modalità di consistenza
- tempo minimo di pausa tra i checkpoint
- tempo massimo di checkpoint prima che venga scartato
- Numero massimo di checkpoint concorrenti in esecuzione
- Configura cosa succede se il checkpoint fallisce
- Mantieni i checkpoint anche se il job viene cancellato manualmente
- Abilita l'allineamento non bloccante (Unaligned Checkpoints) - Opzionale per alta contesa

Andiamo ad utilizzare nella nostra produzione un salvataggio su object storagedistribuito in maniera tale da esere tollerante ai guasti dei nodi. Lo storage scelto è S3.
Andiamo ad abilitare il backend rocksdb con gli snapshot incrementali. In questo modo, Flink non caricherà ogni volta tutto lo stato su S3 (operazione pesante), ma caricherà solo le differenze (le modifiche) dall'ultimo checkpoint. RocksDB è un database di tipo Log-Structured Merge-tree (LSM) scritto in C++.

```
state.backend: rocksdb
state.backend.incremental: true
```
