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

# Specifica Implementativa Query 1

La Query 1 effettua il monitoraggio in tempo reale dello stato operativo delle principali compagnie aeree: American Airlines (AA), Delta (DL), United (UA) e Southwest (WN). Gli eventi vengono aggregati usando finestre di tipo Tumbling basate sull'Event Time, con una durata di 1 ora logica.

## Topologia del Flusso
Per implementare questa query si utilizza la DataStream API di Flink. La topologia del flusso è definita come segue:

```java
DataStream<Q1OutputRecord> q1ResultStream = mainStream
    .filter(event -> Arrays.asList("AA", "DL", "UA", "WN").contains(event.getAirline()))
    .keyBy(FlightEvent::getAirline)
    .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
    .aggregate(new Q1Aggregator(), new Q1WindowProcessor());
```

### Scelte di Progettazione
- **Uso delle Window API**: si è preferito l'uso delle API nativamente fornite da Flink rispetto a una gestione manuale dello stato e dei timer tramite `KeyedProcessFunction` (basso livello), riducendo la complessità del codice e garantendo la corretta gestione integrata di dati tardivi (*late data*) e avanzamento dei watermark. non riusciremmo ad introdurre ottimizzazioni usando le API di più basso livello, anzi probabilmente sarebbero più ottimizzate quelle datastreams.
- **Partizionamento per Carrier**: la chiave di partizionamento (`keyBy`) basata sulla compagnia aerea (`airline`) distribuisce il carico di lavoro in modo bilanciato sui nodi del cluster, evitando i colli di bottiglia e l'overhead di serializzazione legati a un'aggregazione globale non chiavata.
- **Gestione dei ritardi e Short-Circuit**: in accordo con le specifiche, le statistiche sui ritardi devono escludere i voli cancellati o deviati. Invece di filtrare a monte l'intero flusso (il che impedirebbe di conteggiare il numero di voli cancellati o deviati richiesti per la metrica), tutti i record entrano nella finestra. Il filtro viene applicato all'interno dell'aggregatore con una logica a corto circuito.
- **Gestione dei valori null**: eventuali anomalie o valori nulli sul ritardo di partenza (`DEP_DELAY`) vengono convertiti a `0.0` prima dei calcoli per evitare eccezioni a runtime (`NullPointerException`).

## Ottimizzazione dello Stato e Aggregazione Incrementale
L'operatore `.aggregate()` combina due fasi per ottimizzare l'occupazione di memoria:

### A. Aggregazione Incrementale (`Q1Aggregator`)
Utilizza un'implementazione di `AggregateFunction` per elaborare i record riga per riga man mano che arrivano, invece di accumularli in memoria fino alla scadenza della finestra.
Lo stato occupato per ogni chiave è costante $O(1)$ (rappresentato da un singolo accumulatore `Q1Accumulator` per compagnia aerea).

Logica di aggiornamento dell'accumulatore:
```java
public Q1Accumulator add(FlightEvent event, Q1Accumulator acc) {
    acc.numFlights++;

    if (event.getCancelled() == 1) {
        acc.cancelled++;
        return acc; // Short-circuit: evita controlli sui ritardi per voli cancellati
    }
    
    if (event.getDiverted() == 1) {
        acc.diverted++;
        return acc; // Short-circuit: i voli deviati sono esclusi dalle statistiche sui ritardi
    }

    // Voli completati con successo
    acc.completed++;
    double depDelay = event.getDepDelay() != null ? event.getDepDelay() : 0.0;
    acc.sumDepDelay += depDelay;
    acc.countDepDelay++;

    if (depDelay > 15.0) {
        acc.lateDepartures++;
    }
    return acc;
}
```

### B. Processore di Finestra (`Q1WindowProcessor`)
La componente `ProcessWindowFunction` si attiva solo alla chiusura della finestra temporale (quando il Watermark supera la barriera dell'ora logica).
Ha il compito di estrarre i timestamp di inizio/fine finestra dal contesto della finestra e calcolare le metriche derivate:
* `dep_delay_mean = sum_dep_delay / count_dep_delay`
* `cancellation_rate = (cancelled / num_flights) * 100`
* `late_departure_rate = (late_departures / num_flights) * 100`

## Strategia di Sink ed Ingestion
- **Scrittura su Kafka**: i risultati finali vengono scritti sul topic `flights-q1-results`. Poiché vengono emessi solo 4 record ad ogni chiusura di finestra (uno per compagnia), la scrittura avviene senza chiave per distribuire in modo uniforme i messaggi sulle partizioni del topic di output.
- **Integrazione con Telegraf**: Telegraf consuma dal topic `flights-q1-results`, esegue il parsing dei messaggi e scrive i dati su InfluxDB nella tabella dedicata alla Query 1.