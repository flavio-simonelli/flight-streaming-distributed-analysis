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