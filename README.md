# Traffic Flow Manager - Integrazione con Elasticsearch

## Descrizione
Traffic Flow Manager ("TFM") è un sistema per la gestione e l'inserimento di dati di traffico in Elasticsearch. La funzione principale `sendToEs` riceve dati JSON dinamici, li elabora e li inserisce in Elasticsearch dopo averli riorganizzati e arricchiti con coordinate geografiche.

Commit in Snap4City TrafficFlowManager: [`Commit 20452ce`](https://github.com/disit/snap4city/commit/20452cebae326c5ac386865255f617fdaa4a3c5f)


## Funzionalità principali
- **Elaborazione JSON dinamico**: Conversione dei dati in un formato compatibile con Elasticsearch
- **Frammentazione dei dati**: Separazione dei segmenti e associazione con i relativi elementi stradali
- **Calcolo della densità**: Media dei valori di densità per ogni elemento stradale
- **Recupero coordinate geografiche**: Integrazione con un database esterno per ottenere le coordinate precise
- **Inserimento dati in Elasticsearch**: Uso di thread multipli per ottimizzare il caricamento dei dati

## Configurazione
La configurazione si trova nel file `config.properties` all'interno della cartella `TrafficFlowManager` della VM.

I principali parametri configurabili includono:

```ini
url=<indirizzo_elasticsearch>
admin=<username>
password=<password>
KbUrl=<url_knowledge_base>
BatchSize=<numero_roadelement_per_query>
threadNumber=<numero_thread>
maxErrors=<max_errori_tollerati>
IndexName=<nome_indice_es>
```

## Struttura del flusso di elaborazione
1) Pre-processamento: Formattazione del JSON in un array di documenti con indice basato sul segmento stradale
2) Frammentazione: Creazione di documenti separati per ogni elemento stradale
3) Calcolo della densità: Aggregazione e calcolo della densità media per elemento stradale
4) Costruzione dell'indice invertito: Associazione di ogni elemento stradale ai segmenti di appartenenza
5) Recupero coordinate geografiche: Interrogazione della knowledge base per ottenere le coordinate di ogni elemento
6) Post-processamento: Inserimento delle coordinate recuperate nei documenti
7) Inserimento in Elasticsearch: Invio dei documenti finali a Elasticsearch utilizzando thread multipli

## Aggiunta di nuovi parametri ai documenti
Per aggiungere un nuovo parametro ai documenti, seguire questi passaggi:

1) Modificare il mapping dell'indice su Elasticsearch con la query:
  ``` ini
  PUT /index-name/_mapping
  {
    "properties": {
      "parameter-name": "parameter-type"
    }
  }
  ```
2) Aggiungere il parametro nella funzione `preProcess`.
3) (Opzionale) Se il parametro è una chiave, modificare la funzione `constructQuery` nel file `TrafficFlow.java` e aggiornare `index.jsp` in `ServiceMap/web/api/v1/trafficflow`
