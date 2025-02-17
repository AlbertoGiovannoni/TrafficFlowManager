# Traffic Flow Manager e Service Map
Questo repository è suddiviso in due principali componenti:

- **TrafficFlowManager:** Contiene la logica per l'elaborazione e l'inserimento dei dati di traffico in Elasticsearch, inclusa l'API sendToEs.
- **ServiceMap:** Fornisce l'API TrafficFlowSearch per interrogare Elasticsearch e ottenere i dati inseriti tramite TrafficFlowManager.

Commit in Snap4City TrafficFlowManager: [`Commit 20452ce`](https://github.com/disit/snap4city/commit/20452cebae326c5ac386865255f617fdaa4a3c5f)

## Traffic Flow Manager - Integrazione con Elasticsearch
### Descrizione

TrafficFlowManager (`TFM`) è il modulo che gestisce l'inserimento dei dati di traffico in Elasticsearch. La funzione principale `sendToEs` riceve JSON dinamici, li elabora e li inserisce in Elasticsearch dopo averli riorganizzati e arricchiti con coordinate geografiche.



### Funzionalità principali

- **Elaborazione JSON dinamico**: Conversione dei dati in un formato compatibile con Elasticsearch.

- **Frammentazione dei dati**: Separazione dei segmenti e associazione con i relativi elementi stradali.

- **Calcolo della densità**: Media dei valori di densità per ogni elemento stradale.

- **Recupero coordinate geografiche**: Integrazione con un database esterno per ottenere le coordinate precise.

- **Inserimento dati in Elasticsearch**: Uso di thread multipli per ottimizzare il caricamento dei dati.

- **Index Template**: Per garantire la corretta integrazione con Elasticsearch, è stata definita una struttura di **index template** che i JSON inseriti da `sendToEs` devono rispettare. Questi template definiscono il formato dei dati e gli attributi necessari per una corretta indicizzazione.

### Esempio di JSON inserito in Elasticsearch

```json
{
  "dateObserved": "2023-09-28T13:00:00",
  "scenario": "cinque",
  "roadElements": "OS00491331618RE/1",
  "kind": "reconstructed",
  "line": {
    "type": "Polygon",
    "coordinates": [
      [
        [11.2527, 43.0000],
        [11.2527, 43.9905],
        [11.2691, 43.9803],
        [11.2527, 43.0000]
      ]
    ]
  }
}
```

### API SendToEs

La funzione `sendToEs` è sviluppata come API per l'inserimento dei dati di traffico in Elasticsearch. Viene richiamata all'interno della servlet `UploadLayerServlet` nel metodo `doPost` per il caso `reconstruction`.



## ServiceMap

### Descrizione

ServiceMap è il modulo che fornisce l'API `TrafficFlowSearch` per interrogare Elasticsearch e ottenere dati relativi a road elements e segmenti inseriti tramite TrafficFlowManager.



### API TrafficFlowSearch

Permette di interrogare Elasticsearch con diversi parametri:

- `dateObservedStart` e `dateObservedEnd`: Intervallo temporale per filtrare i dati.

- `scenario`: Nome dello scenario.

- `roadElement`: Codice del road element di interesse.

- `kind`: Tipo di dati (`reconstructed`, `predicted`, `TTT`, `measured`).

- `geometry`: Area di ricerca in formato WKT.



Esempio di query:

```sh

http://<domain>/ServiceMap/api/v1/trafficflow/?geometry=POLYGON ((11.2500 43.0000, 11.2527 43.9905, 11.2691 43.9803, 11.2500 43.0000))&dateObserved=2023-11-20T10:00:00&scenario=orion-1_Organization_deviceNamemarcoscenario&roadElement=OS00102356073RE/8&kind=reconstructed

```



Formato di output:

```json

{

    "result":[],

    "time_total": 0,

    "error": "",

    "time_query": 0,

    "status": "ok"

}

```
Il campo result contiene tutti i documenti che rispettano la query eseguita
