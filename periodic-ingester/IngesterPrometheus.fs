module PeriodicIngesterTemplate.Ingester.Prometheus

let baseName stat = "PeriodicIngesterTemplate_ingester_" + stat
let baseDesc desc = "PeriodicIngesterTemplate: Ingester " + desc

module private Counter =

    let private make (config : Prometheus.CounterConfiguration) name desc =
        let ctr = Prometheus.Metrics.CreateCounter(name, desc, config)
        fun tagValues ->
            ctr.WithLabels(tagValues).Inc

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.CounterConfiguration(LabelNames = tagNames)
        make config (baseName stat) (baseDesc desc) tagValues

module Stats =

    let observeChanged =        Counter.create  ([|"status"|],[|"changed"|])    "outcome_total"     "Outcome"
    let observeUnchanged =      Counter.create  ([|"status"|],[|"unchanged"|])  "outcome_total"     "Outcome"
    let observeStale =          Counter.create  ([|"status"|],[|"stale"|])      "outcome_total"     "Outcome"

    open PeriodicIngesterTemplate.Domain

    let observeIngestionOutcome = function
        | IngestionOutcome.Changed -> observeChanged ()
        | IngestionOutcome.Unchanged -> observeUnchanged ()
        | IngestionOutcome.Stale -> observeStale ()
