;;; Records the results of runs to `ici-recorder`.
;;;
;;; We want to record two types of information:
;;;   1. The initial run configuration
;;;   2. The results of a generation
;;; Each run has a UUID. Each generation is recorded with a reference to this
;;; UUID and it's index.
;;;
;;; We build up the data we want to send for the generation/run over time
;;; and then send it off.
(ns clojush.pushgp.record
  (:require [clojure.java.io]
            [cheshire.core]
            [cheshire.generate]))

(cheshire.generate/add-encoder
  clojure.lang.AFunction
  cheshire.generate/encode-str)

; https://github.com/clojure-cookbook/clojure-cookbook/blob/master/05_network-io/5-09_tcp-client.asciidoc
(def writer (atom nil))

(defn ->writer []
  (-> (java.net.Socket. "35.185.102.205" 9990)
    clojure.java.io/writer))

(defn set-writer! []
  (println "SETTING WRITER")
  (try
    (reset! writer (->writer))
    (catch java.net.ConnectException _
      (Thread/sleep 5000)
      (set-writer!))))

(set-writer!)


(defn- write-data! [data]
  (try
    (do
      (cheshire.core/generate-stream data @writer)        
      (.newLine @writer)
      (.flush @writer))
    (catch java.net.SocketException _
      (set-writer!)
      (write-data! data))))

(def data (atom {}))


;; Stores a configuration options for the run, for the sequence of `ks` and value `v`
(defn config-data! [ks v]
  (swap! data assoc-in (cons :config ks) v)
  v)

;; called at the begining of a new run.
;;
;; Resets the state and creates UUID
(defn new-run! []
  (reset! data {}))

(defn uuid! [uuid]
  (swap! data assoc :uuid uuid))

  ;; commented out until apache spark supports timestamp_millis
  ;; https://github.com/apache/spark/pull/15332
  ; (config-data! [:start-time] (java.time.Instant/now)))

;; Records the run configuration with `ici-recorder`
(defn end-config! [])
;   (let [{:keys [configuration uuid]} @data]
;     (write-data!
;       config-writer
;       (assoc configuration :uuid uuid))))

;; Called at the begining of a generation
(defn new-generation! [index]
  (swap!
    data
    assoc
    :index index))
    ; :generation {:start-time (java.time.Instant/now)}))
      

;; stores some data about the generation
(defn generation-data! [ks v]
  (swap! data assoc-in (cons :generation ks) v)
  v)

;; records the generation with `ici-recorder`
(defn end-generation! []
  (let [{:keys [generation uuid index config]} @data]
    (write-data!
      (assoc generation
        :config-uuid uuid
        :index index
        :config config))))
