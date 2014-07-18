(ns sparrow.core
  (:require [cljs.core.async :refer [go-loop chan <! >! put! close!]]))

(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
nested structure. keys is a sequence of keys. Any empty maps that result
will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(declare register send-request send-job send-result)

(defn scheduler
  []
  (let [request-c (chan 1024)]
    (go-loop [state {}]
      (let [{:keys [type request]} (<! request-c)]
        (condp identical? type
          :register (recur (register state request))
          :request (recur (send-request state request))
          :ready (recur (send-job state request))
          :result (recur (send-result state request)))))
    request-c))

(defn register
  [state {:keys [id chan]}]
  (update-in state [:workers id] chan))

(declare select-workers send-to-workers)

(defn send-request
  [state request]
  (let [{:keys [workers] state}
        request-id (java.util.UUID/randomUUID)
        jobs (select-workers workers [request-id])
        state (assoc-in state [:requests request-id] request)]
    (send-to-workers state jobs)))

(defn select-workers
  [workers requests]
  (let [job-count (* 2 (count requests))
        worker-pool (cycle (shuffle workers))
        request-pool (cycle requests)]
    (take job-count (map vector worker-pool request-pool))))

(defn send-to-worker
  [state [[id chan] request-id]]
  (put! chan {:type :reserve :id request-id})
  (assoc-in state [:queued-jobs request-id id] true))

(defn send-to-workers
  [state jobs]
  (reduce send-to-worker state jobs))

(defn cancel-job
  [state request-id]
  (let [remaining-worker (-> state :queued-jobs request-id first key)
        worker-chan (get-in state :workers remaining-worker)]
    (put! worker-chan {:type :cancel :request-id request-id})
    (dissoc-in state [:queued-jobs request-id])))

(defn send-job
  [state {:keys [id request-id]}]
  (let [request (get-in state [:requests request-id])
        worker-chan (get-in state [:workers id])]
    (put! worker-chan {:type :request :request request})
    (-> state
        (dissoc-in [:requests request-id])
        (dissoc-in [:queued-jobs request-id id])
        (cancel-job state request-id))))
