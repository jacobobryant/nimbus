(ns com.biffweb.xtdb
  (:require [clojure.string :as str]
            [malli.core :as malli]
            [xtdb.api :as xt]
            [xtdb.node :as xt-node]))

(def ^:private functions
  '[await-tx
    db
    entity
    latest-completed-tx
    listen
    open-q
    open-tx-log
    ;q
    start-node
    ;submit-tx
    sync
    tx-committed?
    with-tx])

(defn- fail [& args]
  (throw (ex-info "Unsupported operation." {})))

(doseq [sym functions]
  (intern 'xtdb.api sym fail))

(def biff-tx-fns
  '{:biff/if-exists (fn [[query query-args] true-branch false-branch]
                      (if (not-empty (q query {:args query-args}))
                        true-branch
                        false-branch))})

(defn use-xtdb [ctx]
  (let [node (xt-node/start-node {})
        stop #(.close node)]
    (xt/submit-tx node (for [[fn-key fn-body] biff-tx-fns]
                         [:put-fn fn-key fn-body]))
    (-> ctx
        (assoc :biff/node node)
        (update :biff/stop conj stop))))

(defn- bind-template [ks]
  (into {}
        (map (fn [k]
               [k (symbol (str/replace (str k) #"^:" "\\$"))]))
        ks))

(defn upsert [table document {:keys [on defaults]}]
  (let [new-doc (merge {:xt/id (random-uuid)} document defaults)
        _ (when-not (malli/validate table new-doc)
            (throw (ex-info "Document doesn't match table schema"
                            {:table table
                             :document document
                             :errors (:errors (malli/explain table new-doc))})))
        query (xt/template (-> (from ~table [~(bind-template on)])
                               (limit 1)))
        on-doc (select-keys document on)]
    [:call :biff/if-exists [query on-doc]
     [[:update {:table table
                :bind [(bind-template on)]
                :set (apply dissoc document on)}
       on-doc]]
     [[:put-docs table new-doc]]]))
