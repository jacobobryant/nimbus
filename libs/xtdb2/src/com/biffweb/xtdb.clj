(ns com.biffweb.xtdb
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [malli.core :as malli]
            [xtdb.api :as xt]
            [xtdb.node :as xt-node])
  (:import [java.time Instant]))

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

(defn check-attr-schema [attr schema value]
  (when-not (malli/validate schema value)
    (throw (ex-info "Value doesn't match attribute schema"
                    {:attr attr
                     :schema schema
                     :value value
                     :errors (:errors (malli/explain schema value))}))))

(defn check-table-schema [table document]
  (when-not (malli/validate table document)
    (throw (ex-info "Document doesn't match table schema"
                    {:table table
                     :document document
                     :errors (:errors (malli/explain table document))}))))

(defn compile-op-dispatch [op]
  (first op))

(defmulti compile-op #'compile-op-dispatch)

(defmethod compile-op :default
  [[:as op]]
  [op])

(defn- bind-template [ks]
  (into {}
        (map (fn [k]
               [k (symbol (str/replace (str k) #"^:" "\\$"))]))
        ks))

(defmethod compile-op :biff/upsert
  [[_ table {document :set :keys [on defaults]}]]
  (let [new-doc (merge {:xt/id (random-uuid)} document defaults)
        _ (check-table-schema table new-doc)
        query (xt/template (-> (from ~table [~(bind-template on)])
                               (limit 1)))
        on-doc (select-keys document on)]
    [[:call :biff/if-exists [query on-doc]
      [[:update {:table table
                 :bind [(bind-template on)]
                 :set (apply dissoc document on)}
        on-doc]]
      [[:put-docs table new-doc]]]]))

(defmethod compile-op :put-docs
  [[_ table-or-opts & docs :as op]]
  (let [table (if (map? table-or-opts)
                (:into table-or-opts)
                table-or-opts)]
    (doseq [doc docs]
      (check-table-schema table doc))
    [op]))

(defmethod compile-op :update
  [[_ {set-value :set :keys [table]} & arg-rows :as op]]
  (doseq [[attr value] set-value
          :let [schema (-> (malli/schema table)
                           malli/ast
                           :keys
                           attr
                           :value
                           malli/from-ast)]
          args (or arg-rows [{}])
          :let [value (walk/postwalk
                       (fn [x]
                         (let [kw (delay (keyword (subs (str x) 1)))]
                           (if (and (symbol? x)
                                    (str/starts-with? (str x) "$")
                                    (contains? args @kw))
                             (@kw args)
                             x)))
                       value)]]
    (check-attr-schema attr schema value))
  [op])

(defn compile-tx [biff-tx]
  (reduce (fn [tx op]
            (into tx (compile-op op)))
          []
          biff-tx))

(defn submit-tx [node biff-tx]
  (xt/submit-tx node (compile-tx biff-tx)))

(comment
  ;; success

  (-> (malli/schema :user)
      malli/ast
      )

  (compile-tx
   [[:put-docs :user {:xt/id (random-uuid)
                      :email "alice@example.com"
                      :color "yellow"
                      :joined-at (Instant/now)}]
    [:biff/upsert :user {:set {:color "brown" :email "alice@example.com"}
                         :on [:email]
                         :defaults {:joined-at (Instant/now)}}]
    [:update '{:table :user
               :bind [{:email $email}]
               :set {:color "green"}}
     {:email "alice@example.com"}]])

  ;; fail

  (compile-tx
   [[:put-docs :user {:xt/id (random-uuid) :email "alice@example.com" :color :yellow}]])

  (compile-tx
   [[:biff/upsert :user {:set {:color "brown" :email "alice@example.com"}
                         :on [:email]
                         :defaults {:joined-at "now"}}]])

  (compile-tx
   [[:update '{:table :user
               :bind [{:email $email}]
               :set {:email "alice_example.com"}}
     {:email "alice@example.com"}]])

  )
