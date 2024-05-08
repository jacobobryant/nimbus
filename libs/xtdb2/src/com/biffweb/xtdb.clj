(ns com.biffweb.xtdb
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [malli.core :as malli]
            [malli.error :as me]
            [xtdb.api :as xt]
            [xtdb.node :as xt-node])
  (:import [java.time Instant]
           [xtdb.api IXtdb]))

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

(def ^:private xtdb-node?
  [:fn #(instance? IXtdb %)])

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

(defn -malli-wrap [{:keys [value message] :as error}]
  (str "Invalid argument `" (pr-str value) "`: " message))

(defn check-args* [& arg-maps]
  (doseq [{:keys [value schema quoted-schema]} arg-maps
          :when (not (malli/validate schema value))]
    (throw (ex-info (str "Invalid argument: "
                         (pr-str value) " doesn't satisfy schema `"
                         (pr-str quoted-schema) "`")
                    {:argument value
                     :schema quoted-schema}))))

(defmacro check-args [& args]
  (when-not (even? (count args))
    (throw (clojure.lang.ArityException. (+ (count args) 2) "check-args")))
  `(check-args* ~@(for [[value schema] (partition 2 args)]
                    {:value value
                     :schema schema
                     :quoted-schema (list 'quote schema)})))

(defmacro check-arity [valid n-args fn-name]
  `(when-not ~valid
     (throw (clojure.lang.ArityException. ~n-args ~fn-name))))

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
  [[_ table on-doc & [{set-doc :set :keys [defaults]}]]]
  (check-args table :keyword
              on-doc map?
              set-doc [:maybe map?]
              defaults [:maybe map?])
  (let [new-doc (merge {:xt/id (random-uuid)} set-doc defaults on-doc)
        _ (check-table-schema table new-doc)
        on-keys (keys on-doc)
        query (xt/template (-> (from ~table [~(bind-template on-keys)])
                               (limit 1)))]
    [[:call :biff/if-exists [query on-doc]
      (when (not-empty set-doc)
        [[:update {:table table
                   :bind [(bind-template on-keys)]
                    ;; TODO try to pass in set-doc via args? does it even matter?
                   :set set-doc}
          on-doc]])
      [[:put-docs table new-doc]]]]))

(defmethod compile-op :biff/delete
  [[_ table & ids]]
  (check-args table :keyword
              ids [:sequential {:min 1} any?])
  [(into [:delete {:from table :bind '[{:xt/id $id}]}]
         (for [id ids]
           {:id id}))])

(defmethod compile-op :biff/update
  [[_ table {set-doc :set :keys [where]}]]
  (check-args table :keyword
              set-doc map?
              where map?)
  [[:update {:table table
             :bind [(bind-template (keys where))]
             :set (bind-template (keys set-doc))}
    (merge set-doc where)]])

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

(defn- check-lookup-args [fn-name node table kvs]
  (check-arity (even? (count kvs)) (+ (count kvs) 2) fn-name)
  (check-args node xtdb-node? table :keyword))

(defn lookup-id [node table & kvs]
  (check-lookup-args "lookup-id" node table kvs)
  (let [args (apply hash-map kvs)
        query (xt/template
               (-> (from ~table [xt/id ~(bind-template (keys args))])
                   (limit 1)))]
    (-> (xt/q node query {:args args}) first :xt/id)))

(defn lookup-id-all [node table & kvs]
  (check-lookup-args "lookup-id-all" node table kvs)
  (let [args (apply hash-map kvs)
        query (xt/template (from ~table [xt/id ~(bind-template (keys args))]))]
    (mapv :xt/id (xt/q node query {:args args}))))

(defn lookup [node table & kvs]
  (check-lookup-args "lookup" node table kvs)
  (let [args (apply hash-map kvs)
        query (xt/template
               (-> (from ~table [* ~(bind-template (keys args))])
                   (limit 1)))]
    (first (xt/q node query {:args args}))))

(defn lookup-all [node table & kvs]
  (check-lookup-args "lookup-all" node table kvs)
  (let [args (apply hash-map kvs)
        query (xt/template (from ~table [* ~(bind-template (keys args))]))]
    (xt/q node query {:args args})))

(comment
  (bind-template [:foo :bar])

  (with-open [node (xt-node/start-node)]
    (xt/submit-tx node [[:put-docs :user {:xt/id 1 :email "alice@example.com" :color "blue"}]
                        [:put-docs :user {:xt/id 2 :email "bob@example.com" :color "blue"}]])
    ;(xt/q node '(from :user [*]))
    [(lookup-id node :user :email "bob@example.com")
     (lookup-id-all node :user :color "blue")
     (lookup node :user :email "bob@example.com")
     #_(first (xt/q node '(from :user [* {:email $email}]) {:args {:email "bob@example.com"}}))


     (lookup-all node :user :color "blue")
     ])

  (with-open [node (xt-node/start-node)]
    (xt/submit-tx node [[:put-docs :user {:xt/id 1 :email "alice@example.com" :color "blue"}]
                        [:put-docs :user {:xt/id 2 :email "bob@example.com" :color "blue"}]
                        #_[:delete '{:from :user :bind [{:email $email}]} {:email "bob@example.com"}]
                        ;[:delete '{:from :user :bind [{:xt/id $id}]} {:id 2}]
                        
                        #_[:sql "delete from user where user.email = ?" ["bob@example.com"]]
                        ])
    (prn (xt/q node '(from :user [*])))
    ;(xt/submit-tx node (compile-tx [[:biff/delete :user 1 2]]))
    (submit-tx node [[:biff/delete :user 1 2]])
    ;(compile-tx [[:biff/delete :user 1 2]]) ; [[:delete {:from :user, :bind [#:xt{:id $id}]} {:id 1} {:id 2}]]
    (prn (xt/q node '(from :user [*]))))

  (with-open [node (xt-node/start-node)]
    (xt/submit-tx node [[:put-docs :biff.auth/code {:xt/id 1
                                                    :email "alice@example.com"
                                                    :code "123456"
                                                    :created-at (Instant/now)
                                                    :failed-attempts 0}]
                        [:update '{:table :biff.auth/code
                                   :bind [{:xt/id $id} failed-attempts]
                                   :set {:failed-attempts (+ failed-attempts 1)}}
                         {:id 1}]
                        
                        ])
    (xt/q node '(from :biff.auth/code [*]))
    )

  (with-open [node (xt-node/start-node)]
    #_(xt/submit-tx node [[:put-docs :user {:xt/id 1 :email "alice@example.com" :color "blue"}]
                        [:put-docs :user {:xt/id 2 :email "bob@example.com" :color "blue"}]])
    (xt/q node '(-> (from :user [:xt/id])
                        (aggregate {:n (row-count)}))))

  (with-open [node (xt-node/start-node)]
    (submit-tx node [[:put-docs :user {:xt/id #uuid "8281268b-712d-4e56-b84e-398936bbd0d1"
                                       :email "alice@example.com"
                                       :joined-at (Instant/now)
                                       :foo "blue"}]
                     [:biff/update :user {:set {:foo "green"}
                                          :where {:xt/id #uuid "8281268b-712d-4e56-b84e-398936bbd0d1"}}]
                     ])
    (xt/q node '(from :user [*])))

  (with-open [node (xt-node/start-node)]
    (xt/submit-tx node [[:put-docs :message {:xt/id 1, :text "hello", :sent-at #time/instant "2023-05-02T22:27:53.336565627Z"}]
                        [:put-docs :message {:xt/id 2, :text "there", :sent-at #time/instant "2024-05-02T22:27:53.336565627Z"}]])
    (xt/q node '(-> (from :message [* sent-at])
                    (where (< $t0 sent-at)))
          {:args {:t0 (.minus (Instant/now) #time/period "P365D")}}))


  (apply hash-map [:email "bob@example.com"])

  ;; success

  (compile-tx
   [[:put-docs :user {:xt/id (random-uuid)
                      :email "alice@example.com"
                      :color "yellow"
                      :joined-at (Instant/now)}]
    [:biff/upsert :user {:email "alice@example.com"} {:set {:color "brown"}
                                                      :defaults {:joined-at (Instant/now)}}]
    [:update '{:table :user
               :bind [{:email $email}]
               :set {:color "green"}}
     {:email "alice@example.com"}]])

  ;; fail

  (compile-tx
   [[:put-docs :user {:xt/id (random-uuid) :email "alice@example.com" :color :yellow}]])

  (compile-tx
   [[:biff/upsert :user {:email "alice@example.com"} {:set {:color "brown"}
                                                      :defaults {:joined-at "now"}}]])

  (compile-tx
   [[:update '{:table :user
               :bind [{:email $email}]
               :set {:email "alice_example.com"}}
     {:email "alice@example.com"}]])

  )
