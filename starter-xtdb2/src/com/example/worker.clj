(ns com.example.worker
  (:require [clojure.tools.logging :as log]
            [com.biffweb :as biff :refer [q]]
            [xtdb.api :as xt]))

(defn every-n-minutes [n]
  (iterate #(biff/add-seconds % (* 60 n)) (java.util.Date.)))

(defn print-usage [{:keys [biff/db]}]
  ;; For a real app, you can have this run once per day and send you the output
  ;; in an email.
  (let [n-users (nth (q db
                        '{:find (count user)
                          :where [[user :user/email]]})
                     0
                     0)]
    (log/info "There are" n-users "users.")))

(defn alert-new-user [{:keys [biff.xtdb/node]} tx]
  (doseq [_ [nil]
          :let [db-before (xt/db node {::xt/tx-id (dec (::xt/tx-id tx))})]
          [op & args] (::xt/tx-ops tx)
          :when (= op ::xt/put)
          :let [[doc] args]
          :when (and (contains? doc :user/email)
                     (nil? (xt/entity db-before (:xt/id doc))))]
    ;; You could send this as an email instead of printing.
    (log/info "WOAH there's a new user")))

(defn echo-consumer [{:keys [biff/job] :as ctx}]
  (prn :echo job)
  (when-some [callback (:biff/callback job)]
    (callback job)))

(def module
  {:tasks [{:task #'print-usage
            :schedule #(every-n-minutes 5)}]
   :on-tx alert-new-user
   :queues [{:id :echo
             :consumer #'echo-consumer}]})
