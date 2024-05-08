(ns repl
  (:require [com.example :as main]
            [com.biffweb.xtdb :as bxt :refer [submit-tx]]
            [com.biffweb :as biff :refer [q]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xtdb.api :as xt]
            [malli.core :as malli])
  (:import [java.time Instant]))

;; REPL-driven development
;; ----------------------------------------------------------------------------------------
;; If you're new to REPL-driven development, Biff makes it easy to get started: whenever
;; you save a file, your changes will be evaluated. Biff is structured so that in most
;; cases, that's all you'll need to do for your changes to take effect. (See main/refresh
;; below for more details.)
;;
;; The `clj -M:dev dev` command also starts an nREPL server on port 7888, so if you're
;; already familiar with REPL-driven development, you can connect to that with your editor.
;;
;; If you're used to jacking in with your editor first and then starting your app via the
;; REPL, you will need to instead connect your editor to the nREPL server that `clj -M:dev
;; dev` starts. e.g. if you use emacs, instead of running `cider-jack-in`, you would run
;; `cider-connect`. See "Connecting to a Running nREPL Server:"
;; https://docs.cider.mx/cider/basics/up_and_running.html#connect-to-a-running-nrepl-server
;; ----------------------------------------------------------------------------------------

;; This function should only be used from the REPL. Regular application code
;; should receive the system map from the parent Biff component. For example,
;; the use-jetty component merges the system map into incoming Ring requests.
(defn get-context []
  (biff/merge-context @main/system))

(defn add-fixtures []
  (biff/submit-tx (get-context)
    (-> (io/resource "fixtures.edn")
        slurp
        edn/read-string)))

(defn check-config []
  (let [prod-config (biff/use-aero-config {:biff.config/profile "prod"})
        dev-config  (biff/use-aero-config {:biff.config/profile "dev"})
        ;; Add keys for any other secrets you've added to resources/config.edn
        secret-keys [:biff.middleware/cookie-secret
                     :biff/jwt-secret
                     :mailersend/api-key
                     :recaptcha/secret-key
                     ; ...
                     ]
        get-secrets (fn [{:keys [biff/secret] :as config}]
                      (into {}
                            (map (fn [k]
                                   [k (secret k)]))
                            secret-keys))]
    {:prod-config prod-config
     :dev-config dev-config
     :prod-secrets (get-secrets prod-config)
     :dev-secrets (get-secrets dev-config)}))

(comment
  ;; Call this function if you make a change to main/initial-system,
  ;; main/components, :tasks, :queues, config.env, or deps.edn.
  (main/refresh)

  ((:biff/secret (get-context)) :recaptcha/secret)
  (:recaptcha/site-key (get-context))

  (def node (:biff/node (get-context)))

  (xt/status node)

  (submit-tx node [[:biff/upsert :user {:set {:email "alice@example.com"
                                              :color "red"}
                                        :on [:email]
                                        :defaults {:joined-at (Instant/now)}}]
                   [:biff/upsert :user {:set {:email "bob@example.com"
                                              :color "green"}
                                        :on [:email]
                                        :defaults {:joined-at (Instant/now)}}]])

  (:xt/id (xt/q node '(from :user [xt/id {:email $email}])
                {:args {:email "alice@example.com"}}))

  ;; TODO:
  ;; - put with schema check
  ;; - merge op
  ;; - unique field (e.g. email)
  ;; - set attr to "now"
  ;; - create op
  ;; - upsert (e.g. on email address)
  ;; - attr ops: union, difference, add, default
  ;; - update dissoc


  (first (xt/q node '(from :users [xt/id {:email $email}]) {:args {:email "alice@example.com"}}))



  (xt/submit-tx node
    [[:put-docs :users {:xt/id :alice :email "alice@example.com" :color "yellow"}]
     [:put-docs :users {:xt/id :bob :email "bob@example.com" :color "yellow"}]
     ])

  (xt/q node '(from :users [*]))

  (xt/submit-tx node [[:delete {:from :users, :bind '[{:email $email}]} {:email "alice@example.com"}]])
  (xt/submit-tx node [[:sql "delete from users where users.email = ?" ["alice@example.com"]]])

  (xt/q node "select users.xt$id from users where users.email = ?" {:args ["alice@example.com"]})
  (xt/q node "select * from xt$txs")

  (xt)

  (bxt/submit-tx node
    [[:put-docs :user {:xt/id :alice :email "alice@example.com" :color "yellow"}]
     [:biff/upsert :user {:set {:color "brown" :email "alice@example.com"}
                          :on [:email]
                          :defaults {:joined-at (Instant/now)}}]])

  (let [users (xt/q node "select users.xt$id from users where users.email = ?" {:args ["alice@example.com"]})
        tx (if (empty? users)
             [[:assert-not-exists '(from :users [{:email $email}]) {:email "alice@example.com"}]
              [:put-docs :users {:xt/id :alice :email "alice@example.com" :color "red"}]]
             [[:update '{:table :users
                         :bind [{:email $email}]
                         :set {:color "green"}}
               {:email "alice@example.com"}]])]
    (xt/submit-tx node tx))

  (xt/q node '(from :user [*]))

  (xt/submit-tx node
    [[:update '{:table :users
                :bind [{:email $user/email}]
                :set {:color "green"}}
      {:user/email "alice@example.com"}]])


  ;; Call this in dev if you'd like to add some seed data to your database. If
  ;; you edit the seed data (in resources/fixtures.edn), you can reset the
  ;; database by running `rm -r storage/xtdb` (DON'T run that in prod),
  ;; restarting your app, and calling add-fixtures again.
  (add-fixtures)

  ;; Query the database
  (let [{:keys [biff/db] :as ctx} (get-context)]
    (q db
       '{:find (pull user [*])
         :where [[user :user/email]]}))

  ;; Update an existing user's email address
  (let [{:keys [biff/db] :as ctx} (get-context)
        user-id (biff/lookup-id db :user/email "hello@example.com")]
    (biff/submit-tx ctx
      [{:db/doc-type :user
        :xt/id user-id
        :db/op :update
        :user/email "new.address@example.com"}]))

  (sort (keys (get-context)))

  ;; Check the terminal for output.
  (biff/submit-job (get-context) :echo {:foo "bar"})
  (deref (biff/submit-job-for-result (get-context) :echo {:foo "bar"})))
