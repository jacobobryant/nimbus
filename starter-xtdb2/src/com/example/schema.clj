(ns com.example.schema)

(def schema
  {::short-string [:string {:min 1 :max 100}]
   ::long-string  [:string {:min 1 :max 5000}]

   :user [:map {:closed true}
          [:xt/id     :uuid]
          [:email     [:and ::short-string [:re #".+@.+"]]]
          [:joined-at :time/instant]
          [:foo {:optional true} ::short-string]
          [:bar {:optional true} ::short-string]]

   :message [:map {:closed true}
             [:xt/id   :uuid]
             [:user    :uuid]
             [:text    ::long-string]
             [:sent-at :time/instant]]})

(def module
  {:schema schema})
