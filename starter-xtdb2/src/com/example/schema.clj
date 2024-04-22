(ns com.example.schema
  (:require [malli.core :as malc]
            [malli.registry :as malr]
            [malli.experimental.time :as malt]))

(def schema
  {:user/id :uuid
   :user [:map {:closed true}
          [:xt/id                     :user/id]
          [:user/email                :string]
          [:user/joined-at            inst?]
          [:user/foo {:optional true} :string]
          [:user/bar {:optional true} :string]]

   :msg/id :uuid
   :msg [:map {:closed true}
         [:xt/id       :msg/id]
         [:msg/user    :user/id]
         [:msg/text    :string]
         [:msg/sent-at inst?]]})

(def new-schema
  {::string [:string {:min 1 :max 100}]

   :user [:map {:closed true}
          [:xt/id :uuid]
          [:email [:and ::string [:re #".+@.+"]]]
          [:joined-at {::default #(java.time.Instant/now)} :time/instant]
          [:color {:optional true} ::string]]})

(malr/set-default-registry! (malr/composite-registry
                             (malc/default-schemas)
                             (malt/schemas)
                             new-schema))

(def module
  {:schema schema})
