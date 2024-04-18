(ns com.example.app
  (:require [com.biffweb :as biff :refer [q]]
            [com.example.middleware :as mid]
            [com.example.ui :as ui]
            [com.example.settings :as settings]
            [rum.core :as rum]
            [xtdb.api :as xt]
            [ring.adapter.jetty9 :as jetty]
            [cheshire.core :as cheshire]))

(defn set-foo [{:keys [session params] :as ctx}]
  (biff/submit-tx ctx
    [{:db/op :update
      :db/doc-type :user
      :xt/id (:uid session)
      :user/foo (:foo params)}])
  {:status 303
   :headers {"location" "/app"}})

(defn bar-form [{:keys [value]}]
  (biff/form
   {:hx-post "/app/set-bar"
    :hx-swap "outerHTML"}
   [:label.block {:for "bar"} "Bar: "
    [:span.font-mono (pr-str value)]]
   [:.h-1]
   [:.flex
    [:input.w-full#bar {:type "text" :name "bar" :value value}]
    [:.w-3]
    [:button.btn {:type "submit"} "Update"]]
   [:.h-1]
   [:.text-sm.text-gray-600
    "This demonstrates updating a value with HTMX."]))

(defn set-bar [{:keys [session params] :as ctx}]
  (biff/submit-tx ctx
    [{:db/op :update
      :db/doc-type :user
      :xt/id (:uid session)
      :user/bar (:bar params)}])
  (biff/render (bar-form {:value (:bar params)})))

(defn message [{:msg/keys [text sent-at]}]
  [:.mt-3 {:_ "init send newMessage to #message-header"}
   [:.text-gray-600 (biff/format-date sent-at "dd MMM yyyy HH:mm:ss")]
   [:div text]])

(defn notify-clients [{:keys [com.example/chat-clients]} tx]
  (doseq [[op & args] (::xt/tx-ops tx)
          :when (= op ::xt/put)
          :let [[doc] args]
          :when (contains? doc :msg/text)
          :let [html (rum/render-static-markup
                      [:div#messages {:hx-swap-oob "afterbegin"}
                       (message doc)])]
          ws @chat-clients]
    (jetty/send! ws html)))

(defn send-message [{:keys [session] :as ctx} {:keys [text]}]
  (let [{:keys [text]} (cheshire/parse-string text true)]
    (biff/submit-tx ctx
      [{:db/doc-type :msg
        :msg/user (:uid session)
        :msg/text text
        :msg/sent-at :db/now}])))

(defn chat [{:keys [biff/db]}]
  (let [messages (q db
                    '{:find (pull msg [*])
                      :in [t0]
                      :where [[msg :msg/sent-at t]
                              [(<= t0 t)]]}
                    (biff/add-seconds (java.util.Date.) (* -60 10)))]
    [:div {:hx-ext "ws" :ws-connect "/app/chat"}
     [:form.mb-0 {:ws-send true
                  :_ "on submit set value of #message to ''"}
      [:label.block {:for "message"} "Write a message"]
      [:.h-1]
      [:textarea.w-full#message {:name "text"}]
      [:.h-1]
      [:.text-sm.text-gray-600
       "Sign in with an incognito window to have a conversation with yourself."]
      [:.h-2]
      [:div [:button.btn {:type "submit"} "Send message"]]]
     [:.h-6]
     [:div#message-header
      {:_ "on newMessage put 'Messages sent in the past 10 minutes:' into me"}
      (if (empty? messages)
        "No messages yet."
        "Messages sent in the past 10 minutes:")]
     [:div#messages
      (map message (sort-by :msg/sent-at #(compare %2 %1) messages))]]))

(defn app [{:keys [session biff/db] :as ctx}]
  (let [{:user/keys [email foo bar]} (xt/entity db (:uid session))]
    (ui/page
     {}
     [:div "Signed in as " email ". "
      (biff/form
       {:action "/auth/signout"
        :class "inline"}
       [:button.text-blue-500.hover:text-blue-800 {:type "submit"}
        "Sign out"])
      "."]
     [:.h-6]
     (biff/form
      {:action "/app/set-foo"}
      [:label.block {:for "foo"} "Foo: "
       [:span.font-mono (pr-str foo)]]
      [:.h-1]
      [:.flex
       [:input.w-full#foo {:type "text" :name "foo" :value foo}]
       [:.w-3]
       [:button.btn {:type "submit"} "Update"]]
      [:.h-1]
      [:.text-sm.text-gray-600
       "This demonstrates updating a value with a plain old form."])
     [:.h-6]
     (bar-form {:value bar})
     [:.h-6]
     (chat ctx))))

(defn ws-handler [{:keys [com.example/chat-clients] :as ctx}]
  {:status 101
   :headers {"upgrade" "websocket"
             "connection" "upgrade"}
   :ws {:on-connect (fn [ws]
                      (swap! chat-clients conj ws))
        :on-text (fn [ws text-message]
                   (send-message ctx {:ws ws :text text-message}))
        :on-close (fn [ws status-code reason]
                    (swap! chat-clients disj ws))}})

(def about-page
  (ui/page
   {:base/title (str "About " settings/app-name)}
   [:p "This app was made with "
    [:a.link {:href "https://biffweb.com"} "Biff"] "."]))

(defn echo [{:keys [params]}]
  {:status 200
   :headers {"content-type" "application/json"}
   :body params})

(def module
  {:static {"/about/" about-page}
   :routes ["/app" {:middleware [mid/wrap-signed-in]}
            ["" {:get app}]
            ["/set-foo" {:post set-foo}]
            ["/set-bar" {:post set-bar}]
            ["/chat" {:get ws-handler}]]
   :api-routes [["/api/echo" {:post echo}]]
   :on-tx notify-clients})
