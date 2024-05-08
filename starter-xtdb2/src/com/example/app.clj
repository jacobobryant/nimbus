(ns com.example.app
  (:require [com.biffweb :as biff]
            [com.biffweb.xtdb :as bxt]
            [com.example.middleware :as mid]
            [com.example.ui :as ui]
            [com.example.settings :as settings]
            [rum.core :as rum]
            [xtdb.api :as xt]
            [ring.adapter.jetty9 :as jetty]
            [cheshire.core :as cheshire])
  (:import [java.util Date]
           [java.time Instant]))

(defn set-foo [{:keys [biff/node session params]}]
  (bxt/submit-tx node
    [[:biff/update :user {:set {:foo (:foo params)}
                          :where {:xt/id (:uid session)}}]])
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

(defn set-bar [{:keys [biff/node session params]}]
  (bxt/submit-tx node
    [[:biff/update :user {:set {:bar (:bar params)}
                          :where {:xt/id (:uid session)}}]])
  (biff/render (bar-form {:value (:bar params)})))

(defn ui-message [{:keys [text sent-at]}]
  [:.mt-3 {:_ "init send newMessage to #message-header"}
   [:.text-gray-600 (biff/format-date (cond-> sent-at
                                        (instance? java.time.ZonedDateTime sent-at) (.toInstant)
                                        true (Date/from))
                                      "dd MMM yyyy HH:mm:ss")]
   [:div text]])

(defn send-message [{:keys [biff/node com.example/chat-clients session]} {:keys [text]}]
  (let [{:keys [text]} (cheshire/parse-string text true)
        message {:xt/id (random-uuid)
                 :user (:uid session)
                 :text text
                 :sent-at (Instant/now)}
        html (rum/render-static-markup
              [:div#messages {:hx-swap-oob "afterbegin"}
               (ui-message message)])]
    (bxt/submit-tx node [[:put-docs :message message]])
    (doseq [ws @chat-clients]
      (jetty/send! ws html))))

(defn chat [{:keys [biff/node]}]
  (let [messages (xt/q node
                       '(-> (from :message [* sent-at])
                            (where (< $t0 sent-at)))
                       {:args {:t0 (.minusSeconds (Instant/now) (* 60 10))}})]
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
      (map ui-message (sort-by :msg/sent-at #(compare %2 %1) messages))]]))

(defn app [{:keys [session biff/node] :as ctx}]
  (let [{:keys [email foo bar]} (bxt/lookup node :user :xt/id (:uid session))]
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
                   (biff/catchall-verbose
                    (send-message ctx {:ws ws :text text-message})))
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
   #_#_:on-tx notify-clients})
