(ns com.example.auth
  (:require [com.biffweb :as biff]
            [com.biffweb.xtdb :as bxt :refer [submit-tx]]
            [clj-http.client :as http]
            [clojure.string :as str]
            [rum.core :as rum])
  (:import [java.time Instant]))

(defn passed-recaptcha? [{:keys [biff/secret biff.recaptcha/threshold params]
                          :or {threshold 0.5}}]
  (or (nil? (secret :recaptcha/secret-key))
      (let [{:keys [success score]}
            (:body
             (http/post "https://www.google.com/recaptcha/api/siteverify"
                        {:form-params {:secret (secret :recaptcha/secret-key)
                                       :response (:g-recaptcha-response params)}
                         :as :json}))]
        (and success (or (nil? score) (<= threshold score))))))

(defn email-valid? [ctx email]
  (and email
       (re-matches #".+@.+\..+" email)
       (not (re-find #"\s" email))))

(defn new-link [{:keys [biff.auth/check-state
                        biff/base-url
                        biff/secret
                        anti-forgery-token]}
                email]
  (str base-url "/auth/verify-link/"
       (biff/jwt-encrypt
        (cond-> {:intent "signin"
                 :email email
                 :exp-in (* 60 60)}
          check-state (assoc :state (biff/sha256 anti-forgery-token)))
        (secret :biff/jwt-secret))))

(defn new-code [length]
  ;; We use (SecureRandom.) instead of (SecureRandom/getInstanceStrong) because
  ;; the latter can block, and on some shared hosts often does. Blocking is
  ;; fine for e.g. generating environment variables in a new project, but we
  ;; don't want to block here.
  ;; https://tersesystems.com/blog/2015/12/17/the-right-way-to-use-securerandom/
  (let [rng (java.security.SecureRandom.)]
    (format (str "%0" length "d")
            (.nextInt rng (dec (int (Math/pow 10 length)))))))

(defn send-link! [{:keys [biff.auth/email-validator
                          biff/node
                          biff.auth/get-user-id
                          biff/send-email
                          params]
                   :as ctx}]
  (let [email (biff/normalize-email (:email params))
        url (new-link ctx email)
        user-id (delay (get-user-id node email))]
    (cond
      (not (passed-recaptcha? ctx))
      {:success false :error "recaptcha"}

      (not (email-validator ctx email))
      {:success false :error "invalid-email"}

      (not (send-email ctx
                       {:template :signin-link
                        :to email
                        :url url
                        :user-exists (some? @user-id)}))
      {:success false :error "send-failed"}

      :else
      {:success true :email email :user-id @user-id})))

(defn verify-link [{:keys [biff.auth/check-state
                           biff/secret
                           path-params
                           params
                           anti-forgery-token]}]
  (let [{:keys [intent email state]} (-> (merge params path-params)
                                         :token
                                         (biff/jwt-decrypt (secret :biff/jwt-secret)))
        valid-state (= state (biff/sha256 anti-forgery-token))
        valid-email (= email (:email params))]
    (cond
      (not= intent "signin")
      {:success false :error "invalid-link"}

      (or (not check-state) valid-state valid-email)
      {:success true :email email}

      (some? (:email params))
      {:success false :error "invalid-email"}

      :else
      {:success false :error "invalid-state"})))

(defn send-code! [{:keys [biff.auth/email-validator
                          biff/send-email
                          biff/node
                          biff.auth/get-user-id
                          params]
                   :as ctx}]
  (let [email (biff/normalize-email (:email params))
        code (new-code 6)
        user-id (delay (get-user-id node email))]
    (cond
      (not (passed-recaptcha? ctx))
      {:success false :error "recaptcha"}

      (not (email-validator ctx email))
      {:success false :error "invalid-email"}

      (not (send-email ctx
                       {:template :signin-code
                        :to email
                        :code code
                        :user-exists (some? @user-id)}))
      {:success false :error "send-failed"}

      :else
      {:success true :email email :code code :user-id @user-id})))

;;; HANDLERS -------------------------------------------------------------------

(defn send-link-handler [{:keys [biff.auth/single-opt-in
                                 biff.auth/new-user-tx
                                 biff/node
                                 params]
                          :as ctx}]
  (let [{:keys [success error email user-id]} (send-link! ctx)]
    (when (and success single-opt-in (not user-id))
      (submit-tx node (new-user-tx ctx email)))
    {:status 303
     :headers {"location" (if success
                            (str "/link-sent?email=" (:email params))
                            (str (:on-error params "/") "?error=" error))}}))

(defn verify-link-handler [{:keys [biff.auth/app-path
                                   biff.auth/invalid-link-path
                                   biff.auth/new-user-tx
                                   biff.auth/get-user-id
                                   biff/node
                                   session
                                   params
                                   path-params]
                            :as ctx}]
  (let [{:keys [success error email]} (verify-link ctx)
        existing-user-id (when success (get-user-id node email))
        token (:token (merge params path-params))]
    (when (and success (not existing-user-id))
      (submit-tx node (new-user-tx ctx email)))
    {:status 303
     :headers {"location" (cond
                            success
                            app-path

                            (= error "invalid-state")
                            (str "/verify-link?token=" token)

                            (= error "invalid-email")
                            (str "/verify-link?error=incorrect-email&token=" token)

                            :else
                            invalid-link-path)}
     :session (cond-> session
                success (assoc :uid (or existing-user-id (get-user-id node email))))}))

(defn send-code-handler [{:keys [biff.auth/single-opt-in
                                 biff.auth/new-user-tx
                                 biff/node
                                 params]
                          :as ctx}]
  (let [{:keys [success error email code user-id]} (send-code! ctx)]
    (when success
      (submit-tx node
        (into [[:biff/upsert :biff.auth/code {:email email}
                {:set {:code code
                       :created-at (Instant/now)
                       :failed-attempts 0}}]]
              (when (and single-opt-in (not user-id))
                (new-user-tx ctx email)))))
    {:status 303
     :headers {"location" (if success
                            (str "/verify-code?email=" (:email params))
                            (str (:on-error params "/") "?error=" error))}}))

(defn verify-code-handler [{:keys [biff.auth/app-path
                                   biff.auth/new-user-tx
                                   biff.auth/get-user-id
                                   biff/node
                                   params
                                   session]
                            :as ctx}]
  (let [email (biff/normalize-email (:email params))
        code (bxt/lookup node :biff.auth/code :email email)
        success (and (passed-recaptcha? ctx)
                     (some? code)
                     (< (:failed-attempts code) 3)
                     (not (biff/elapsed? (:created-at code) :now 3 :minutes))
                     (= (:code params) (:code code)))
        existing-user-id (when success (get-user-id node email))
        tx (cond
             success
             (into [[:biff/delete :user (:xt/id code)]]
                   (when-not existing-user-id
                     (new-user-tx ctx email)))

             (and (not success)
                  (some? code)
                  (< (:failed-attempts code) 3))
             [[:update '{:table :biff.auth/code
                         :bind [{:xt/id $id} failed-attempts]
                         :set {:failed-attempts (+ failed-attempts 1)}}
               {:id (:xt/id code)}]])]
    (submit-tx node tx)
    (if success
      {:status 303
       :headers {"location" app-path}
       :session (assoc session :uid (or existing-user-id (get-user-id node email)))}
      {:status 303
       :headers {"location" (str "/verify-code?error=invalid-code&email=" email)}})))

(defn signout [{:keys [session]}]
  {:status 303
   :headers {"location" "/"}
   :session (dissoc session :uid)})

;;; ----------------------------------------------------------------------------

(defn new-user-tx [ctx email]
  [[:biff/upsert :user {:email email} {:defaults {:joined-at (Instant/now)}}]])

(defn get-user-id [node email]
  (bxt/lookup-id node :user :email email))

(def default-options
  #:biff.auth{:app-path "/app"
              :invalid-link-path "/signin?error=invalid-link"
              :check-state true
              :new-user-tx new-user-tx
              :get-user-id get-user-id
              :single-opt-in false
              :email-validator email-valid?})

(defn wrap-options [handler options]
  (fn [ctx]
    (handler (merge options ctx))))

(defn module [options]
  {:schema {::short-string [:string {:min 1 :max 100}]
            :biff.auth/code [:map {:closed true}
                             [:xt/id           :uuid]
                             [:email           ::short-string]
                             [:code            ::short-string]
                             [:created-at      :time/instant]
                             [:failed-attempts integer?]]}
   :routes [["/auth" {:middleware [[wrap-options (merge default-options options)]]}
             ["/send-link"          {:post send-link-handler}]
             ["/verify-link/:token" {:get verify-link-handler}]
             ["/verify-link"        {:post verify-link-handler}]
             ["/send-code"          {:post send-code-handler}]
             ["/verify-code"        {:post verify-code-handler}]
             ["/signout"            {:post signout}]]]})


;; No one should be depending on this var since this namespace isn't part of the
;; public API, but it doesn't hurt to add an alias anyway...
(def plugin module)

;;; FRONTEND HELPERS -----------------------------------------------------------

(def recaptcha-disclosure
  [:div {:style {:font-size "0.75rem"
                 :line-height "1rem"
                 :color "#4b5563"}}
   "This site is protected by reCAPTCHA and the Google "
   [:a {:href "https://policies.google.com/privacy"
        :target "_blank"
        :style {:text-decoration "underline"}}
    "Privacy Policy"] " and "
   [:a {:href "https://policies.google.com/terms"
        :target "_blank"
        :style {:text-decoration "underline"}}
    "Terms of Service"] " apply."])

(defn recaptcha-callback [fn-name form-id]
  [:script
   (biff/unsafe
    (str "function " fn-name "(token) { "
         "document.getElementById('" form-id "').submit();"
         "}"))])
