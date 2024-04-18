(ns com.example.middleware
  (:require [com.biffweb :as biff]
            [muuntaja.middleware :as muuntaja]
            [ring.middleware.anti-forgery :as csrf]
            [ring.middleware.defaults :as rd]))

(defn wrap-redirect-signed-in [handler]
  (fn [{:keys [session] :as ctx}]
    (if (some? (:uid session))
      {:status 303
       :headers {"location" "/app"}}
      (handler ctx))))

(defn wrap-signed-in [handler]
  (fn [{:keys [session] :as ctx}]
    (if (some? (:uid session))
      (handler ctx)
      {:status 303
       :headers {"location" "/signin?error=not-signed-in"}})))

;; Stick this function somewhere in your middleware stack below if you want to
;; inspect what things look like before/after certain middleware fns run.
(defn wrap-debug [handler]
  (fn [ctx]
    (let [response (handler ctx)]
      (println "REQUEST")
      (biff/pprint ctx)
      (def ctx* ctx)
      (println "RESPONSE")
      (biff/pprint response)
      (def response* response)
      response)))

(defn wrap-site-defaults [handler]
  (-> handler
      biff/wrap-render-rum
      biff/wrap-anti-forgery-websockets
      csrf/wrap-anti-forgery
      biff/wrap-session
      muuntaja/wrap-params
      muuntaja/wrap-format
      (rd/wrap-defaults (-> rd/site-defaults
                            (assoc-in [:security :anti-forgery] false)
                            (assoc-in [:responses :absolute-redirects] true)
                            (assoc :session false)
                            (assoc :static false)))))

(defn wrap-api-defaults [handler]
  (-> handler
      muuntaja/wrap-params
      muuntaja/wrap-format
      (rd/wrap-defaults rd/api-defaults)))

(defn wrap-base-defaults [handler]
  (-> handler
      biff/wrap-https-scheme
      biff/wrap-resource
      biff/wrap-internal-error
      biff/wrap-ssl
      biff/wrap-log-requests))
