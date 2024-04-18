(ns com.example.email
  (:require [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [clj-http.client :as http]
            [com.example.settings :as settings]
            [clojure.tools.logging :as log]
            [rum.core :as rum]))

(defn signin-link [{:keys [to url user-exists]}]
  (let [[subject action] (if user-exists
                           [(str "Sign in to " settings/app-name) "sign in"]
                           [(str "Sign up for " settings/app-name) "sign up"])]
    {:to [{:email to}]
     :subject subject
     :html (rum/render-static-markup
            [:html
             [:body
              [:p "We received a request to " action " to " settings/app-name
               " using this email address. Click this link to " action ":"]
              [:p [:a {:href url :target "_blank"} "Click here to " action "."]]
              [:p "This link will expire in one hour. "
               "If you did not request this link, you can ignore this email."]]])
     :text (str "We received a request to " action " to " settings/app-name
                " using this email address. Click this link to " action ":\n"
                "\n"
                url "\n"
                "\n"
                "This link will expire in one hour. If you did not request this link, "
                "you can ignore this email.")}))

(defn signin-code [{:keys [to code user-exists]}]
  (let [[subject action] (if user-exists
                           [(str "Sign in to " settings/app-name) "sign in"]
                           [(str "Sign up for " settings/app-name) "sign up"])]
    {:to [{:email to}]
     :subject subject
     :html (rum/render-static-markup
            [:html
             [:body
              [:p "We received a request to " action " to " settings/app-name
               " using this email address. Enter the following code to " action ":"]
              [:p {:style {:font-size "2rem"}} code]
              [:p
               "This code will expire in three minutes. "
               "If you did not request this code, you can ignore this email."]]])
     :text (str "We received a request to " action " to " settings/app-name
                " using this email address. Enter the following code to " action ":\n"
                "\n"
                code "\n"
                "\n"
                "This code will expire in three minutes. If you did not request this code, "
                "you can ignore this email.")}))

(defn template [k opts]
  ((case k
     :signin-link signin-link
     :signin-code signin-code)
   opts))

(defn send-mailersend [{:keys [biff/secret mailersend/from mailersend/reply-to]} form-params]
  (let [result (http/post "https://api.mailersend.com/v1/email"
                          {:oauth-token (secret :mailersend/api-key)
                           :content-type :json
                           :throw-exceptions false
                           :as :json
                           :form-params (merge {:from {:email from :name settings/app-name}
                                                :reply_to {:email reply-to :name settings/app-name}}
                                               form-params)})
        success (< (:status result) 400)]
    (when-not success
      (log/error (:body result)))
    success))

(defn send-console [ctx form-params]
  (println "TO:" (:to form-params))
  (println "SUBJECT:" (:subject form-params))
  (println)
  (println (:text form-params))
  (println)
  (println "To send emails instead of printing them to the console, add your"
           "API keys for MailerSend and Recaptcha to config.env.")
  true)

(defn send-email [{:keys [biff/secret recaptcha/site-key] :as ctx} opts]
  (let [form-params (if-some [template-key (:template opts)]
                      (template template-key opts)
                      opts)]
    (if (every? some? [(secret :mailersend/api-key)
                       (secret :recaptcha/secret-key)
                       site-key])
      (send-mailersend ctx form-params)
      (send-console ctx form-params))))
