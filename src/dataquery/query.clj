(ns dataquery.query
  "Query the database"
  (:use [datomic.api :only [q db] :as d]
        [clojure.set :only [union]])
  (:require [dataquery.database :as db]))

;; -------------------------------------------------------------------
;;  Base query

(def vote-join
  '[[vote-join ?v ?y ?p ?s]
    [?v :year ?y]
    [?v :state ?s-id]
    [?v :party ?p-id]
    [?s-id :state/name ?s]
    [?p-id :party/name ?p]])

(def base-query
  {:find   ['?v]
   :in     ['$ '%]
   :where  ['(vote-join ?v ?y ?p ?s)]})

(declare constrain)
(defn votes
  "Return all the votes from the db."
  ([query]
     (votes query []))
  ([query constraints]
     (q (constrain query constraints)
        (db @db/conn)
        [vote-join])))

;; --------------------------------------------------------------------
;;  Constraints
;;  Constraints must be a vector of individual constraints
(defn year [y]
  [['?v :year y]])

(defn >year* [y1 y2] (> (Integer. y1) (Integer. y2)))
(defn >year [y]
  [`[(dataquery.query/>year* ~(symbol '?y) ~y)]])

(defn state [s]
  [['?v :state '?s-id]
   ['?s-id :state/name s]])

(defn party [p]
  [['?v :party '?p-id]
   ['?p-id :party/name p]])

(defn constrain
  "Apply constraints to a query"
  [query constraints]
  (update-in query [:where] (partial apply concat) constraints))

;; --------------------------------------------------------------------
;;  Probability layer

;; This could be parameterised by query, but there's no need
(def count-query
  (assoc base-query :find ['(count ?v)]))
(def num-votes (comp #(or (ffirst %) 0) (partial votes count-query)))

(defn- div0 [n d]
  (if (or (zero? n) (zero? d))
    0
    (/ n d)))

(defn p
  "Returns the probability of event of p(J|C), with a the joint
events J and conditional events C (these events are described as constraints)."
  ([J]   (p J []))
  ([J C]
     (div0 (num-votes (concat J C))
           (num-votes C))))
