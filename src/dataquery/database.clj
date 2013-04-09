(ns dataquery.database
  (use [datomic.api :only [q db tempid] :as d]))

;; -----------------------------------------------------------------------------
(def db-uri "datomic:mem://votes")
(def conn (delay (d/connect db-uri)))

(def schema-trxn
  [;; ------- States ---------
   {:db/id #db/id[:db.part/db]
    :db/ident :state/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/unique :db.unique/value
    :db/doc "Name of a state"
    :db.install/_attribute :db.part/db}

   ;; ------- Party ----------
   {:db/id #db/id[:db.part/db]
    :db/ident :party/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/unique :db.unique/value
    :db/doc "Political Party"
    :db.install/_attribute :db.part/db}

   ;; ------- Vote ----------
   {:db/id #db/id[:db.part/db]
    :db/ident :year
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/unique :db.unique/value
    :db/doc "In which year was the vote"
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :party
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/unique :db.unique/value
    :db/doc "For which party"
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :state
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/unique :db.unique/value
    :db/doc "From which state"
    :db.install/_attribute :db.part/db}])

(defn load-schema []
  ;; Create the database
  (d/create-database db-uri)
  ;; Store the schema transaction
  (d/transact (d/connect db-uri) schema-trxn))

(defn format-state [s]
  {:db/id      (tempid :db.part/user)
   :state/name s})

(defn load-states [states]
  @(d/transact @conn (map format-state states)))

(def parties
  [{:db/id      (tempid :db.part/user)
    :party/name "R"}
   {:db/id      (tempid :db.part/user)
    :party/name "D"}
   {:db/id      (tempid :db.part/user)
    :party/name "I"}])

;; hardcode
(defn load-parties
  []
  @(d/transact @conn parties))

(declare state-id party-id)

(defn format-vote [[state year party]]
  {:db/id (tempid :db.part/user)
   :year  year
   :party (party-id party)
   :state (state-id state)})

(defn load-votes [votes]
  @(d/transact @conn
               (map format-vote votes)))

;; ----------------
;; I'm doing this wrong.
(defn state-id
  "Find a states datomic id"
  ([u-id] (state-id u-id (db @conn)))
  ([u-id db]
     (ffirst
      (q '[:find ?u
           :in $ ?n
           :where
           [?u :state/name ?n]]
         db
         u-id))))

(defn party-id
  "Find a party datomic id"
  ([u-id] (party-id u-id (db @conn)))
  ([u-id db]
     (ffirst
      (q '[:find ?u
           :in $ ?n
           :where
           [?u :party/name ?n]]
         db
         u-id))))
