(ns dataquery.ingest
  "Read the csv into the db"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dataquery.database :as db]))

(def votes-fn "./resources/votes.csv")

(defn read-file
  "Read the file into a data structure"
  [fn]
  (with-open [in-file (io/reader fn)]
    (doall
     (csv/read-csv in-file))))

(defn clean-string
  [s]
  (-> s
      (str/replace " " "")
      (str/replace #"\\\d" "")))

(defn import-data-block
  "Read a .csv block and clean it up. Return as vec-of-vecs"
  [fn]
  (->> fn read-file (map (partial map clean-string))))

(defn data-map
  "Turn a row from a block into a map keyed by header"
  [header row]
  (apply hash-map (interleave header row)))

(defn expand-state-vote
  "Split the R-10 etc"
  [state year count-string]
  (let [[party _] (str/split count-string #"-")]
    [state year party]))

(defn expand-votes
  "Return a set of votes matching the aggregated data in vote-map"
  [years {s "State" :as vote-map}]
  (map (fn [y] ((partial expand-state-vote s y) (vote-map y))) years))

(defn ingest []
  (let [data-block (import-data-block votes-fn)
        header     (first data-block)
        rows       (rest data-block)
        data-map   (map (partial data-map header) rows)
        years      (drop 4 header)
        states     (map first rows)
        vote-facts (mapcat (partial expand-votes years) data-map)]
    (db/load-schema)
    (db/load-states states)
    (db/load-parties)
    (db/load-votes vote-facts)))
