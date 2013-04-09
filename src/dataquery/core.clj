(ns dataquery.core
  (:use [dataquery.ingest :only [ingest]]
        [dataquery.query]))

(comment
  ;; Kick everything off
  (ingest)

  ;; Sample Queries
  (num-votes [(year "2000")])
  (num-votes [(state "Utah")])
  (num-votes [(state "Utah") (>year 1990)])
  (num-votes [(party "D") (state "Texas") (>year 1980)])

  ;; Sample Probabilities
  (p [(party "I")])
  (p [(party "D")] [(state "Texas") (>year 1980)])
  (p [(party "D")] [(>year 2000)]))
