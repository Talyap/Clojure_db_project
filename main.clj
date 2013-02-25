;this main program is a Clojure course project, implementing a Database with partial DDL + CRUD SQL query support
;Copyright (C) 2013  Ori Naor, Talya Parish Plas & Shay Busidan

;This program is free software; you can redistribute it and/or
;modify it under the terms of the GNU General Public License
;as published by the Free Software Foundation; either version 2
;of the License, or (at your option) any later version.

;This program is distributed in the hope that it will be useful,
;but WITHOUT ANY WARRANTY; without even the implied warranty of
;MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;GNU General Public License for more details.

;You should have received a copy of the GNU General Public License
;along with this program; if not, write to the Free Software
;Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

(ns main
    (:require [clojure.string :as str])
    (:require [clojure.core :as core])
    (:require [clojure.set :as set])
  )

;;;;;;;;;;;;;DATA BASE;;;;;;;;;;;;;

(def DB (ref {}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update_DB [func table change]
  "update the database with assoc or dissoc, according to function received"
    (dosync (alter DB func table change) )
)

(defn print_db [message]
  "print current db state"
    (println (str "-Info- " message))
    (doseq [iter (deref DB)] 
        (println (first iter))
        (println (deref (first (rest iter))))
    )    
)

(defn str2com [allowed_com command]
    {:pre [(contains? allowed_com command)]}
  "receive a command (string) and allowed commands, and return the currect command if legal"
    (resolve (symbol (if (= command "<>") "not=" command)))
)

(defn SQL_PARSER [data]
  "receive data, parse into values, recognize query type, check if legal, and call query for further evaluation"
    (let [command (first (str/split data #"\s"))
          values (rest (str/split (str/replace data #"[()]" "") #"[,\s]"))
          ]
        (println (str "--" command "--"))
        ((str2com #{"CREATE_TABLE" "DROP_TABLE" "INSERT_INTO" "SELECT" "DELETE" "UPDATE"} command) values)     
    )
)

(defn indexes [attr col]
  "receive an attribute and a sequence (schema for the entry), calculate the position of the attribute in sequence"
    (for [ [a b] 
        (#(map vector (iterate inc 0) %) col) 
        :when (attr b)] a 
    )
)

(defn change-if-needed [str]
  "reveive string input, and return it in the currect form: string or number"
    (if (number? (read-string str))
        (read-string str)
        str 
    )
)

(defn WHERE [table_name condition]
    {:pre [ (>= (count (str/split condition #"\s")) 3) ]}
  "receive a table name and conditions for the table, calculate the conditions, which may be separeted by ANDs or ORs, return concurrent entries"
   (if-let [and_or (if (contains? (into #{}(str/split condition #"\s"))"AND") #" AND "
                   (if (contains? (into #{}(str/split condition #"\s"))"OR") #" OR " nil))]
         (let [f (first(str/split condition and_or)) 
               r (str/join and_or (rest (str/split condition and_or)))] 
             (if (= (str and_or) " AND ") 
                  (remove nil? (set/intersection (into #{} (WHERE table_name f)) (into #{}(WHERE table_name r))))
                  (remove nil? (set/union (into #{}(WHERE table_name f)) (into #{}(WHERE table_name r))))
               )
           )
         (let [[a b c] (str/split condition #"\s")
             table (DB table_name)
             index (first (indexes #{a} (into [] (deref (DB (str table_name "_cap"))))))]
             (assert (>= index 0))        
             (map #(if ((str2com #{"=" "<" ">" "<=" ">=" "<>"} b) (change-if-needed (nth % index)) (change-if-needed c)) %)  (into [] (deref table)))
           )
     )
)

(defn SELECT [values]
  "SELECT query manager: receive query, partition by sections, determine table and entries concurrent with the conditions, and print them"
    (let [columns (flatten (first (core/split-with #(core/not= % "FROM") values)))
          table_name (first (rest (flatten (first (core/split-with #(core/not= % "WHERE") (flatten (rest (core/split-with #(core/not= % "FROM") values)))))))) 
          condition (flatten (rest (flatten (rest (core/split-with #(core/not= % "WHERE") values)))))
          answer (remove nil? (WHERE table_name (str/join " " condition)))
          pos (map #(first (indexes #{%} (into [] (deref (DB (str table_name "_cap")))))) columns )
           ]
        (println "Select Result:" (map (fn [x] (map (fn [y] (nth x y)) (into [] pos))) (into [] answer)))
    )
) 

(defn DELETE [values]
  "DELETE query manager: receive query, partition by sections, determine table and entries concurrent with the conditions, delete them from database"
    (let [table_name (second values)
          condition (rest (flatten (rest (core/split-with #(core/not= % "WHERE") values))))
          answer (into #{} (remove nil? (WHERE table_name (str/join " " condition))))
         ]
        (update_DB assoc table_name (ref (set/difference (deref (DB table_name)) answer)))
     )
)
 
(defn update_line [line attr col]
  "receive an entry of a table, attributes that should be changes, and new values, update the line recursively"
    (lazy-seq
         (if (> (count attr) 0) 
             (update_line (assoc (into [] line) (first attr) (second (first col))) (rest attr) (rest col))
             line  
         )
    )
)

(defn UPDATE [values]
  "UPDATE query manager: receive query, partition by sections, determine table and entries concurrent with the conditions, update attributes with new values"
    (let [table_name (first values)
          [set conditions]  (core/split-with #(core/not= % "WHERE") (flatten (rest (core/split-with #(core/not= % "SET") values))))
          answer (into #{} (remove nil? (WHERE table_name (str/join " " (rest conditions)))))
          columns (map #(str/split % #"\=") (rest set))
          atr (map #(first(indexes #{(first %)} (into [] (deref (DB (str table_name "_cap")))))) columns )
           ]
        (DELETE (concat (list "FROM" table_name) conditions))
        (dosync (alter (DB table_name) set/union (deref (DB table_name)) (into #{} (map (fn [line] (update_line line atr columns)) (into [] answer)))))
    )
)

(defn INSERT_INTO [values]
  "INSERT INTO query manager: receive query, partition by sections, determine table, check if attributes new values are concurrent with table schema, insert new entries"
  {:pre [(= (count (for [[a b] (map vector (rest values) (deref (DB (str (first values) "_cap"))))
                         :let [type1 (number? (change-if-needed a ))
                               type2 (=
                                       "number"
                                       ((deref (DB (str (first values) "_map"))) b)
                                       )]
                         :when (core/not= type1 type2)]
                     true))
            0)

         ]
   }
    (let [table_name (DB (first values))]
        (dosync (alter table_name conj (rest values)))
    )
)

(defn CREATE_TABLE [values]
    {:pre [(not (contains? (deref DB) (first values)))]}
  "CREATE TABLE query manager: receive query, partition by sections, determine table, crate an empty new table with table_name and attributes, add to Database"
    (let [table_name (first values) 
          captions (keep-indexed #(if (even? %1) %2) (rest values))     
          data_map (apply hash-map (rest values))
         ]
        (update_DB assoc table_name (ref #{}))
        (update_DB assoc (str table_name "_cap") (ref captions))
        (update_DB assoc (str table_name "_map") (ref data_map))
    )
)

(defn DROP_TABLE [table_name]
  "DROP TABLE query manager: receive query, determine table, remove table from database if exists"
    (let [table (first table_name)]
        (update_DB dissoc (deref DB) table)
        (update_DB dissoc (deref DB) (str table "_cap"))
        (update_DB dissoc (deref DB) (str table "_map"))
    )
)


; -------------------------   execution:   --------------------------


;------------------  READ file and execute each line (command)  --------------:
(println "starting to read data from file:")
  (require '[clojure.java.io :as io])
  (with-open [rdr (io/reader "src\\input.data")]
    (doseq [line (line-seq rdr)]
    (SQL_PARSER line)))
  
(print_db "summary:")