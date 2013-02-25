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


(ns tests
  (:use [clojure.test] [main])
)
(defn clean-DB []
  (dosync (alter DB (ref {})))
)
(deftest check_insert_delete 
        (clean-DB)
        (SQL_PARSER "CREATE_TABLE grades (name string,last string,hw_grade number)")
        (SQL_PARSER "INSERT_INTO grades (shlomo shalom 93)")
        (SQL_PARSER "INSERT_INTO grades (Yonatan baruh 86)")
        (SQL_PARSER "DROP_TABLE grades")
        (is (=(DB "grades") nil ))  
  )

(deftest create_table
   (clean-DB)
   (SQL_PARSER "CREATE_TABLE grades (name string,last string,hw_grade number)")
   (is (not= nil (DB "grades"))) 
)
(deftest add_two_tables
   (clean-DB)
   (SQL_PARSER "CREATE_TABLE grade (name string,last string,hw_grade number)")
   (SQL_PARSER "CREATE_TABLE grades (name string,last string,hw_grade number)")
   (SQL_PARSER "DROP_TABLE grades")
   (is (count (deref DB)) 1)   
   (SQL_PARSER "DROP_TABLE grades")
   (is (count (deref DB)) 1)  
 )
(deftest insert
  (clean-DB)
  (SQL_PARSER "CREATE_TABLE emails (name string,last string,email string)")
  (SQL_PARSER "INSERT_INTO emails (shlomo shalom shs@gmnail.com)")
  (SQL_PARSER "INSERT_INTO emails (abcf efg shs@gmnail.com)")
  (SQL_PARSER "INSERT_INTO emails (shlomo slomzion shs@gmnail.com)")
  (SQL_PARSER "INSERT_INTO emails (abc efg shs@gmnail.com)")
  (is (=(count(deref (DB "emails")))4))
  (SQL_PARSER "INSERT_INTO emails (shlomo shalom shs@gmnail.com)")
  (is (=(count(deref (DB "emails")))4))

  )
; UPDATE table_name SET column1=value,column2=value,... WHERE condition

;DELETE FROM table_name WHERE condition
(deftest where
    (clean-DB)
    (SQL_PARSER "CREATE_TABLE email (name string,last string,email string)")
    (SQL_PARSER "INSERT_INTO email (shlomo shalom shs@gmail.com)")
    (SQL_PARSER "INSERT_INTO email (abcf efg shs@gmail.com)")
    (SQL_PARSER "INSERT_INTO email (one two three@gmail.com)")
    (SQL_PARSER "DELETE FROM email WHERE email <> three@gmail.com")
    (is (=(count(deref (DB "email")))1))
    (SQL_PARSER "DELETE FROM email WHERE email = three@gmail.com")
    (is (=(count(deref (DB "email")))0))
    (SQL_PARSER "INSERT_INTO email (shlomo shalom shs@gmail.com)")
    (SQL_PARSER "DELETE FROM email WHERE email = three@gmail.com")
    (SQL_PARSER "UPDATE email SET name=Shlomo,email=a@b.com WHERE name = shlomo")
    (SQL_PARSER "DELETE FROM email WHERE name = shlomo")
    (is (=(count(deref (DB "email")))1))
    (SQL_PARSER "DELETE FROM email WHERE name = Shlomo")
    (is (=(count(deref (DB "email")))0))
)
(deftest where2
    (clean-DB)
    (SQL_PARSER "CREATE_TABLE email (name string,last string,email string)")
    (SQL_PARSER "INSERT_INTO email (avi banini abd@walla.co.il)")
    (SQL_PARSER "INSERT_INTO email (avi eliahu abd@walla.co.il)")
    (SQL_PARSER "INSERT_INTO email (batia eliahu abd@walla.co.il)")
    (SQL_PARSER "INSERT_INTO email (avi gelman abd@gmail.co.il)")
    (is (=(count(deref (DB "email")))4))
    (SQL_PARSER "DELETE FROM email WHERE name = avi AND last = banini")
    (is (=(count(deref (DB "email")))3))
    (SQL_PARSER "DELETE FROM email WHERE last = eliahu OR last = gelman")
    (is (=(count(deref (DB "email")))0))
    (SQL_PARSER "INSERT_INTO email (avi banini abd2@gmail.co.il)")
    (SQL_PARSER "INSERT_INTO email (avi eliahu abd@walla.co.il)")
    (SQL_PARSER "DELETE FROM email WHERE last = eliahu AND name = avi AND email = abd@walla.co.il")
    (is (=(count(deref (DB "email")))1))
)

    

(deftest check_comperators
  (clean-DB)
  (SQL_PARSER "CREATE_TABLE grades (name string,last string,hw_grade number)")
  (SQL_PARSER "INSERT_INTO grades (shlomo shalom 93)")
  (SQL_PARSER "INSERT_INTO grades (niv palpal 100)")
  (SQL_PARSER "INSERT_INTO grades (Yonatan baruh 86)")
  (is (=(count(deref (DB "grades")))3))
  (SQL_PARSER "DELETE FROM grades WHERE hw_grade < 90")
  (is (=(count(deref (DB "grades")))2))
  (SQL_PARSER "INSERT_INTO grades (Yonatan baruh 86)")
  (SQL_PARSER "DELETE FROM grades WHERE hw_grade >= 93")
  (is (=(count(deref (DB "grades")))1))

)

;-----------  RUNNING TESTS  ------------;
(clean-DB)
(run-tests)