#!/bin/bash
# shell script to export sqlite3 data into a csv
# https://stackoverflow.com/questions/31175018/run-command-line-sqlite3-query-and-exit
# https://askubuntu.com/questions/682095/create-bash-menu-based-on-file-list-map-files-to-numbers
# https://askubuntu.com/questions/1146166/bash-how-to-create-a-menu-with-submenu

SOURCEDIR="databases/"
DESTDIR="products/"
INNAME="2020_caseload_to_4_14.db"


OUTFORMAT=".mode csv"
HEADERS=".headers on"
SEPARATOR=".separator ,"
OUTPUT=".output $OUTNAME"

BASE=" "

submenu () {
  local PS3='Please enter sub option: '
  local options=("Bulk export" "By vist threshold" "By date range" "Sub menu quit")
  local opt
  select opt in "${options[@]}"
  do
      case $opt in
          "Bulk export")
              echo "you chose Bulk export for $BASE"
			  QUERY="SELECT * FROM Visit_Table;"
			  OUTNAME=bulk-export-$(date +%Y-%m-%d)-$(date +%H-%M-%S).csv
			  sqlite3 $BASE "$HEADERS" "$SEPARATOR" "$OUTFORMAT" ".output $OUTNAME" "$QUERY" ".exit" 
              echo "exported $OUTNAME"
              ;;
          "By vist threshold")
              echo "you chose By vist threshold for $BASE"
			  read -p "enter number " threshold
              QUERY2="SELECT * FROM Visit_Table GROUP BY hh_id HAVING COUNT(distinct visit_date) >= $threshold ORDER BY date(visit_date);"
			  OUTNAME2=threshold-$threshold-export-$(date +%Y-%m-%d)-$(date +%H-%M-%S).csv
              sqlite3 $BASE "$HEADERS" "$SEPARATOR" "$OUTFORMAT" ".output $OUTNAME2" "$QUERY2" ".exit" 
              echo "exported $OUTNAME"
              ;;
		  "By date range" )
              echo "you chose By date range for $BASE"
              read -p "enter start of range YYYY-MM-DD " start_d
              read -p "enter end of range YYYY-MM-DD " end_d
			  OUTNAME3=range-$start_d-$end_d-export-$(date +%Y-%m-%d)-$(date +%H-%M-%S).csv
              QUERY3="SELECT * FROM Visit_Table GROUP BY hh_id HAVING date(visit_date) BETWEEN date('$start_d') AND date('$end_d') ORDER BY date(visit_date);"
              sqlite3 $BASE "$HEADERS" "$SEPARATOR" "$OUTFORMAT" ".output $OUTNAME3" "$QUERY3" ".exit"
              echo "exported $OUTNAME"
			  ;;
          "Sub menu quit")
              return
              ;;
          *) echo "invalid option $REPLY";;
      esac
  done
}

unset options i
while IFS= read -r -d $'\0' f; do
    options[i++]="$f"
done < <(find . -maxdepth 1 -type f -name "*.db" -print0 )

select opt in "${options[@]}" "Stop the script"; do
    case $opt in
        *.db)
        echo "db file $opt selected"
		BASE=$opt
        submenu
		break
        ;;
    "Stop the script")
        echo "You chose to stop"
        break
        ;;
        *)
        echo "This is not a number"
        ;;
    esac
done
