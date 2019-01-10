#!/bin/ksh
OPTION=$(whiptail --title "SELECT SCRIPT TYPE" --menu "CHOOSE FROM BELOW SCRIPTS" 15 60 2 \
"1" "CLONING SCRIPT FOR BULK USAGE" \
"2" "CLONING SCRIPT FOR ONE OR FEW TABLES"  3>&1 1>&2 2>&3) 
exitstatus=$?
if [ $exitstatus = 0 ]; then
    
	case "$OPTION" in
	             1)
	             	/home/satheesmohan/MOVE/.Clonebulk.sh
	             	;;
	             2)
	             	/home/satheesmohan/MOVE/.Clone.sh
	             	;;
			     *)
	             	echo "Sorry, I don't understand"
	             	;;
    esac
else
    whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50       
   	exit 0 
fi
