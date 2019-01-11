#!/bin/ksh
####################CORE-PART###################
vertica(){
check_and_prepare(){        
##INITIALIZING SOURCE_OBJECT_STRING
         export SOURCE_OBJECT_STRING=""
         
             con_qry(){
                   ##REMOVING OLD CONFIG-QUERY FILES FOR PREPARING NEW CONFIG-QUERY FILES    
					   rm -rf /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/CONFIG_QUERIES"_"$tablename.txt || true
                       cat /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2 | sed "s/^/'/" | sed "s/$/'/g" > test1.txt;mv test1.txt /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2
                   ##CALCULATING CONFIG-QUERIES COUNT 
					   export config_query_created=0
                       export TOTAL_IDS=`cat /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2 | wc -l`
                       #echo "$TOTAL_IDS Ids found"
                       export loopcount=`expr $TOTAL_IDS / $ID_COUNT_2`
                       export remainingIDS=`expr $TOTAL_IDS % $ID_COUNT_2`
                          
                           if [ $remainingIDS -ne 0 ]; then
						        export loopcount=`expr $loopcount + 1`
                                #echo "We are creating $loopcount config queries plz wait"
                           fi
                       
				      Creation () {
                                 sed -n -e "1,${ID_COUNT_2}p" /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2 > process.txt
                                 sed -e "1,${ID_COUNT_2}d" /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2 > test1.txt; mv test1.txt /home/muthupan/$SUBJECT_AREA/$tablename"~"$ID_COUNT_2
                                 cat process.txt | tr '\n' ',' | sed 's/.$//' | sed "s/^/$SOURCE_OBJECT_STRING where id in(/" | sed "s/$/);/g" >> /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/CONFIG_QUERIES"_"$tablename.txt
                                 export config_query_created=`expr $config_query_created + 1`
                                 #echo "$config_query_created query created successfully"
                              } 
                             
					 if [ $TOTAL_IDS -ne 0 ]; then	   
				     	   
				     	 printf "$(date '+%Y-%m-%d %H:%M:%S') | -----> Table Name : $tablename # Total IDs : $TOTAL_IDS # Total_Config_Queries_Created : $loopcount\n" >> /home/muthupan/CONFIG_QUERIES/logfile.txt 					  
						 export success_table_count=`expr $success_table_count + 1`
                         ##CONFIG-QUERIES CREATION BLOCK          
				     			  {
                                   for w in `seq 1 $loopcount`
                                   do
                         		     Creation
                                     y=`expr $w - 3`
                                     echo $y
                                   done
                                   } | whiptail --gauge "Please wait while Creating  $loopcount config Queries for $tablename table" 6 90 0
                            
                              sed -i -e "s/;/\n/g" /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/CONFIG_QUERIES"_"$tablename.txt
                              rm process.txt
                    else
                      echo $(date '+%Y-%m-%d %H:%M:%S') " TABLENAME ->  $tablename   Reason ->  $tablename~$ID_COUNT_2 FILE HAVING NO IDs.PLEASE PASTE IDs IN $tablename~$ID_COUNT_2 ." >> /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/exception.txt 
					fi					
                       
                }
         
         
         check_it(){
                          
             if ! [ "$ID_COUNT_2" -eq "$ID_COUNT_2" ] 2> ./null
             then
                 #echo "You have typed an non-numeric value!!.Please enter numeric value."
                 rm ./null
                 echo $(date '+%Y-%m-%d %H:%M:%S') " TABLENAME ->  $tablename   Reason ->  Non-numeric ID VALUE ($ID_COUNT_2)" >> /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/exception.txt
                 echo $tablename >> /home/muthupan/CONFIG_QUERIES/failed_tables.txt
			 else
                 rm ./null
                 if [ "$ID_COUNT_2" -ne 0 ]; then
                   con_qry  
                 else
				   echo $(date '+%Y-%m-%d %H:%M:%S') " TABLENAME ->  $tablename   Reason ->  Check File name (ID VALUE = 0) #ALTER FILE NAME  CURRENT NAME = $exactname_file_name" >> /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/exception.txt
                   echo $tablename >> /home/muthupan/CONFIG_QUERIES/failed_tables.txt
				 fi
             fi
             
               }
                  ##TABLE NAME FROM DB and \0 AT THE END
                  export y=` /opt/vertica/bin/vsql -h mc4t00873.itcs.softwaregrp.net -p 5433 -d air -C -U srvc_hpswomt_dev -w 'Vertica.air2' -t -c "select OBJECT_NAME from swt_rpt_stg.SOURCE_OBJECT_STRING where OBJECT_NAME='$tablename'"`
                  export TABLE_NAME=`echo $y|sed "s/$//g"`
          
                   if [ "$TABLE_NAME" = "$tablename" ]; then
                       ##SOURCE_OBJECT_STRING FROM DB and \0 AT THE END
			          export y=` /opt/vertica/bin/vsql -h mc4t00873.itcs.softwaregrp.net -p 5433 -d air -C -U srvc_hpswomt_dev -w 'Vertica.air2' -t -c "select OBJECT_STRING from swt_rpt_stg.SOURCE_OBJECT_STRING where OBJECT_NAME='$tablename'"`
                      export SOURCE_OBJECT_STRING=`echo $y|sed "s/$//g"`
                      break
                   fi
         
         
         
                  if [ ${#SOURCE_OBJECT_STRING} = 0 ]; then
                  #printf  "SOURCE_OBJECT_STRING  not found please verify the tablename\n"
                      echo $(date '+%Y-%m-%d %H:%M:%S') " TABLENAME ->  $tablename   Reason ->  SOURCE_OBJECT_STRING NOT FOUND!!!PLZ UPDATE DATABASE" >> /home/muthupan/CONFIG_QUERIES/$SUBJECT_AREA/exception.txt
				      echo $tablename >> /home/muthupan/CONFIG_QUERIES/failed_tables.txt
                  else  
                      check_it 
                  fi
         
         
         

}

FIRST_METHOD(){
            ########for all subject area#############
            ALL_AREAS(){
                     			
				DO_IT_FOR_ALL(){
				    for z in `seq 1 $TOTAL_FILES`
                    do
                      export exactname_file_name=`sed -n '1p' /home/muthupan/LIST.txt`
					  export tablename=`sed -n '1p' /home/muthupan/LIST.txt | cut -d'~' -f1` 
                      export ID_COUNT_2=`sed -n '1p' /home/muthupan/LIST.txt | cut -d'~' -f2`
                      sed -i '1d' /home/muthupan/LIST.txt
                      check_and_prepare		   
	                done
					
					rm -rf /home/muthupan/ALL_SUBJECT_AREAS
		            export FAILED_COUNT=`expr $TOTAL_FILES - $success_table_count`
					whiptail --title "VERTICA-FAST_1.0" --msgbox " SUBJECT_AREA:$SUBJECT_AREA\nTOTAL_TABLES:$TOTAL_FILES\n***SUCCESS TABLE COUNT : $success_table_count\n***FAILED TABLE COUNT : $FAILED_COUNT" 10 60
                    }
			    
                mkdir /home/muthupan/ALL_SUBJECT_AREAS
                cp -r /home/muthupan/APTTUS/* /home/muthupan/ALL_SUBJECT_AREAS
                cp -r /home/muthupan/NETSUITE/* /home/muthupan/ALL_SUBJECT_AREAS
                cp -r /home/muthupan/SFDC/* /home/muthupan/ALL_SUBJECT_AREAS
                cd /home/muthupan/ALL_SUBJECT_AREAS
                ls -ltr | rev | cut -d' ' -f1 | rev | sed '1d' > /home/muthupan/LIST.txt
                export TOTAL_FILES=` cat /home/muthupan/LIST.txt | wc -l`
                whiptail --title "VERTICA-FAST_1.0" --msgbox "SUBJECT AREA: SFDC,NETSUITE and APTTUS\nTOTAL FILES:$TOTAL_FILES" 10 50 
				printf "$(date '+%Y-%m-%d %H:%M:%S') ::  SUBJECT AREA CHOOSED = $SUBJECT_AREA  TOTAL FILES = $TOTAL_FILES\n" >> /home/muthupan/CONFIG_QUERIES/logfile.txt
                     if [ $TOTAL_FILES = 0 ]; then
                              if (whiptail --title "VERTICA-FAST_1.0" --yesno "********NO FILES FOUND********\nWould you like to try again for other subject areas.?" 10 60) then
                                export success_table_count=0
                				FIRST_METHOD
                              else
                                 #echo "Thanks for using the script."
                                 whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 60
                              fi
                     else
                         DO_IT_FOR_ALL
                     fi	 
            }
            #########################################
DO_IT(){
         for z in `seq 1 $TOTAL_FILES`
         do
           export tablename=`sed -n '1p' /home/muthupan/LIST.txt | cut -d'~' -f1` 
           export ID_COUNT_2=`sed -n '1p' /home/muthupan/LIST.txt | cut -d'~' -f2`
           sed -i '1d' /home/muthupan/LIST.txt
           check_and_prepare		   
	     done
		 if (whiptail --title "VERTICA-FAST_1.0" --yesno "SUBJECT_AREA:$SUBJECT_AREA\nTOTAL_TABLES:$TOTAL_FILES\n***CONFIG-QUERIES CREATED FOR $success_table_count TABLES***\nWould you like to continue..?" 20 60) then
            FIRST_METHOD
         else
            #echo "Thanks for using the script."
            whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 60
         fi
}
GET_LIST(){
cd /home/muthupan/$SUBJECT_AREA
ls -ltr | rev | cut -d' ' -f1 | rev | sed '1d' > /home/muthupan/LIST.txt
export TOTAL_FILES=` cat /home/muthupan/LIST.txt | wc -l`
printf "$(date '+%Y-%m-%d %H:%M:%S') ::  SUBJECT AREA CHOOSED = $SUBJECT_AREA  TOTAL FILES = $TOTAL_FILES" >> /home/muthupan/CONFIG_QUERIES/logfile.txt
whiptail --title "VERTICA-FAST_1.0" --msgbox "SUBJECT AREA:$SUBJECT_AREA\nTOTAL FILES:$TOTAL_FILES" 10 50 

     if [ $TOTAL_FILES = 0 ]; then
              if (whiptail --title "VERTICA-FAST_1.0" --yesno "********NO FILES FOUND FOR $SUBJECT_AREA SUBJECT AREA********\nWould you like to try again for other subject areas.?" 10 60) then
                export success_table_count=0
				FIRST_METHOD
              else
                 #echo "Thanks for using the script."
                 whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 60
              fi
     else
         DO_IT
     fi	 
}
OPTION=$(whiptail --title "SELECT SUBJECT AREA" --menu "Choose your option" 15 60 4 \
"1" "APTTUS" \
"2" "NETSUITE" \
"3" "SFDC" \
"4" "ALL"  3>&1 1>&2 2>&3) 
exitstatus=$?
if [ $exitstatus = 0 ]; then
    
	case "$OPTION" in
	             1)
	             	export SUBJECT_AREA="APTTUS"
					GET_LIST
	             	;;
	             2)
	             	export SUBJECT_AREA="NETSUITE"
					GET_LIST
	             	;;
	             3)
	             	export SUBJECT_AREA="SFDC"
					GET_LIST
	             	;;
				 4)
	             	export SUBJECT_AREA="ALL_SUBJECT_AREAS"
					ALL_AREAS
					;;
			     *)
	             	echo "Sorry, I don't understand"
	             	;;
    esac
else
    whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50       
   	exit 0 
fi
}



##VERIFYING USERNAME #STEP-3
verify_password(){
##PASSWORD FROM USER         
		 if [ $limit -le 3 ]; then
                 password=$(whiptail --title "VERTICA-FAST_1.0" --passwordbox "PASSWORD:" 10 50 3>&1 1>&2 2>&3)
                 exitstatus=$?
                 if [ $exitstatus = 0 ]; then
                     
		  	          if [ "$password" = "$PSWD" ]; then
		                    printf "\n$(date '+%Y-%m-%d %H:%M:%S') :: USER_NAME : $USER" >> /home/muthupan/CONFIG_QUERIES/logfile.txt
                            printf "\n==========================================================\n " >> /home/muthupan/CONFIG_QUERIES/logfile.txt 							
##PASSWORD VERIFICATION SUCCESS CALLING FIRST_METHOD METHOD
                              	FIRST_METHOD
		                    exit 0
		  	          else
##GIVING 3 CHANCE TO RE-ENTER PASSWORD				
							if [ $limit -ne 3 ]; then
                                   if (whiptail --title "VERTICA-FAST_1.0" --yes-button "Re-Enter"  --no-button "Cancel" --yesno "\nINCORRECT PASSWORD!!!\nRe-enter the Password:" 10 30) then
                                     export limit=`expr $limit + 1`
					    	         verify_password
                                   else
                                     whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50
		  	          	             exit 0
                                   fi
		                    else 
							     whiptail --title "VERTICA-FAST_1.0" --msgbox "SORRY!!!\nAll Attempts are Failed.!\nPlease Try again later" 10 50
		                         exit 0	 
					        fi
					  fi		
		  	      	
	             else
                   exit 0
                 fi  
        
		 fi			 

}

##VERIFYING USERNAME #STEP-2
Verify_user(){

export result=1


##USERNAME FROM DB  AND REMOVING \0 AT THE END
   export x=` /opt/vertica/bin/vsql -h mc4t00873.itcs.softwaregrp.net -p 5433 -d air -C -U srvc_hpswomt_dev -w 'Vertica.air2' -t -c "select name from swt_rpt_stg.credential where name='$username'"`
   export USER=`echo $x|sed "s/$//g"`
   if [ "$username" = "$USER" ]; then
##PASSWORD FROM DB AND REMOVING \0 AT THE END
         export x=`/opt/vertica/bin/vsql -h mc4t00873.itcs.softwaregrp.net -p 5433 -d air -C -U srvc_hpswomt_dev -w 'Vertica.air2' -t -c "select pwd_one from swt_rpt_stg.credential where name='$username'"`    
		 export PSWD=`echo $x|sed "s/$//g"`
		 export result=0
         export limit=1		 
         verify_password
   fi

   if [ $result = 1 ]; then       
	      if (whiptail --title "VERTICA-FAST_1.0" --yesno "\nINCORRECT USER NAME!!!\nWould you like to continue..?" 10 50) then	  
           ./vertica.sh       
         else       
           whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50       
   	       exit 0       
	      fi		
   fi       
          

		  
}


##GETTING USER NAME##STEP-1
export success_table_count=0
username=$(whiptail --title "VERTICA-FAST_1.0" --inputbox "USER NAME:" 10 50  3>&1 1>&2 2>&3)
exitstatus=$?
if [ $exitstatus = 0 ]; then
   Verify_user	 
else
    exit 0
fi

}

#############################################################################################################################################################

#backup new files
#clear old files
rm -rf /home/muthupan/SFDC/*
rm -rf /home/muthupan/NETSUITE/*
rm -rf /home/muthupan/APTTUS/*

mkdir /home/muthupan/TEMP_NEW
cp -r /home/muthupan/NEW/* /home/muthupan/TEMP_NEW

export default_IDCOUNT=250
cd /home/muthupan/NEW
ls -ltr | rev | cut -d' ' -f1 | rev | sed '1d' > /home/muthupan/LIST.txt
export Repeatedfilescount=0
export freshfilescount=0
export REPEATED_FILE_WITH_SAME_IDS_count=0
export TOTAL_FILES=` cat /home/muthupan/LIST.txt | wc -l`

find_subject_area(){

                   if [ $SA = "sf" ];then
				     export SUBJECTAREA="SFDC";
				   elif [ $SA = "ns" ];then
				     export SUBJECTAREA="NETSUITE";
				   elif [ $SA = "at" ];then
				     export SUBJECTAREA="APTTUS";
				   else
				     print "invalid subject area for $filename"
				   fi

}

find_and_place_new_IDS(){
                   #finding subject area of file
				   			   
				   find_subject_area
				   
				   #finding newIDS ( if the length of grepresult is zero  then that ID is not present in OLD file)
				   #(if the length of grepresult is equal to the length of newfileID then that ID is present in OLD file)
				   export is_REPEATED_FILE_WITH_SAME_IDS="TRUE"
				   
				   while read newfileID 
                   do
                   export grepresult=`grep -w $newfileID /home/muthupan/NEW/$filename`
				   if [ `echo ${#grepresult}` = 0 ]; then
				      
                      echo $newfileID >> /home/muthupan/$SUBJECTAREA/$altered_filename"~"$default_IDCOUNT
                      export is_REPEATED_FILE_WITH_SAME_IDS="FALSE"
				   fi
                   done < /home/muthupan/NEW/$filename
				   
				   if [ $is_REPEATED_FILE_WITH_SAME_IDS = "TRUE" ];then
			           export REPEATED_FILE_WITH_SAME_IDS_count=`expr $REPEATED_FILE_WITH_SAME_IDS_count + 1`
                       echo $filename >> /home/muthupan/REPEATED_FILE_WITH_SAME_IDS.txt
                   fi				   
            
}

while read filename 
do
   
   export SA=`echo $filename | cut -d'_' -f3`
   export altered_filename=` echo $filename | sed 's/true_fallouts_//g' | sed 's/_field_fallouts.txt//g' | sed 's/bkp_prev_//g' | sed 's/^...//g'`
   
   if [ -e /home/muthupan/OLD/$filename ]
   then
       export Repeatedfilescount=`expr $Repeatedfilescount + 1`
	   echo $filename >> /home/muthupan/REPEATEDFILES.txt
	   #Removing column names all other column values except ID Field
	   cat $filename | cut -d'~' -f1 | sed -e "1,1d" > temp.txt
	   mv temp.txt /home/muthupan/NEW/$filename
	   
	   find_and_place_new_IDS
   else
       export freshfilescount=`expr $freshfilescount + 1`
	   
	   #gettinfg SUBJECTAREA
	   find_subject_area
	   
	   #Removing column names all other column values except ID Field
	   cat $filename | cut -d'~' -f1 | sed -e "1,1d" > temp.txt
	   mv temp.txt /home/muthupan/NEW/$filename
       #COPY THE FRESH FILE TO CORRESPONDING FOLDER BASED ON SUBJECTAREA
	   cp /home/muthupan/NEW/$filename /home/muthupan/$SUBJECTAREA/$altered_filename"~"$default_IDCOUNT
	   echo $filename >> /home/muthupan/FRESHFILES.txt
   fi

done < /home/muthupan/LIST.txt

#moving backup files to NEW folder and delete backup folder
rm -rf /home/muthupan/NEW/*
mv  /home/muthupan/TEMP_NEW/* /home/muthupan/NEW 
rmdir /home/muthupan/TEMP_NEW


whiptail --title "VERTICA-FAST_1.0" --msgbox "TOTAL FILES:$TOTAL_FILES\nFRESH FILES:$freshfilescount\nREPEATED FILES:$Repeatedfilescount\nREPEATED_FILE_WITH_SAME_IDS : $REPEATED_FILE_WITH_SAME_IDS_count" 10 50 

vertica