#!/bin/ksh
#
#    Version              1.0
#    Author               Daniel Muthupandi
#    Short Describtion    Copy(Truncating and copying) or clone(creating,Truncating and copying) 
#                         a table from one environment to another.
####################################### DESCRIPTION ############################################################
#The main purpose of this script is cloning the table structure and copying data from one environment to another. 
#This script will read the source tablenames from /home/satheesmohan/MOVE/INPUT/tablenames.txt and using 
#/home/satheesmohan/MOVE/INPUT/EnvironmentDetails.txt file to get the DB Environment Details.User can select the 
#source and destination environment,user account type (READ/ALL/ADMIN) and destination schema from the menu 
#during the script execution.The script will generate four output files including log file.
#The two input files are,
#========================
#1./home/satheesmohan/MOVE/INPUT/tablenames.txt
#2./home/satheesmohan/MOVE/INPUT/EnvironmentDetails.txt
#The four output files are,
#==========================
#1./home/satheesmohan/MOVE/OUTPUT/log.txt
#2./home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt
#3./home/satheesmohan/MOVE/OUTPUT/Missingcolumns.txt
#4./home/satheesmohan/MOVE/OUTPUT/SuccessTables.txt  
################################################################################################################


CORE_METHOD(){

 ####         BULK(){
 ####              while read tablename 
 ####              do
 ####                export truncate_status=`echo "Truncate table swt_rpt_base.$tablename" | /opt/vertica/bin/vsql -E -e -a --echo-all -h mc4t01630.itcs.softwaregrp.net -p 5433 -d AIR_DEV2 -C -U srvc_hpsw_dev2_all -w 'Her3tage$hpe' | grep -w 'TRUNCATE TABLE' `
 ####                if [ ${#truncate_status} != 0 ]; then
 ####                  echo "CONNECT TO VERTICA  air_PRO USER srvc_hpsw_pro_read PASSWORD 'Micro17Focus' ON 'mc4t01045.itcs.softwaregrp.net',5433;copy swt_rpt_base.$tablename from vertica air_PRO.swt_rpt_base.$tablename direct;" | /opt/vertica/bin/vsql -E -e -a --echo-all -h mc4t01630.itcs.softwaregrp.net -p 5433 -d AIR_DEV2 -C -U srvc_hpsw_dev2_all -w 'Her3tage$hpe' > tmp.txt  ; cat  tmp.txt >> /home/satheesmohan/MOVE/OUTPUT/log.txt ;
 ####                  export COPY_status=`cat tmp.txt | grep -w 'Rows Loaded'`
 ####              	  if [ ${#COPY_status} = 0 ]; then
 ####                         print "TABLE NAME : $tablename  REASON : CONNECT (OR) COPY  ERROR" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt         
 ####                    fi		  
 ####                else
 ####                  print "TABLE NAME : $tablename  REASON : TRUNCATION ERROR" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt 
 ####                fi	
 ####                 
 ####              done < /home/satheesmohan/MOVE/INPUT/tablenames.txt
 ####         }

     DO_COPY(){
	                
					PREPARE_COPY_STATEMENT(){
					           cat /home/satheesmohan/MOVE/OUTPUT/common_columns.txt |  tr '\n' ','|sed "s/^/(/g" | sed 's/.$//' | sed "s/$/)/g" | sed "s/^/copy $DESTINATION_SCHEMA.$table/"  > /home/satheesmohan/MOVE/OUTPUT/temp2.txt
							   cat /home/satheesmohan/MOVE/OUTPUT/common_columns.txt |  tr '\n' ','|sed "s/^/(/g" | sed 's/.$//' | sed "s/$/)/g" |sed "s/^/ from vertica $SOURCE_db.$TABLENAME/"  | sed "s/$/ direct;/"  >> /home/satheesmohan/MOVE/OUTPUT/temp2.txt
							   					
					}
	                export truncate_status=`echo "Truncate table $destination_tablename" | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd | grep -w 'TRUNCATE TABLE'`
                    if [ ${#truncate_status} != 0 ]; then
					##copy staement preparation
					  PREPARE_COPY_STATEMENT
                     ###executing copy staement
           			  echo "CONNECT TO VERTICA  $SOURCE_db USER $SOURCE_usermode PASSWORD '$SOURCE_pswd' ON '$SOURCE_loadbalancer',5433;`cat /home/satheesmohan/MOVE/OUTPUT/temp2.txt`" | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd > tmp.txt  ; cat  tmp.txt >> /home/satheesmohan/MOVE/OUTPUT/log.txt ;
                      export COPY_status=`cat tmp.txt | grep -w 'Rows Loaded'`
                      if [ ${#COPY_status} = 0 ]; then
                           print "[ERROR] => TABLE NAME : $table  REASON : CONNECT (OR) COPY  ERROR" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt  
						   print "TABLE NAME : $table  REASON : CONNECT (OR) COPY  ERROR" >> /home/satheesmohan/MOVE/OUTPUT/log.txt  
                           export RejectedTable_Count=`expr $RejectedTable_Count + 1`						   
                      else
                           print "[COPY - SUCCESS]TABLE NAME : $table " >> /home/satheesmohan/MOVE/OUTPUT/SuccessTables.txt
						   print "[COPY - SUCCESS]TABLE NAME : $table \n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt  
						   export SuccessTable_Count=`expr $SuccessTable_Count + 1`
						   
						   ##showing current table count after copy staement 
						      export e=`/opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;" | sed -n 2p`
		                      if [ ${#e} != 0 ];then
                                 /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                         print "COPY STATEMENT EXECUTED SUCCESSFULLY (DB : $DESTINATION_db TABLENAME : $destination_tablename)\n||==>[TABLE DATA COUNT AFTER COPY - $e]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								 
								 ###GRANT_PERMISSION to read and all users
								 if [ $is_CLONING = "TRUE" ];then
								        print "GRANTING PERMISSION\n===================" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								        export count=0
								        while read line
								        do
								          export count=`expr $count + 1`
								          export DB=`echo $line | cut -d'~' -f2`
								          export USER=`echo $line | cut -d'~' -f3`
								          
								          if [ $DESTINATION_db = $DB ]; then
										     
								             if [ $count = 1 ];then
								  	                  
								  	     			export e=`echo "GRANT  SELECT  ON $destination_tablename TO $USER;" | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd`
                                                       if [ ${#e} != 0 ];then
                                        
								                           print "USER : $USER[READ ONLY] : $USER => GRANT PERMISSION SUCCESS" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               else
                                                           print "USER : $USER[READ ONLY] : $USER => GRANT PERMISSION FAILED" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               fi
								  	      
								  	          elif [ $count = 2 ];then
								  	                  
								  	     			export e=`echo "GRANT  ALL PRIVILEGES  ON $destination_tablename TO $USER;" | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd`
                                                       if [ ${#e} != 0 ];then
                                        
								                           print "USER : $USER[ALL PERMISSION] : $USER => GRANT PERMISSION SUCCESS" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               else
                                                           print "USER : $USER[ALL PERMISSION] : $USER => GRANT PERMISSION FAILED" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               fi
								  	         else
											        export e=`echo "GRANT  ALL PRIVILEGES  ON $destination_tablename TO $USER;" | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd`
                                                       if [ ${#e} != 0 ];then
                                        
								                           print "USER : $USER[ALL PERMISSION] : $USER => GRANT PERMISSION SUCCESS" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               else
                                                           print "USER : $USER[ALL PERMISSION] : $USER => GRANT PERMISSION FAILED" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                                               fi
											  fi
								          fi 
								        
								        done < /home/satheesmohan/MOVE/INPUT/Environment_Details.txt
								 fi
								 
								 print "\n########################################################################################################################################################################################################################################################################\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
				              else
							     /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								 print "COPY STATEMENT EXECUTED SUCCESSFULLY (DB : $DESTINATION_db TABLENAME : $destination_tablename)\n||==>[TABLE DATA COUNT AFTER COPY - UNABLE TO SHOW THE COUNT]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								 print "\n########################################################################################################################################################################################################################################################################\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
							  fi
							  
                      fi					  
                    else
                      print "[ERROR] => TABLE NAME : $table  REASON : TRUNCATION ERROR" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt 
					  print "TABLE NAME : $table  REASON : TRUNCATION ERROR" >> /home/satheesmohan/MOVE/OUTPUT/log.txt 
					  export RejectedTable_Count=`expr $RejectedTable_Count + 1`						   
                    fi	

	           
	 }
    
     DO_CLONE(){
	 
	    
	                				 
					 CLONE_TABLE_CREATION(){

                              #echo $DESTINATION_loadbalancer
							  #echo $DESTINATION_db
							  #echo $DESTINATION_usermode
							  #echo $DESTINATION_pswd
							  
							  export e=`cat DDL_Statement.txt | /opt/vertica/bin/vsql -E -e -a --echo-all -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -C -U $DESTINATION_usermode -w $DESTINATION_pswd | grep -w 'CREATE TABLE'`
                              if [ ${#e} != 0 ];then
                                 
								 print "  | |-->[SUCCESS]-->TABLE CREATED SUCCESSFULLY" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                         if [ "$SOURCE_SCHEMA" = "$DESTINATION_SCHEMA" ]; then
								    print "  |\n  |->STEP-2 : COPY STATEMENT IN PROGRESS..." >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								 else
								    print "  |\n  |->STEP-3 : COPY STATEMENT IN PROGRESS..." >> /home/satheesmohan/MOVE/OUTPUT/log.txt
								 fi
								 
								 export is_CLONING="TRUE"
								 CHECK_COLUMN_DETAILS
							  else
                                 print "  | |-->[FAILED]-->TABLE CREATION FAILED" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                         print "TABLE NAME : $table  REASON : CLONING DECLARED (DESTINATION TABLE CREATION FAILED)" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt  
				                 print "  |\n |->TABLE NAME : $table  REASON : CLONING DECLARED (DESTINATION TABLE CREATION FAILED)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt  
                                 export RejectedTable_Count=`expr $RejectedTable_Count + 1`						   
                              fi	
				
                      
					  }					 
		
		
		
		
		
		#Getting source DDL statement
	    #just printing the vsql in log file
		print "  GETTING SOURCE DDL STATEMENT\n  ============================" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		print "  /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c \"select export_objects('','$TABLENAME');\"\n  |" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		
		/opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c "select export_objects('','$TABLENAME');" | sed '/^$/d' | sed '1,2d' > DDL_Statement.txt
	    export line_no_of_first_occurance=`cat -n DDL_Statement.txt | sed -n '/);/p' | head -1 | grep -Eo '[0-9]{1,4}' | sed -n -e "1,1p"`
	    sed -i -n -e "1,${line_no_of_first_occurance}p"  DDL_Statement.txt
	    export SOURCE_SCHEMA=`echo $TABLENAME | cut -d'.' -f1`
	    
	    if [ "$SOURCE_SCHEMA" = "$DESTINATION_SCHEMA" ]; then
	     print "SOURCE AND DESTINATION SCHEMA IS SAME " >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     #cat DDL_Statement.txt | /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd 
	     print "  |\n  |->STEP-1 : TABLE CREATION IN PROGRESS..." >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     CLONE_TABLE_CREATION
		 
	     
	    elif [ "$SOURCE_SCHEMA" = "swt_rpt_stg" ] && [ "$DESTINATION_SCHEMA" = "swt_rpt_base" ];then
   	     
	     print "SOURCE AND DESTINATION SCHEMA IS DIFFERENT " >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     ##ALTER Current stage DDL staement to base DDL staement and then execute DDL staement
	     #cat DDL_Statement.txt | sed 's/swt_rpt_stg./swt_rpt_base./g' | head -n-3 | sed '$ s/.$/ );/'
	     print "  |\n  |->STEP-1 : ALTER DDL STATEMENT (Stage to Base)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     cat DDL_Statement.txt | sed 's/swt_rpt_stg./swt_rpt_base./g' | sed '/auto_id /d;/STG_LD_DT /d' | head -n-1  > temp3.txt;
	     echo "SWT_INS_DT timestamp );" >> temp3.txt;mv temp3.txt DDL_Statement.txt;
	     #cat DDL_Statement.txt | /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd    
	     print "  |\n  |->STEP-2 : TABLE CREATION IN PROGRESS..." >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     CLONE_TABLE_CREATION
		 
		elif [ "$SOURCE_SCHEMA" = "swt_rpt_base" ] && [ "$DESTINATION_SCHEMA" = "swt_rpt_stg" ];then
	     
		 ##ALTER Current base DDL staement to stage DDL staement and then execute DDL staement
		 print "  |\n  |->source_schema = base  & destination_schema = stg" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     print "  |\n  |->STEP-1 : ALTER DDL STATEMENT (Base to Stage)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
	     cat DDL_Statement.txt | sed 's/swt_rpt_base./swt_rpt_stg./g' | sed '/SWT_INS_DT /d' | head -n-1  > temp3.txt;
	     echo "auto_id  IDENTITY ," >> temp3.txt;echo "STG_LD_DT timestamp DEFAULT \"sysdate\"()" >> temp3.txt;
	     echo ");" >> temp3.txt;mv temp3.txt DDL_Statement.txt;
	     #cat DDL_Statement.txt | /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd    
		 print "  |\n  |->STEP-2 : TABLE CREATION IN PROGRESS..." >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		 CLONE_TABLE_CREATION
		 	     
	    else
	     echo "NOTHING"
	    fi
	 
	 }

	CHECK_COLUMN_DETAILS(){
		 
		       COLUMN_NAME_VERIFICATION(){
		                    
							rm -rf /home/satheesmohan/MOVE/OUTPUT/common_columns.txt | true 
							print "[TABLE NAME] : $table"\n >> /home/satheesmohan/MOVE/OUTPUT/missing_columns.txt
							
                            export MISSING_COLUMN_COUNT=0
							while read line 
                            do
                               export grepresult=`grep -w $line /home/satheesmohan/MOVE/OUTPUT/destination_table_columns.txt`
                     		   if [ `echo ${#grepresult}` = 0 ]; then
                     		          export MISSING_COLUMN_COUNT=`expr $MISSING_COLUMN_COUNT + 1`
                                      echo $table"."$line >> /home/satheesmohan/MOVE/OUTPUT/temp1.txt
                               else
							          echo $line >> /home/satheesmohan/MOVE/OUTPUT/common_columns.txt
							   fi
       					   
                            done < /home/satheesmohan/MOVE/OUTPUT/source_table_columns.txt
							
							

                            if [ $MISSING_COLUMN_COUNT	= 0	];then
                            	print "    |->STEP-2[SUCCESS] COULMN NAME VERIFICATION DONE SUCCESSFULLY" >> /home/satheesmohan/MOVE/OUTPUT/log.txt	
                                DO_COPY
                            else
							    cat /home/satheesmohan/MOVE/OUTPUT/temp1.txt >> /home/satheesmohan/MOVE/OUTPUT/missing_columns.txt ; rm /home/satheesmohan/MOVE/OUTPUT/temp1.txt;
    							print "    |->STEP-2[FAILED] COULMN NAME VERIFICATION FAILED # TOTAL MISSING COLUMNS = $MISSING_COLUMN_COUNT\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		                        DO_COPY
						    fi
		 
		         }
		 
		 
		 ##getting source and destination column details and removing empty line
		 print "    |->QUERY TO GET DESTINATION TABLE COLUMNS:\n    |------------------------------------------\n    /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c \"SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME='$table' AND TABLE_SCHEMA='$DESTINATION_SCHEMA';\" \n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		 /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME='$table' AND TABLE_SCHEMA='$DESTINATION_SCHEMA';" | sed 1d | sed '/^$/d' > /home/satheesmohan/MOVE/OUTPUT/destination_table_columns.txt
		 print "    |->QUERY TO GET SOURCE TABLE COLUMNS:\n    |------------------------------------------\n    /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c \"SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME='$table' AND TABLE_SCHEMA='$source_schema';\"\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		 /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c "SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_NAME='$table' AND TABLE_SCHEMA='$source_schema';" | sed 1d | sed '/^$/d' > /home/satheesmohan/MOVE/OUTPUT/source_table_columns.txt
		 export source_table_column_count=`cat /home/satheesmohan/MOVE/OUTPUT/source_table_columns.txt | wc -l`
		 export destination_table_column_count=`cat /home/satheesmohan/MOVE/OUTPUT/destination_table_columns.txt | wc -l`

		 ####check whether both table having same number of columns	
	
		 if [ $source_table_column_count = $destination_table_column_count ];then
		   print "    |->STEP-1[COLUMN COUNT VERIFICATION SUCCESS] BOTH SOURCE AND DESTINATION TABLES HAVING SAME NUMBER OF COLUMNS (count : $destination_table_column_count)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		   COLUMN_NAME_VERIFICATION
		 else
		   print "    |->STEP-1[COLUMN COUNT VERIFICATION FAILED] BOTH SOURCE AND DESTINATION TABLES HAVING DIFFERENT NUMBER OF COLUMNS (Destination table count : $destination_table_column_count # Source table count : $source_table_column_count)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
           COLUMN_NAME_VERIFICATION		 
		 fi
		 
		
		} 

   COPY_ONLY(){
   
          ##checking the presence of destination table [TO COPY THE TABLE THE MENTIONED TABLE SHOULD BE THERE IN THE DESTINATION]
		  
           export destination_tablename=`echo $TABLENAME | cut -d'.' -f2`
           export destination_tablename=$DESTINATION_SCHEMA"."$destination_tablename
           print "=======================================================\nTABLE $destination_tablename COPY OPERATION INITIATED\n=======================================================" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		  
   		   export e=`/opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;" | sed -n 2p`
		   
   		        if [ ${#e} != 0 ];then
                   print "  |\n  |->DESTINATION TABLE EXISTANCE INFO: $destination_tablename FOUND IN $DESTINATION_env Environment (DB : $DESTINATION_db) [TABLE DATA COUNT BEFORE COPY - $e]" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		           /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		           CHECK_COLUMN_DETAILS
		        else
                   print "  |\n  |->DESTINATION TABLE EXISTANCE INFO: $destination_tablename NOT FOUND IN $DESTINATION_env Environment (DB : $DESTINATION_db) [Table Rejected]" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		           print "TABLE NAME : $table  REASON : COPY DECLARED (TABLE NOT EXIST)" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt  
				   print " |\n |->TABLE NAME : $table  REASON : COPY DECLARED (TABLE NOT EXIST)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt  
                   export RejectedTable_Count=`expr $RejectedTable_Count + 1`						   
                fi	
   
  }
   
  CLONE(){
           
		   ##checking the presence of destination table [TO CLONE THE TABLE THE MENTIONED TABLE SHOULD NOT BE EXIST IN THE DESTINATION]
		   
		   print "\n|*************************|\n|CLONING PROCESS INITIATED|\n|*************************|" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
           export destination_tablename=`echo $TABLENAME | cut -d'.' -f2`
           export destination_tablename=$DESTINATION_SCHEMA"."$destination_tablename
          
   		   export e=`/opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;" | sed -n 2p`
		   
   		        if [ ${#e} != 0 ];then
                   /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd -t -c "select count(*) from $destination_tablename;"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
                   print "  |\n  |->DESTINATION TABLE EXISTANCE INFO: $destination_tablename ALREADY EXIST IN $DESTINATION_env Environment (DB : $DESTINATION_db) \n [TABLE DATA COUNT is $e]\n=>=>=>=>  CLONING DECLARED  <=<=<=<=\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		           print "TABLE NAME : $table  REASON : CLONING DECLARED (ALREADY TABLE EXIST)" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt  
				   print "  |\n  |->TABLE NAME : $table  REASON : CLONING DECLARED (ALREADY TABLE EXIST)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt  
				   print "\n########################################################################################################################################################################################################################################################################\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
                   export RejectedTable_Count=`expr $RejectedTable_Count + 1`						   
		        else
                   print "  |\n  |->DESTINATION TABLE EXISTANCE INFO: $destination_tablename NOT FOUND IN $DESTINATION_env Environment (DB : $DESTINATION_db)\n\n  <=>=>=>=>  CLONING APPROVED  <=<=<=<=>\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
		           DO_CLONE
				fi	
   
  }  
   
  
   READ_TABLENAME_AND_VERIFY(){  
     
               VERIFY_TABLE_PRESENCE(){
                    
					export table=`echo $TABLENAME | cut -d'.' -f2`
            		export source_schema=`echo $TABLENAME | cut -d'.' -f1`  
                    export e=`/opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c "select count(*) from $TABLENAME;" | sed -n 2p`
            		
					if [ ${#e} != 0 ];then
                       print "\nSOURCE TABLE EXISTANCE INFO: $TABLENAME FOUND IN $SOURCE_env Environment (DB : $SOURCE_db) TABLE DATA COUNT - $e" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
            		   /opt/vertica/bin/vsql -E -e -a --echo-all -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd -t -c "select count(*) from $TABLENAME;"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
            		   if [ $selection = 1 ];then
					       CLONE
					   else
					       COPY_ONLY
					   fi
                    else
                       print "\nSOURCE TABLE EXISTANCE INFO: $TABLENAME NOT FOUND IN $SOURCE_env Environment (DB : $SOURCE_db)" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
            		fi		   
               }
            
            
#               c=`find "/home/satheesmohan/MOVE/INPUT/tablenames.txt" -type f -exec echo Found file {} \;`
#               if [ `echo ${#c}` != 0 ];then 
                 
#            	 if [ `cat /home/satheesmohan/MOVE/INPUT/tablenames.txt | wc -l` != 0 ];then
            	    while read line
                    do
                     export TABLENAME=$line
              	     VERIFY_TABLE_PRESENCE
                    done < /home/satheesmohan/MOVE/INPUT/tablenames.txt       
#                 else
#              	    whiptail --title "VERTICA-FAST_1.0" --msgbox " ### INPUT FILE IS EMPTY ###\nPLEASE CHECK /home/satheesmohan/MOVE/INPUT/tablenames.txt FILE" 10 50
#                 fi
            	 
#               else
#                 whiptail --title "VERTICA-FAST_1.0" --msgbox " INPUT TABLE LIST IS NOT FOUND\nPLEASE PLACE /home/satheesmohan/MOVE/INPUT/tablenames.txt FILE" 10 50
#               fi
			   
			   
	     }		   
   
  PERFORM_OPERATION(){
                   
				   OPTION=$(whiptail --title "SELECT OPERATION" --menu "Two Operations available :" 15 60 4 \
                   "1" "CLONE (Source table cloned in destination)" \
                   "2" "COPY ONLY (Source data copied into destination table)"  3>&1 1>&2 2>&3) 
                   exitstatus=$?
                   if [ $exitstatus = 0 ]; then
                       
                   	case "$OPTION" in
                   	             1)
                   	             	export selection=1
									READ_TABLENAME_AND_VERIFY
                   	             	;;
                   	             2)
                   	             	export selection=2
									READ_TABLENAME_AND_VERIFY
                   	             	;;
                   	             *)
                   	             	echo "Sorry, I don't understand"
                   	             	;;
                       esac
                   else
                       whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 8 40       
                       exit 0 
                   fi
   
   
   }
   
  PERFORM_OPERATION 

}

########################################################################################################################### MAIN-START #########################################

MAIN_METHOD(){

    
    CHECK_CONNECTION_STATUS(){
    
    #checking source connection
	/opt/vertica/bin/vsql -l -h $SOURCE_loadbalancer -p 5433 -d $SOURCE_db -U $SOURCE_usermode -w $SOURCE_pswd >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    
    if [ $? = 0 ];then
       print "\n|-->SOURCE CONNECTION - SUCCESS [ DB : $SOURCE_db/USER_MODE : $SOURCE_usermode ]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
       #checking destination connection
       /opt/vertica/bin/vsql -l -h $DESTINATION_loadbalancer -p 5433 -d $DESTINATION_db -U $DESTINATION_usermode -w $DESTINATION_pswd >> /home/satheesmohan/MOVE/OUTPUT/log.txt
       
       if [ $? = 0 ];then
              print "\n|-->DESTINATION CONNECTION - SUCCESS[ DB : $DESTINATION_db/USER_MODE : $DESTINATION_usermode ]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    		  CORE_METHOD
       else	  
              print "\n|-->DESTINATION CONNECTION - FAILURE[ DB : $DESTINATION_db/USER_MODE : $DESTINATION_usermode ]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
              if (whiptail --title "VERTICA-FAST_1.0" --yes-button "Try Again" --no-button "Exit" --yesno "*******UNABLE TO ESTABLISH DESTINATION CONNECTION*******\nWould you like to try again.?" 10 60) then
                MAIN_METHOD
              else
                whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50
              fi
       fi		  
    else
       print "\n|-->SOURCE CONNECTION - FAILURE [ DB : $SOURCE_db/USER_MODE : $SOURCE_usermode ]\n" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
       if (whiptail --title "VERTICA-FAST_1.0" --yes-button "Try Again" --no-button "Exit" --yesno "*******UNABLE TO ESTABLISH SOURCE CONNECTION*******\nWould you like to try again.?" 10 60) then
         MAIN_METHOD
       else
         whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 10 50
       fi
    fi
      
    
     }
    
    
	GET_USERMODE(){
    
                        
						GET_CREDENTIALS(){
    					          
    							  while read line 
                                  do
                                      export env=`echo $line | cut -d'~' -f1`
                                      export usermode=`echo $line | cut -d'~' -f3`
    								  
                                      if [ "$env" = "$ENV" ] && [ "$usermode" = "$USERMODE" ]; then
                                         
										 ####DONT ALTER THE BELOW STATEMENTs (You may get "invalid user or password " error)
                                         export $SOURCE_OR_DESTINTION"_"db=`echo $line | cut -d'~' -f2`
										 export $SOURCE_OR_DESTINTION"_"pswd="`echo $line | cut -d'~' -f4`"
                                         export $SOURCE_OR_DESTINTION"_"loadbalancer=`echo $line | cut -d'~' -f5`
    									 export $SOURCE_OR_DESTINTION"_"env=$ENV
    									 export $SOURCE_OR_DESTINTION"_"usermode=$USERMODE
    									 
    									 ##getting the Destination schema
    									 if [ ${SOURCE_OR_DESTINTION} = "DESTINATION" ] ; then
    									     OPTION=$(whiptail --title "SELECT [$SOURCE_OR_DESTINTION] SCHEMA" --menu "In which layer of destination DB you want to perform operation.?" 15 60 2 \
                                             "1" "swt_rpt_base" \
                                             "2" "swt_rpt_stg"  3>&1 1>&2 2>&3) 
                                             exitstatus=$?
                                             if [ $exitstatus = 0 ]; then
                                                 
                                             	case "$OPTION" in
                                             	             1)
                                             	             	export DESTINATION_SCHEMA=swt_rpt_base
                                             					;;
                                             	             2)
                                             	             	export DESTINATION_SCHEMA=swt_rpt_stg
                                             					;;
                                             	             *)
                                             	             	echo "Sorry, I don't understand"
                                             	             	;;
                                                 esac
                                             else
                                                 whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 8 40       
                                                 exit 0 
                                             fi
    									   
    									 fi
    								  fi
    					          done < /home/satheesmohan/MOVE/INPUT/Environment_Details.txt
    							 					  
    							  
    					}
                        
    					export user_mode_count=1
    					while read line 
                        do
                          export env=`echo $line | cut -d'~' -f1`
                          export usermode=`echo $line | cut -d'~' -f3`
                          if [ $env = $ENV ]; then
                            export usermode_$user_mode_count=$usermode
                        	export user_mode_count=`expr $user_mode_count + 1`
                          fi
                        done < /home/satheesmohan/MOVE/INPUT/Environment_Details.txt
    					
                 OPTION=$(whiptail --title "SELECT USER MODE [$SOURCE_OR_DESTINTION]" --menu "CHOOSE USER ACCESS TYPE:" 15 60 3 \
                  "1" "READ_ONLY" \
                  "2" "ALL" \
                  "3" "ADMIN"  3>&1 1>&2 2>&3) 
                  exitstatus=$?
                  if [ $exitstatus = 0 ]; then
                      
                  	case "$OPTION" in
                  	             1)
                  	             	export USERMODE=$usermode_1
                  					GET_CREDENTIALS
                  	             	;;
                  	             2)
                  	             	export USERMODE=$usermode_2
                  					GET_CREDENTIALS
                  	             	;;
                  	             3)
                  	             	export USERMODE=$usermode_3
                  					GET_CREDENTIALS
                  	             	;;
                  				 *)
                  	             	echo "Sorry, I don't understand"
                  	             	;;
                      esac
                  else
                      whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 8 40       
                     	exit 0 
                  fi
				  
				  
                
    }
    
    get_env_details(){
    
    OPTION=$(whiptail --title "SELECT $SOURCE_OR_DESTINTION ENVIRONMENT" --menu "$SOURCE_OR_DESTINTION ENVIRONMENT :" 15 60 6 \
    "1" "DEV-2" \
    "2" "SIT-2" \
    "3" "PRO" \
	"4" "DEV" \
    "5" "SIT-3" \
    "6" "SIT"  3>&1 1>&2 2>&3) 	
    exitstatus=$?
    if [ $exitstatus = 0 ]; then
        
    	case "$OPTION" in
    	             1)
    	             	export ENV="DEV2"
    					GET_USERMODE
    	             	;;
    	             2)
    	             	export ENV="SIT2"
    					GET_USERMODE
    	             	;;
    	             3)
    	             	export ENV="PRO"
    					if [ $SOURCE_OR_DESTINTION = "SOURCE"  ];then 
						 GET_USERMODE
						else
                         whiptail --title "VERTICA-FAST_1.0" --msgbox "\n<******> ACCESS DENIED <******>\nKindly Don't select Production As Destination Environment.\n\nTry Again." 15 60       						
						 get_env_details
						fi
    	             	;;
    				 4)
    	             	export ENV="DEV"
    					GET_USERMODE
    					;;
					 5)
    	             	
						 whiptail --title "VERTICA-FAST_1.0" --msgbox "\n<******> ACCESS DENIED <******>\nSORRY CURRENTLY SIT-3 IS NOT AVAILABLE\n\nTry Again." 15 60       						
						 get_env_details
						
    					;;
					 6)
    	             	export ENV="SIT"
    					GET_USERMODE
    					;;
    			     *)
    	             	echo "Sorry, I don't understand"
    	             	;;
        esac
    else
        whiptail --title "VERTICA-FAST_1.0" --msgbox "Thanks for using the script!!!" 8 40       
       	exit 0 
    fi
    
    }
    
    export SOURCE_OR_DESTINTION="SOURCE";get_env_details;
    print "SOURCE DETAILS\n=================" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    print "SOURCE_db : $SOURCE_db\nSOURCE_pswd : $SOURCE_pswd\nSOURCE_loadbalancer : $SOURCE_loadbalancer\nSOURCE_env : $SOURCE_env\nSOURCE_usermode : $SOURCE_usermode\n"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    export SOURCE_OR_DESTINTION="DESTINATION";get_env_details;
    print "DESTINATION DETAILS\n=================" >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    print "DESTINATION_db : $DESTINATION_db\nDESTINATION_pswd : $DESTINATION_pswd\nDESTINATION_loadbalancer : $DESTINATION_loadbalancer\nDESTINATION_env : $DESTINATION_env\nDESTINATION_usermode : $DESTINATION_usermode\n"  >> /home/satheesmohan/MOVE/OUTPUT/log.txt
    
	CHECK_CONNECTION_STATUS

}
##################################################################################### MAIN-END #########################################

print "[SCRIPT_STARTED]\n---------------" > /home/satheesmohan/MOVE/OUTPUT/log.txt
export SuccessTable_Count=0
export RejectedTable_Count=0
export is_CLONING="FALSE"
print "Below tables are Copied successfully\n=========================================" > /home/satheesmohan/MOVE/OUTPUT/SuccessTables.txt
print "Below tables are Rejected dueto runtime error\n============================================" > /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt
print "DESTINATION MISSING COLUMN DETAILS\n============================================" >> /home/satheesmohan/MOVE/OUTPUT/missing_columns.txt
rm -rf /home/satheesmohan/MOVE/OUTPUT/missing_columns.txt | true 

c=`find "/home/satheesmohan/MOVE/INPUT/tablenames.txt" -type f -exec echo Found file {} \;`
if [ `echo ${#c}` != 0 ];then 
   
   if [ `cat /home/satheesmohan/MOVE/INPUT/tablenames.txt | wc -l` != 0 ];then 
        c=`cat /home/satheesmohan/MOVE/INPUT/tablenames.txt | wc -l`
        whiptail --title "VERTICA-FAST_1.0" --msgbox " INPUT FILE DETECTED \n INPUT TABLE COUNT = $c" 10 50
		MAIN_METHOD
   else
 	    whiptail --title "VERTICA-FAST_1.0" --msgbox " ### INPUT FILE IS EMPTY ###\nPLEASE CHECK /home/satheesmohan/MOVE/INPUT/tablenames.txt FILE" 10 50
   fi

else
   whiptail --title "VERTICA-FAST_1.0" --msgbox " INPUT TABLE LIST IS NOT FOUND\nPLEASE PLACE /home/satheesmohan/MOVE/INPUT/tablenames.txt FILE" 10 50
fi


rm -rf DDL_Statement.txt
rm -rf /home/satheesmohan/MOVE/OUTPUT/common_columns.txt
rm -rf /home/satheesmohan/MOVE/OUTPUT/source_table_columns.txt
rm -rf /home/satheesmohan/MOVE/OUTPUT/destination_table_columns.txt
rm -rf tmp.txt
rm -rf /home/satheesmohan/MOVE/OUTPUT/temp2.txt
print "============================================\nTOTAL SUCCESS TABLES : $SuccessTable_Count\n============================================" >> /home/satheesmohan/MOVE/OUTPUT/SuccessTables.txt
print "============================================\nTOTAL REJECTED TABLES : $RejectedTable_Count\n============================================" >> /home/satheesmohan/MOVE/OUTPUT/RejectedTables.txt
