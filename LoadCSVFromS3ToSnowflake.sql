/* TRUNCATE TABLE */
/* Remove all data from the table PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATES */
TRUNCATE TABLE PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATES;

/* CREATE TABLES */
/* Create or replace the table PROJECT_DATES with specified columns */
CREATE OR REPLACE TABLE PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATES (
    Project_Id NUMBER, 
    Project_Completion_Date VARCHAR, 
    Date_Contract_Expiration_sfdc VARCHAR, 
    Date_Contract_sfdc VARCHAR, 
    Date_Expiration VARCHAR, 
    Project_Start VARCHAR, 
    Project_Created VARCHAR, 
    Earliest_Allocation VARCHAR, 
    Earliest_Task_Start VARCHAR, 
    Earliest_Time_Entry VARCHAR 
);

/* COPY INTO */
/* Load data into PROJECT_DATES table from the specified CSV file in the S3 stage */
COPY INTO "PROJECT_DB"."PROJECT_MANUAL_DATA"."PROJECT_DATES" FROM 
(select t.$1,t.$2 from '@analytics_data_files_files_s3_dev_stage/Global_Services/Project_Dates.csv' 
(FILE_FORMAT => 'PROJECT_DB.PROJECT_MANUAL_DATA.CSV')t) ON_ERROR = 'CONTINUE';

/* CREATE FILE FORMAT */
/* Create a file format named CR_CSV for reading CSV files with specified options */
CREATE FILE FORMAT "RAW_DB"."PROJECT_MANUAL_DATA".CR_CSV  
    COMPRESSION = 'NONE' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 8 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N','NaN');

/* CREATE PROCEDURE */
/* Create or replace a stored procedure to load files from S3 into Snowflake table */
CREATE OR REPLACE PROCEDURE RAW_DB.PROJECT_MANUAL_DATA.GLOBALSERVICES_FILES_LOAD_S3_INTO_SNOWFLAKE(
    FOLDER VARCHAR, 
    FILENAME VARCHAR, 
    TABLENAME VARCHAR 
) 
RETURNS VARCHAR 
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER 
AS  
$$ 
    try { 
        var s3_stage = "@PROJECT_DB.PROJECT_MANUAL_DATA.analytics_data_files_files_s3_dev_stage/Global_Services/"; 
        var s3_path = s3_stage + FOLDER + "/" + FILENAME;
         
        var sqlUpdStmt = "COPY INTO " + TABLENAME + " FROM " + "'" + s3_path + "'"; 
         
        var file_format1 = " FILE_FORMAT = (FORMAT_NAME = 'PROJECT_DB.PROJECT_MANUAL_DATA.CSV') ON_ERROR = 'CONTINUE';"; 
        var err_code = ""; 

        query1 = sqlUpdStmt + file_format1; 
        query2 = sqlUpdStmt + file_format2;

        /* Truncate the target table before loading data */
        snowflake.execute({ sqlText: "TRUNCATE TABLE IDENTIFIER(?)", binds: [TABLENAME] });

        /* Get the current date */
        date_result = snowflake.execute({sqlText: `Select to_varchar(Current_date())`});
        date_result.next();
        today = date_result.getColumnValue(1);

        /* Execute the appropriate query based on the table name */
        if (TABLENAME == "PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATES") { 
            result_scan_1 = snowflake.execute({ sqlText: query1 }); 
        } else { 
            result_scan_1 = snowflake.execute({ sqlText: query2 }); 
        }
         
        result = result_scan_1.next();
         
        /* Log the results into audit_table_logs */
        snowflake.execute({ 
            sqlText: `INSERT INTO PROJECT_DB.PROJECT_MANUAL_DATA.audit_table_logs VALUES (?,?,?,?,?,?,?,?,?)`, 
            binds: [ 
                result_scan_1.getColumnValue(1), 
                result_scan_1.getColumnValue(2), 
                result_scan_1.getColumnValue(3), 
                result_scan_1.getColumnValue(4), 
                today, 
                result_scan_1.getColumnValue(6), 
                result_scan_1.getColumnValue(7), 
                result_scan_1.getColumnValue(8), 
                result_scan_1.getColumnValue(10) 
            ] 
        });
    }  
    catch (err) { 
        if (err_code == "") { 
            err_code = (!err.code) ? 0 : err.code; 
            err_state = (!err.state) ? "ERROR" : err.state; 
            err_msg = (!err.message) ? "ERROR" : err.message; 
            err_trace = (!err.stackTraceTxt) ? "ERROR" : err.stackTraceTxt; 
            result = err_msg; 
            throw result; 
        } 
    }  
return query1; 
$$;

/* CALL PROCEDURE */
/* Execute the stored procedure to load the Project_Dates.csv file into the PROJECT_DATES table */
CALL RAW_DB.MANUAL_DATA.GLOBALSERVICES_FILES_LOAD_S3_INTO_SNOWFLAKE(
    'Projects', 
    'Project_Dates.csv', 
    'PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATES'
);

/* CREATE TASK */
/* Create or replace a task to run the stored procedure daily at 22:00 UTC */
CREATE OR REPLACE TASK projects_data_load_daily_task 
    WAREHOUSE = DEV_TRANSFORMING_WH 
    SCHEDULE = 'USING CRON * 22 * * * UTC' 
    COMMENT = 'Run stored procedure to populate Projects_Dates data into Snowflake table daily' 
AS CALL RAW_DB.MANUAL_DATA.GLOBALSERVICES_FILES_LOAD_S3_INTO_SNOWFLAKE(
    'Projects', 
    'Project_Dates.csv', 
    'PROJECT_DB.PROJECT_MANUAL_DATA.PROJECT_DATEST'
);
