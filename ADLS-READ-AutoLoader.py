# Databricks notebook source
# Import functions
from pyspark.sql.functions import when, lit, coalesce, to_json, col, from_json, udf, expr, size, concat, explode_outer, row_number, split,struct, udf, array, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, MapType,ArrayType, BooleanType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from ipaddress import IPv4Address,ip_address
from pyspark.sql import Row
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

# COMMAND ----------

# Storage Connection Initialization
container_name = "iot-messages2"
iothub_name = "rmm-dev-iot"
storage_account_name1 = "storageaccforeventgrid"
storage_account_name = "messagestorage4poc" #DEV6

# COMMAND ----------

# Create schema
hfsi_schema = StructType([
    StructField("HFSIIndex", IntegerType(), True),
    StructField("HFSIChainLink", StringType(), True),
    StructField("HFSILifeSpec", IntegerType(), True),
    StructField("HFSICurrentLife", IntegerType(), True),
])

billing_meter_schema = StructType([
    StructField("BillingMeterId", IntegerType(), True),
    StructField("BillingMeterCount", LongType(), True),
])

billing_meter_detail_schema = StructType([
    StructField("BillingMeterDetailId", IntegerType(), True),
    StructField("BillingMeterDetailType", IntegerType(), True),
    StructField("BillingMeterDetailCount", LongType(), True),
])

HFSIList_schema = ArrayType(hfsi_schema)
billingMeterList_schema = ArrayType(billing_meter_schema)
billingMeterDetailList_schema = ArrayType(billing_meter_detail_schema)

production_schema = StructType([
    StructField("HFSIList", HFSIList_schema, True),
    StructField("billingMeterList", billingMeterList_schema, True),
    StructField("billingMeterDetailList", billingMeterDetailList_schema, True),
])
code_schema = StructType([
    StructField("id", StringType(), False),
    StructField("value", StringType(), False)
])
imageSchema = StructType([
    StructField("url", StringType(), False),
    StructField("height", StringType(), False),
    StructField("width", StringType(), False)
])
ethernet_schema = StructType([
    StructField("id", StringType(), False),
    StructField("type", StringType(), False),
    StructField("address", StringType(), False),
])
ip_obj_chema = StructType([
    StructField("address", StringType(), False),
    StructField("ethernetId", StringType(), False),
    StructField("subnetMask", StringType(), False),
    StructField("defaultRoute", StringType(), False)
])
device_entry_obj_schema = StructType([
    StructField("type", StringType(), False),
    StructField("description", StringType(), False),
    StructField("status", StringType(), False),
    StructField("errors", StringType(), False),
    StructField("deviceId", StringType(), False),
    StructField("deviceIndex", StringType(), False)
])
capability_obj_schema = StructType([
    StructField("available", BooleanType(), False),
    StructField("version", StringType(), False)
])
status_schema = StructType([
    StructField("communicationStatus", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("status", ArrayType(StringType()), False)
])
maintanance_code_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("maintenanceCodeList", ArrayType(code_schema), False)
])
service_code_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("serviceCodeList", ArrayType(code_schema), False)
])
interFace_schema = StructType([
    StructField("ethernetList",ArrayType(ethernet_schema), True),
    StructField("ipList",ArrayType(ip_obj_chema), True)
])
basicInfo_schema = StructType([
    StructField("deviceEntryList", ArrayType(device_entry_obj_schema), True),
    StructField("machineId", StringType(), True)
])
capability_schema = StructType([
    StructField("security", BooleanType(), True),
    StructField("poweroff", BooleanType(), True),
    StructField("serviceReport", BooleanType(), True),
    StructField("powerManagement", BooleanType(), True),
    StructField("deviceCloning", BooleanType(), True),
    StructField("fss", BooleanType(), True),
    StructField("fssDataCloning", capability_obj_schema, True),
    StructField("fwUpdateOTA", capability_obj_schema, True),
    StructField("logData", capability_obj_schema, True)
])
fwUpdateOTASetting_schema = StructType([
    StructField("mode", StringType(),True),
    StructField("startHour",IntegerType(),True),
    StructField("endHour",IntegerType(),True)
])
alertInfo_schema = StructType([
    StructField("newStatus",status_schema,True),
    StructField("oldStatus",status_schema,True),
    StructField("newMaintenanceCode",maintanance_code_schema,True),
    StructField("oldMaintenanceCode",maintanance_code_schema,True),
    StructField("newServiceCode",service_code_schema,True),
    StructField("oldServiceCode",service_code_schema,True)
])
optional_schema = StructType([StructField("fileFilter", StringType(), False)])
serviceCodeList_schema = ArrayType(code_schema)
schema_checkpoint = StructType([
    StructField("partition", StringType(), True),
    StructField("last_read_file", StringType(), True)
])

# COMMAND ----------

# Window specification for the last record per device
main_window_spec = Window.partitionBy("deviceId").orderBy(col("timestamp").desc())

# Define status code mapping dictionary
status_code_map = {
    "printerError": 10,
    "accountLimit": 20,
    "overduePreventMaintenance": 30,
    "paperJam": 40,
    "markerSupplyMissing": 50,
    "tonerEmpty": 60,
    "coverOpen": 70,
    "paperEmpty": 80,
    "specifiedInputTrayEmpty": 90,
    "specifiedInputTrayMissing": 100,
    "allOutputTrayFull": 110,
    "specifiedOutputTrayMissing": 120,
    "offline": 130,
    "printerWarning": 140,
    "tonerLow": 150,
    "paperLow": 160,
    "inputTrayMissing": 170,
    "outputTrayFull": 180,
    "outputTrayNearFull": 190,
    "outputTrayMissing": 200,
    "stackerNotInstalled": 210,
    "nearOverduePreventMaintenance": 220,
    "standby": 230,
    "warmUp": 240,
    "printing": 250,
    "online": 260,
}


# Function to get mac, gateway and subnet
def get_comm_settings(interface_object, ip_address):
    mac_id = ""
    default_route = ""
    subnet_mask = ""
    try:
        ip_lists = interface_object["ipList"]
        ethernet_lists = interface_object["ethernetList"]
        ethernet_obj = [
            ipList for ipList in ip_lists if ipList["address"] == ip_address
        ][0]
        ethernet_id = ethernet_obj["ethernetId"]
        default_route = ethernet_obj["defaultRoute"]
        subnet_mask = ethernet_obj["subnetMask"]
        mac_id = [
            ethernetList["address"]
            for ethernetList in ethernet_lists
            if ethernetList["id"] == ethernet_id and len(ethernetList["address"]) > 0
        ][0]
    except Exception:
        return f"{mac_id}/{default_route}/{subnet_mask}"

    return f"{mac_id}/{default_route}/{subnet_mask}"


# Define the UDF
def ip_to_long(ip_str):
    try:
        # Convert the IP address string to an integer
        return int(ip_address(ip_str))
    except ValueError:
        # Return None if it's not a valid IP address
        return None

# Define the UDF
def get_status_list(status_set_dict):
    if status_set_dict is None:
        return []
    true_keys = []
    try:
        for field in status_set_dict.__fields__:
            if getattr(status_set_dict, field) == "true":
                true_keys.append(field)
    except ValueError:
        return None
    return true_keys


# Define the UDF
def get_status_code(status_list):
    status_code_list = [
        status_code_map.get(status)
        for status in status_list
        if status in status_code_map
    ]
    status_code_list.sort()
    if len(status_code_list) > 0:
        return status_code_list[0]
    else:
        return 0


# Register the UDFs with PySpark
ip_to_long_udf = udf(ip_to_long, LongType())
get_comm_settings_udf = udf(get_comm_settings, StringType())
get_status_list_udf = udf(get_status_list, ArrayType(StringType()))
get_status_code_udf = udf(get_status_code, IntegerType())

# COMMAND ----------


# Define variables used in code below
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"  # Update with your ADLS path
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"adls_iothub_etl"
checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_quickstart"
counter_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_devicestatus"
deviceStatus_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_counterupdate"
toner_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_tonerUpdate"
basicStatus_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_basicUpdate"
deviceLog_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_deviceLog"
tray_checkpoint_path = f"/tmp/ADLS_IOTHUB_ETL/checkpoint/etl_trayUpdate"

# Clear out data from previous demo execution
# Do not remove the checkpoint directory to maintain the state
#spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(counter_checkpoint_path, True)
dbutils.fs.rm(deviceStatus_checkpoint_path, True)
dbutils.fs.rm(toner_checkpoint_path, True)
dbutils.fs.rm(basicStatus_checkpoint_path, True)
dbutils.fs.rm(deviceLog_checkpoint_path, True)
dbutils.fs.rm(tray_checkpoint_path, True)

# IOT Message schema
schema = StructType([
    StructField("EnqueuedTimeUtc", StringType(), True),
    StructField("Properties", StructType([
        StructField("appTopic", StringType(), True),
        StructField("tenantId", StringType(), True),
        StructField("dealershipId", StringType(), True),
    ]), True),
    StructField("SystemProperties", StructType([
        StructField("connectionDeviceId", StringType(), True),
        StructField("connectionAuthMethod", StringType(), True),
        StructField("connectionDeviceGenerationId", StringType(), True),
        StructField("contentType", StringType(), True),
        StructField("contentEncoding", StringType(), True),
        StructField("enqueuedTime", StringType(), True),
    ]), True),
    StructField("Body", StructType([
        StructField("type", StringType(), True),
        StructField("mnsn", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("scheduleName", StringType(), True),
        StructField("smsProperties", StructType([
            StructField("service", StructType([
                StructField("print", StructType([
                    StructField("mediaPathList", ArrayType(StructType([
                        StructField("id", StringType(), True),
                        StructField("type", StringType(), True),
                    ])), True),
                    StructField("channelList", ArrayType(StructType([
                        StructField("descriptionLanguageDefault", StringType(), True),
                        StructField("mediaPathDefault", StringType(), True),
                        StructField("id", StringType(), True),
                    ])), True),
                    StructField("descriptionLanguageList", ArrayType(StructType([
                        StructField("description", StringType(), True),
                        StructField("langVersion", StringType(), True),
                        StructField("orientationDefault", StringType(), True),
                        StructField("id", StringType(), True),
                        StructField("family", StringType(), True),
                        StructField("version", StringType(), True),
                        StructField("level", StringType(), True),
                    ])), True),
                ]), True),
            ]), True),
            StructField("interface", StructType([
                StructField("ethernetList", ArrayType(StructType([
                    StructField("id", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("address", StringType(), True),
                ])), True),
                StructField("ipList", ArrayType(StructType([
                    StructField("subnetMask", StringType(), True),
                    StructField("ethernetId", StringType(), True),
                    StructField("defaultRoute", StringType(), True),
                    StructField("address", StringType(), True),
                ])), True),
            ]), True),
            StructField("device", StructType([
                StructField("markerList", ArrayType(StructType([
                    StructField("description", StringType(), True),
                    StructField("amount", StructType([
                        StructField("capacity", StringType(), True),
                        StructField("unit", StringType(), True),
                        StructField("typical", StringType(), True),
                        StructField("state", StringType(), True),
                    ]), True),
                    StructField("color", StringType(), True),
                    StructField("type", StringType(), True),
                ])), True),
                StructField("disposalMarkerList", ArrayType(StructType([
                    StructField("description", StringType(), True),
                    StructField("amount", StructType([
                        StructField("capacity", StringType(), True),
                        StructField("unit", StringType(), True),
                        StructField("typical", StringType(), True),
                        StructField("state", StringType(), True),
                    ]), True),
                    StructField("color", StringType(), True),
                    StructField("type", StringType(), True),
                ])), True),
                StructField("memory", StructType([
                    StructField("size", StringType(), True),
                ]), True),
                StructField("deviceEntryList", ArrayType(StructType([
                    StructField("description", StructType([]), True),
                    StructField("status", StructType([]), True),
                    StructField("deviceID", StructType([]), True),
                    StructField("deviceIndex", StructType([]), True),
                    StructField("errors", StructType([]), True),
                    StructField("type", StructType([]), True),
                ])), True),
                StructField("configuration", StructType([
                    StructField("duplexModule", StringType(), True),
                    StructField("installedOptions", StructType([
                        StructField("dsk", StringType(), True),
                        StructField("value", StringType(), True),
                    ]), True),
                ]), True),
                StructField("trapSupported", StringType(), True),
                StructField("description", StringType(), True),
                StructField("coverList", ArrayType(StructType([
                    StructField("description", StringType(), True),
                    StructField("status", StringType(), True),
                ])), True),
                StructField("statusSet", StructType([
                   StructField("online", StringType(), True),
                   StructField("specifiedOutputTrayMissing", StringType(), True),
                   StructField("allOutputTrayFull", StringType(), True),
                   StructField("outputTrayMissing", StringType(), True),
                   StructField("stackerNotInstalled", StringType(), True),
                   StructField("specifiedInputTrayMissing", StringType(), True),
                   StructField("inputTrayMissing", StringType(), True),
                   StructField("nearOverduePreventMaintenance", StringType(), True),
                   StructField("overduePreventMaintenance", StringType(), True),
                   StructField("specifiedInputTrayEmpty", StringType(), True),
                   StructField("markerSupplyMissing", StringType(), True),
                   StructField("outputTrayNearFull", StringType(), True),
                   StructField("warmUp", StringType(), True),
                   StructField("printing", StringType(), True),
                   StructField("tonerLow", StringType(), True),
                   StructField("paperLow", StringType(), True),
                   StructField("coverOpen", StringType(), True),
                   StructField("paperEmpty", StringType(), True),
                   StructField("standby", StringType(), True),
                   StructField("accountLimit", StringType(), True),
                   StructField("offline", StringType(), True),
                   StructField("paperJam", StringType(), True),
                   StructField("printerError", StringType(), True),
                   StructField("tonerEmpty", StringType(), True),
                   StructField("printerWarning", StringType(), True),
                   StructField("outputTrayFull", StringType(), True), 
                ]), True),
                StructField("outTrayDefault", StringType(), True),
                StructField("capability", StructType([
                    StructField("function", StructType([
                        StructField("security", StringType(), True),
                        StructField("poweroff", StringType(), True),
                        StructField("fss", StringType(), True),
                        StructField("fssDataCloning", StructType([
                            StructField("available", StringType(), True),
                            StructField("version", StringType(), True),
                        ]), True),
                        StructField("fwUpdateOTA", StructType([
                            StructField("available", StringType(), True),
                            StructField("version", StringType(), True),
                        ]), True),
                        StructField("logData", StructType([
                            StructField("available", StringType(), True),
                            StructField("version", StringType(), True),
                        ]), True),
                        StructField("powerManagement", StringType(), True),
                        StructField("deviceCloning", StringType(), True),
                        StructField("serviceReport", StringType(), True),
                    ]), True),
                ]), True),
                StructField("serialId", StringType(), True),
                StructField("engine", StructType([
                    StructField("pagesPerMinitues", StringType(), True),
                ]), True),
                StructField("codeSet", StringType(), True),
                StructField("familyName", StringType(), True),
                StructField("serviceCodeList", ArrayType(StructType([
                    StructField("id", StringType(), True),
                    StructField("value", StringType(), True),
                ])), True),
                StructField("contactAddress", StringType(), True),
                StructField("outTrayList", ArrayType(StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("amount", StructType([
                        StructField("capacity", StringType(), True),
                        StructField("unit", StringType(), True),
                        StructField("typical", StringType(), True),
                        StructField("state", StringType(), True),
                    ]), True),
                    StructField("stackingOrder", StringType(), True),
                    StructField("modelName", StringType(), True),
                    StructField("deliveryOrientation", StringType(), True),
                    StructField("id", StringType(), True),
                ])), True),
                StructField("firmwareVersion", StringType(), True),
                StructField("friendlyName", StringType(), True),
                StructField("address", StringType(), True),
                StructField("maintenanceCodeList", ArrayType(StructType([
                    StructField("id", StringType(), True),
                    StructField("value", StringType(), True),
                ])), True),
                StructField("counter", StructType([
                    StructField("TYPE", StructType([
                        StructField("detail", StructType([
                            StructField("send", StructType([
                                StructField("JOBTYPE", StructType([
                                    StructField("faxSend", StructType([
                                        StructField("value", IntegerType(), True),
                                        StructField("fax3Send", StructType([]), True),
                                        StructField("fax1Send", StructType([]), True),
                                        StructField("fax2Send", StructType([]), True),
                                        StructField("COLORTYPE", StructType([
                                            StructField("color", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True),
                                            StructField("monochrome", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True)
                                        ]), True),
                                    ]), True),
                                    StructField("scanToHDD", StructType([
                                        StructField("value", IntegerType(), True),
                                        StructField("COLORTYPE", StructType([
                                            StructField("color", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True),
                                            StructField("monochrome", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True)
                                        ]), True),
                                    ]), True),
                                    StructField("scanToEmail", StructType([
                                        StructField("value", IntegerType(), True),
                                        StructField("COLORTYPE", StructType([
                                            StructField("color", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True),
                                            StructField("monochrome", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True)
                                        ]), True),
                                    ]), True),
                                    StructField("internetFaxSend", StructType([
                                        StructField("value", IntegerType(), True),
                                        StructField("COLORTYPE", StructType([
                                            StructField("color", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True),
                                            StructField("monochrome", StructType([
                                                StructField("value", IntegerType(), True)
                                            ]), True),
                                        ]), True),
                                    ]), True),
                                ]), True),
                            ]), True),
                            StructField("output", StructType([
                                StructField("JOBTYPE",StructType([
                                    StructField("copies",StructType([]), True),
                                    StructField("filingData",StructType([]), True),
                                    StructField("fax",StructType([]), True),
                                    StructField("prints",StructType([]), True),
                                    StructField("internetFax",StructType([]), True),
                                    StructField("others",StructType([]), True),
                                ]), True),
                            ]), True),
                        ]), True),
                        StructField("total", StructType([
                            StructField("send", StructType([]), True),
                            StructField("output", StructType([
                                StructField("value", IntegerType(), True),
                                StructField("COLORTYPE", StructType([
                                    StructField("color", StructType([
                                        StructField("value", IntegerType(), True),
                                    ]), True),
                                    StructField("monochrome", StructType([
                                        StructField("value", IntegerType(), True),
                                    ]), True),
                                ]), True),
                            ]), True),
                        ]), True),
                        StructField("a3orAbove", StructType([]), True),
                        StructField("postcard", StructType([]), True),
                        StructField("feeder", StructType([
                            StructField("value", IntegerType(), True),
                        ]), True),
                        StructField("duplex", StructType([
                            StructField("value", IntegerType(), True),
                        ]), True),
                        StructField("powerOnCount", StructType([
                            StructField("unit", StringType(), True),
                            StructField("value", IntegerType(), True),
                            StructField("type", StringType(), True),
                        ]), True),
                        StructField("lifeCount", StructType([
                            StructField("unit", StringType(), True),
                            StructField("value", IntegerType(), True),
                            StructField("type", StringType(), True),
                        ]), True),
                    ]), True),
                ]), True),
                StructField("statusRawValue", StringType(), True),
                StructField("errorLevel", StringType(), True),
                StructField("embededWebServerSupported", StringType(), True),
                StructField("modelName", StringType(), True),
                StructField("upTime", StringType(), True),
                StructField("machineId", StringType(), True),
                StructField("deviceImage", StructType([
                    StructField("currentImage", StructType([
                        StructField("rawData", StringType(), True),
                        StructField("height", StringType(), True),
                        StructField("width", StringType(), True),
                        StructField("size", StringType(), True),
                        StructField("format", StringType(), True),
                        StructField("URL", StringType(), True),
                    ]), True),
                    StructField("allowedFormat", StructType([
                        StructField("format", StringType(), True),
                    ]), True),
                ]), True),
                StructField("location", StructType([
                    StructField("address", StringType(), True),
                    StructField("language", StringType(), True),
                    StructField("country", StringType(), True),
                ]), True),
                StructField("inTrayList", ArrayType(StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("amount", StructType([
                        StructField("capacity", StringType(), True),
                        StructField("unit", StringType(), True),
                        StructField("typical", StringType(), True),
                        StructField("state", StringType(), True),
                    ]), True),
                    StructField("inserter", StringType(), True),
                    StructField("mediaSize", StringType(), True),
                    StructField("mediaSizeUnit", StringType(), True),
                    StructField("manual", StringType(), True),
                    StructField("mediaDimXFeed", StringType(), True),
                    StructField("mediaSizeHeight", StringType(), True),
                    StructField("mediaSizeName", StringType(), True),
                    StructField("mediaDimFeed", StringType(), True),
                    StructField("mediaSizeWidth", StringType(), True),
                    StructField("virtual", StringType(), True),
                    StructField("mediaName", StringType(), True),
                    StructField("mediaType", StringType(), True),
                    StructField("modelName", StringType(), True),
                    StructField("id", StringType(), True),
                ])), True),
                StructField("inTrayDefault", StringType(), True),

            ]), True),
        ]), True),
    ]), True),
])

# Update or Insert the Record based on deviceId
def upsert_to_delta_table(batch_df, batch_id,schedule_name, updateDataSet):
    window_spec = Window.partitionBy("deviceId").orderBy(F.col("timestamp").desc())
    # Define a function to process each schedule name
    try:
        # Filter the DataFrame based on the current schedule name
        #filtered_df = batch_df.filter(F.col("scheduleName") == schedule_name)
        final_latest_df = batch_df.withColumn("row_num", row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
        
        # Define the table name based on the schedule name
        table_name = f"default.{schedule_name}_table"  # Adjust this according to your naming convention

        # Load the Delta table
        deltaTable = DeltaTable.forName(spark, table_name)
        print(f"Table {table_name} exists and is a Delta table.")

        # Define the merge condition and the update actions
        if schedule_name !=  "deviceTonerInfo" or schedule_name !=  "deviceTrayInfo":
            merge_condition = "target.deviceId = source.deviceId"
        else:
            merge_condition = "target.deviceId = source.deviceId AND target.description = source.description"
        

        # Perform the merge operation
        (deltaTable.alias("target")
            .merge(
                final_latest_df.alias("source"),
                merge_condition
            )
        .whenMatchedUpdate(set= updateDataSet)
        .whenNotMatchedInsertAll()
        .execute()
        )
    except Exception as e:
        print(e)
        print(f"Table {table_name} not available. Creating a new Delta table.")
        final_latest_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name)

# Configure Auto Loader to ingest JSON data from ADLS to a Delta table
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .schema(schema)  # Specify the schema explicitly
  .load(adls_path)  # Read from ADLS
)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#Status Update
df_deviceStatus = (df.filter(col("Body.scheduleName") == "deviceStatus")
                    .withColumn("timestamp", col("Body.timestamp"))
                    .withColumn("scheduleName",col("Body.scheduleName"))
                    .withColumn("relatedGroupId", when(col("Properties.tenantId").isNotNull(), col("Properties.tenantId")).otherwise(None).cast(StringType()))
                    .withColumn("communicationStatus", lit("0000"))
                    .withColumn("relatedAgentId", split(col("Properties.appTopic"), "/").getItem(2))
                    .withColumn("scheduleName",col("Body.scheduleName"))
                    .withColumn("timestamp", col("Body.timestamp"))
                    .withColumn("deviceId", col("Body.mnsn"))
                    .withColumn("type", lit("mfp"))
                    .withColumn("relatedGroupId", when(col("Properties.tenantId").isNotNull(), col("Properties.tenantId")).otherwise(None).cast(StringType()))
                    .withColumn("communicationStatus", lit("0000"))
                    .withColumn("relatedAgentId", split(col("Properties.appTopic"), "/").getItem(2))
                    .withColumn("familyName", when(col("Body.smsProperties.device.familyName").isNotNull(), col("Body.smsProperties.device.familyName")).otherwise(None).cast(StringType()))
                    .withColumn("productFamilyName", when(col("Body.smsProperties.device.familyName").isNotNull(), col("Body.smsProperties.device.familyName")).otherwise(None).cast(StringType()))
                    .withColumn("friendlyName", when(col("Body.smsProperties.device.friendlyName").isNotNull(), col("Body.smsProperties.device.friendlyName")).otherwise(None).cast(StringType()))
                    .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
                    .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
                    .withColumn("ipAddress", when(col("Body.smsProperties.device.address").isNotNull(), col("Body.smsProperties.device.address")).otherwise(None).cast(StringType()))
                    .withColumn("location", when(col("Body.smsProperties.device.location.address").isNotNull(), col("Body.smsProperties.device.location.address")).otherwise(None).cast(StringType())) #NEED ATTENTION
                    .withColumn("description", lit(None).cast(StringType()))
                    .withColumn("firmwareVersion", lit(None).cast(StringType()))
                    .withColumn("lastBasicUpdate", lit(None).cast(LongType()))
                    .withColumn("basicInfo", lit(None).cast(basicInfo_schema))
                    .withColumn("capability", lit(None).cast(capability_schema))
                    .withColumn("deviceImage", lit(None).cast(imageSchema))
                    .withColumn("comm_settings", get_comm_settings_udf(col("Body.smsProperties.interface"), col("ipAddress")))
                    .withColumn("macAddress", split(col("comm_settings"), "/").getItem(0))
                    .withColumn("subnetMask", split(col("comm_settings"), "/").getItem(2))
                    .withColumn("gateway", split(col("comm_settings"), "/").getItem(1))
                    .withColumn("ipToLong", ip_to_long_udf(col("ipAddress")))
                    .withColumn("dskFlag", lit(None).cast(BooleanType()))
                    .withColumn("detectedDeviceType", when(col("familyName") == lit("PM00"), lit("PM00")).otherwise(col("familyName")))
                    .withColumn("createTimestamp", lit(None).cast(LongType()))
                    .withColumn("attribute", lit(None).cast(StringType()))
                    .withColumn("tagIds", lit(None).cast(ArrayType(StringType())))
                    .withColumn("accessMode", lit(None).cast(IntegerType()))
                    .withColumn("optional", lit(None).cast(optional_schema))
                    .withColumn("status", get_status_list_udf(col("Body.smsProperties.device.statusSet")))
                    .withColumn("statusCode", get_status_code_udf(col("status")))
                    .withColumn(
                        "dispErrorCode",
                        when((col("statusCode") >= 140) & (col("statusCode") <= 220), 20)
                        .when((col("statusCode") >= 230) & (col("statusCode") <= 260), 30)
                        .otherwise(10)
                        )
                    .withColumn("lifeCount", when(col("Body.smsProperties.device.counter").isNotNull(), col("Body.smsProperties.device.counter.TYPE.lifeCount.value").cast(LongType())))
                    .withColumn("serviceCodeList", lit(None).cast(serviceCodeList_schema))
                    .withColumn("maintenanceCodeList", lit(None).cast(serviceCodeList_schema))
                    .withColumn("lastStatusUpdate", when(col("timestamp").isNotNull(), col("timestamp")).otherwise(None).cast(LongType()))
                    .withColumn("fwUpdateOTASetting", lit(None).cast(fwUpdateOTASetting_schema))
                    .withColumn("alertInfo", lit(None).cast(alertInfo_schema))
                    .withColumn("interface", lit(None).cast(interFace_schema))
                    .withColumn("troubleCode", when(col("serviceCodeList").isNull() | col("maintenanceCodeList").isNull(), 0).otherwise(-10))
                    .select(["deviceId","timestamp","scheduleName","createTimestamp","type","attribute","relatedGroupId","tagIds","communicationStatus","accessMode","relatedAgentId","familyName","friendlyName","modelName","serialNumber","dispErrorCode","optional","ipAddress","ipToLong","macAddress","status","statusCode","troubleCode","lifeCount","serviceCodeList","maintenanceCodeList","location","subnetMask","gateway","deviceImage","dskFlag","description","firmwareVersion","lastBasicUpdate","lastStatusUpdate","detectedDeviceType","productFamilyName","interface","capability","fwUpdateOTASetting","alertInfo"])
                    )

deviceStatus_update_set = {
    "deviceId": col("source.deviceId"),
    "type": col("source.type"),
    "relatedGroupId": col("source.relatedGroupId"),
    "modelName": col("source.modelName"),
    "serialNumber": col("source.serialNumber"),
    "communicationStatus": col("source.communicationStatus"),
    "relatedAgentId": col("source.relatedAgentId"),
    "familyName": col("source.familyName"),
    "friendlyName": col("source.friendlyName"),
    "modelName": col("source.modelName"),
    "serialNumber": col("source.serialNumber"),
    "ipAddress": col("source.ipAddress"),
    "ipToLong": col("source.ipToLong"),
    "macAddress": col("source.macAddress"),
    "location": col("source.location"),
    "subnetMask": col("source.subnetMask"),
    "gateway": col("source.gateway"),
    "dskFlag": coalesce(col("source.dskFlag"), col("target.dskFlag")),
    "deviceImage": coalesce(col("source.deviceImage"), col("target.deviceImage")),
    "description": coalesce(col("source.description"), col("target.description")),
    "firmwareVersion": coalesce(col("source.firmwareVersion"), col("target.firmwareVersion")),
    "lastBasicUpdate": coalesce(col("source.lastBasicUpdate"), col("target.lastBasicUpdate")),
    "basicInfo": coalesce(col("source.basicInfo"), col("target.basicInfo")),
    "capability": coalesce(col("source.capability"), col("target.capability")),
    "detectedDeviceType": col("source.detectedDeviceType"),
    "productFamilyName": col("source.productFamilyName"),
    "createTimestamp": coalesce(col("source.createTimestamp"), col("target.createTimestamp")),
    "dispErrorCode": coalesce(col("source.dispErrorCode"), col("target.dispErrorCode")),
    "status": col("source.status"),
    "lifeCount": coalesce(col("source.lifeCount"), col("target.lifeCount")),
    "serviceCodeList": coalesce(col("source.serviceCodeList"), col("target.serviceCodeList")),
    "maintenanceCodeList": coalesce(col("source.maintenanceCodeList"), col("target.maintenanceCodeList")),
    "lastStatusUpdate": col("source.lastStatusUpdate")
}




# COMMAND ----------

#Counter Update
df_counterUpdate = ( df.filter(col("Body.scheduleName") == "counterUpdate")
            .withColumn("timestamp", col("Body.timestamp"))
            .withColumn("scheduleName",col("Body.scheduleName"))
            .withColumn("deviceId", when(col("Body.mnsn").isNotNull(), col("Body.mnsn")).otherwise(None).cast(StringType()))
            .withColumn("timestamp", col("Body.timestamp"))
            .withColumn("type", lit("mfp"))
            #.withColumn("relatedGroupId", when(col("Properties.relatedGroupId").isNotNull(), col("Properties.relatedGroupId")).otherwise(None).cast(StringType()))
            .withColumn("relatedGroupId", lit("relatedGroupId-1")) #UPDATE REQUIRED
            .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
            .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
            .withColumn("lastCounterUpdate", when(col("Body.timestamp").isNotNull(), col("Body.timestamp")).otherwise(None).cast(LongType()))
            .withColumn("total_output_color", when(col("Body.smsProperties.device.counter.TYPE.total.output.COLORTYPE.color.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.total.output.COLORTYPE.color.value")).otherwise(None).cast(IntegerType()))
            .withColumn("total_output_mono", when(col("Body.smsProperties.device.counter.TYPE.total.output.COLORTYPE.monochrome.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.total.output.COLORTYPE.monochrome.value")).otherwise(None).cast(IntegerType()))
            .withColumn("total_output_value", when(col("Body.smsProperties.device.counter.TYPE.total.output.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.total.output.value")).otherwise(None).cast(IntegerType()))
            .withColumn("total_scan_color", when(col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.COLORTYPE.color.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.COLORTYPE.color.value")).otherwise(None).cast(IntegerType()))
            .withColumn("total_scan_mono", when(col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.COLORTYPE.monochrome.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.COLORTYPE.monochrome.value")).otherwise(None).cast(IntegerType()))
            .withColumn("total_scan_value", when(col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.value").isNotNull(), col("Body.smsProperties.device.counter.TYPE.detail.send.JOBTYPE.scanToEmail.value")).otherwise(None).cast(IntegerType()))
            .withColumn("counter", when(col("Body.smsProperties.device.counter").isNotNull(), to_json(col("Body.smsProperties.device.counter"))).otherwise(None).cast(StringType()))
            .withColumn("production", lit(None).cast(production_schema))
            .select(["deviceId","timestamp","scheduleName","type","relatedGroupId","lastCounterUpdate","modelName","serialNumber","total_output_color","total_output_mono","total_output_value", "total_scan_color","total_scan_mono","total_scan_value","counter","production"])
        )
# Custom update logic to handle null values in counter_info_table
counter_update_set = {
    "deviceId": col("source.deviceId"),
    "type": col("source.type"),
    "relatedGroupId": col("source.relatedGroupId"),
    "lastCounterUpdate": coalesce(col("source.lastCounterUpdate"), col("target.lastCounterUpdate")),
    "modelName": col("source.modelName"),
    "serialNumber": col("source.serialNumber"),
    "total_output_color": coalesce(col("source.total_output_color"), col("target.total_output_color")),
    "total_output_mono": coalesce(col("source.total_output_mono"), col("target.total_output_mono")),
    "total_output_value": coalesce(col("source.total_output_value"), col("target.total_output_value")),
    "total_scan_color": coalesce(col("source.total_scan_color"), col("target.total_scan_color")),
    "total_scan_mono": coalesce(col("source.total_scan_mono"), col("target.total_scan_mono")),
    "total_scan_value": coalesce(col("source.total_scan_value"), col("target.total_scan_value")),
    "counter": coalesce(col("source.counter"), col("target.counter")),
    "production": col("source.production")
}



# COMMAND ----------

# Device Basic Info Table (Basic-update)
df_basicUpdate = ( df
            .filter(col("Body.scheduleName") == "basicUpdate")
            .withColumn("timestamp", col("Body.timestamp"))
            .withColumn("scheduleName",col("Body.scheduleName"))
            .withColumn("deviceId", when(col("Body.mnsn").isNotNull(), col("Body.mnsn")).otherwise(None).cast(StringType()))
            .withColumn("type", lit("mfp"))
            .withColumn("relatedGroupId", when(col("Properties.tenantId").isNotNull(), col("Properties.tenantId")).otherwise(None).cast(StringType()))
            .withColumn("communicationStatus", lit("0000"))
            .withColumn("relatedAgentId", split(col("Properties.appTopic"), "/").getItem(2))
            .withColumn("familyName", when(col("Body.smsProperties.device.familyName").isNotNull(), col("Body.smsProperties.device.familyName")).otherwise(None).cast(StringType()))
            .withColumn("productFamilyName", when(col("Body.smsProperties.device.familyName").isNotNull(), col("Body.smsProperties.device.familyName")).otherwise(None).cast(StringType()))
            .withColumn("friendlyName", when(col("Body.smsProperties.device.friendlyName").isNotNull(), col("Body.smsProperties.device.friendlyName")).otherwise(None).cast(StringType()))
            .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
            .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
            .withColumn("ipAddress", when(col("Body.smsProperties.device.address").isNotNull(), col("Body.smsProperties.device.address")).otherwise(None).cast(StringType()))
            .withColumn("location", when(col("Body.smsProperties.device.location.address").isNotNull(), col("Body.smsProperties.device.location.address")).otherwise(None).cast(StringType())) #NEED ATTENTION
            .withColumn("description", when(col("Body.smsProperties.device.description").isNotNull(), col("Body.smsProperties.device.description")).otherwise(None).cast(StringType()))
            .withColumn("firmwareVersion", when(col("Body.smsProperties.device.firmwareVersion").isNotNull(), col("Body.smsProperties.device.firmwareVersion")).otherwise(None).cast(StringType()))
            .withColumn("lastBasicUpdate", when(col("timestamp").isNotNull(), col("timestamp")).otherwise(None).cast(LongType()))
            .withColumn(
                "deviceEntryList",
                when(
                    col("Body.smsProperties.device.deviceEntryList").isNotNull() & (size(col("Body.smsProperties.device.deviceEntryList"))>0),
                    expr("""
                            transform(Body.smsProperties.device.deviceEntryList, x -> struct(
                                x.type as type,
                                x.description as description,
                                x.status as status,
                                x.errors as errors,
                                x.deviceId as deviceId,
                                x.deviceIndex as deviceIndex
                            ))
                        """)
                ).otherwise(lit([]))
            )
            .withColumn(
                "basicInfo",
                struct(
                    col("deviceEntryList"),
                    col("Body.smsProperties.device.machineId").alias("machineId")
                )
            )
            .withColumn(
                "capability",
                struct(
                    col("Body.smsProperties.device.capability.function.security").cast(BooleanType()).alias("security"),
                    col("Body.smsProperties.device.capability.function.poweroff").cast(BooleanType()).alias("poweroff"),
                    col("Body.smsProperties.device.capability.function.serviceReport").cast(BooleanType()).alias("serviceReport"),
                    col("Body.smsProperties.device.capability.function.powerManagement").cast(BooleanType()).alias("powerManagement"),
                    col("Body.smsProperties.device.capability.function.deviceCloning").cast(BooleanType()).alias("deviceCloning"),
                    col("Body.smsProperties.device.capability.function.fss").cast(BooleanType()).alias("fss"),
                    struct(
                        col("Body.smsProperties.device.capability.function.fssDataCloning.available").cast(BooleanType()).alias("available"),
                        col("Body.smsProperties.device.capability.function.fssDataCloning.version")
                    ).alias("fssDataCloning"),
                    struct(
                        col("Body.smsProperties.device.capability.function.fwUpdateOTA.available").cast(BooleanType()).alias("available"),
                        col("Body.smsProperties.device.capability.function.fwUpdateOTA.version")
                    ).alias("fwUpdateOTA"),
                    struct(
                        col("Body.smsProperties.device.capability.function.logData.available").cast(BooleanType()).alias("available"),
                        col("Body.smsProperties.device.capability.function.logData.version")
                    ).alias("logData")
                )
            )
            .withColumn(
                "deviceImage",
                struct(
                    col("Body.smsProperties.device.deviceImage.currentImage.URL").alias("url"),
                    col("Body.smsProperties.device.deviceImage.currentImage.height"),
                    col("Body.smsProperties.device.deviceImage.currentImage.width")
                )
            )
            .withColumn("comm_settings", get_comm_settings_udf(col("Body.smsProperties.interface"), col("ipAddress")))
            .withColumn("macAddress", split(col("comm_settings"), "/").getItem(0))
            .withColumn("subnetMask", split(col("comm_settings"), "/").getItem(2))
            .withColumn("gateway", split(col("comm_settings"), "/").getItem(1))
            .withColumn("ipToLong", ip_to_long_udf(col("ipAddress")))
            .withColumn("dskFlag", when(col("Body.smsProperties.device.configuration").isNotNull(), col("Body.smsProperties.device.configuration.installedOptions.dsk").cast(BooleanType())).otherwise(None))
            .withColumn("detectedDeviceType", when(col("familyName") == lit("PM00"), lit("PM00")).otherwise(col("familyName")))
            .withColumn("createTimestamp", lit(None).cast(LongType()))
            .withColumn("attribute", lit(None).cast(StringType()))
            .withColumn("tagIds", lit(None).cast(ArrayType(StringType())))
            .withColumn("accessMode", lit(None).cast(IntegerType()))
            .withColumn("dispErrorCode", lit(None).cast(IntegerType()))
            .withColumn("optional", lit(None).cast(optional_schema))
            .withColumn("status", lit(None).cast(ArrayType(StringType())))
            .withColumn("statusCode", lit(None).cast(IntegerType()))
            .withColumn("troubleCode", lit(None).cast(IntegerType()))
            .withColumn("lifeCount", lit(None).cast(LongType()))
            .withColumn("serviceCodeList", lit(None).cast(serviceCodeList_schema))
            .withColumn("maintenanceCodeList", lit(None).cast(serviceCodeList_schema))
            .withColumn("lastStatusUpdate", lit(None).cast(LongType()))
            .withColumn("fwUpdateOTASetting", lit(None).cast(fwUpdateOTASetting_schema))
            .withColumn("alertInfo", lit(None).cast(alertInfo_schema))
            .withColumn("interface", lit(None).cast(interFace_schema))
            .select(["deviceId","createTimestamp","type","timestamp","scheduleName","attribute","relatedGroupId","tagIds","communicationStatus","accessMode","relatedAgentId","familyName","friendlyName","modelName","serialNumber","dispErrorCode","optional","ipAddress","ipToLong","macAddress","status","statusCode","troubleCode","lifeCount","serviceCodeList","maintenanceCodeList","location","subnetMask","gateway","deviceImage","dskFlag","description","firmwareVersion","lastBasicUpdate","lastStatusUpdate","detectedDeviceType","productFamilyName","interface","capability","fwUpdateOTASetting","alertInfo"])
        )

# Custom update logic to handle null values in DeviceBasic table (basic-update)
update_set_basic = {
    "deviceId": col("source.deviceId"),
    "type": col("source.type"),
    "relatedGroupId": col("source.relatedGroupId"),
    "modelName": col("source.modelName"),
    "serialNumber": col("source.serialNumber"),
    "communicationStatus": col("source.communicationStatus"),
    "relatedAgentId": col("source.relatedAgentId"),
    "familyName": col("source.familyName"),
    "friendlyName": col("source.friendlyName"),
    "modelName": col("source.modelName"),
    "serialNumber": col("source.serialNumber"),
    "ipAddress": col("source.ipAddress"),
    "ipToLong": col("source.ipToLong"),
    "macAddress": col("source.macAddress"),
    "location": col("source.location"),
    "subnetMask": col("source.subnetMask"),
    "gateway": col("source.gateway"),
    "dskFlag": col("source.dskFlag"),
    "description": col("source.description"),
    "firmwareVersion": col("source.firmwareVersion"),
    "lastBasicUpdate": col("source.lastBasicUpdate"),
    "detectedDeviceType": col("source.detectedDeviceType"),
    "productFamilyName": col("source.productFamilyName"),
    "createTimestamp": coalesce(col("source.createTimestamp"), col("target.createTimestamp")),
    "dispErrorCode": coalesce(col("source.dispErrorCode"), col("target.dispErrorCode")),
    "status": coalesce(col("source.status"), col("target.status")),
    "lifeCount": coalesce(col("source.lifeCount"), col("target.lifeCount")),
    "serviceCodeList": coalesce(col("source.serviceCodeList"), col("target.serviceCodeList")),
    "maintenanceCodeList": coalesce(col("source.maintenanceCodeList"), col("target.maintenanceCodeList")),
    "lastStatusUpdate": coalesce(col("source.lastStatusUpdate"), col("target.lastStatusUpdate"))
}



# COMMAND ----------


# Common Dataframe for the DeviceTonerInfo & DeviceTray_Supplies tables
df_toner_supplies = (df
        .filter(col("Body.scheduleName") == "suppliesUpdate")
        .withColumn("timestamp", col("Body.timestamp"))
        .withColumn("scheduleName",col("Body.scheduleName"))
        .withColumn("deviceId", when(col("Body.mnsn").isNotNull(), col("Body.mnsn")).otherwise(None).cast(StringType()))
        .withColumn("timestamp", col("Body.timestamp"))
        .withColumn("type", lit("mfp"))
        .withColumn("relatedGroupId", when(col("Properties.tenantId").isNotNull(), col("Properties.tenantId")).otherwise(None).cast(StringType()))
        #.withColumn("relatedGroupId",lit("group-1")) #UPDATE REQUIRED
        .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
        .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
        )

# Dataframe for the DeviceTonerInfo table
df_toner_supplies = (df_toner_supplies
            .withColumn("lastTonerUpdate", when(col("Body.timestamp").isNotNull(), col("Body.timestamp")).otherwise(None).cast(LongType()))
            .withColumn("combined_list", when(col("Body.smsProperties.device.disposalMarkerList").isNotNull() & col("Body.smsProperties.device.markerList").isNotNull(), concat(col("Body.smsProperties.device.disposalMarkerList"),col("Body.smsProperties.device.markerList"))).otherwise(array())) 
            .select("*",explode_outer("combined_list").alias("temp_col"))
            .withColumn("markertype", when(col("temp_col").isNotNull() & col("temp_col.type").isNotNull(),col("temp_col.type").cast(StringType())).otherwise(None))
            .withColumn("description", when(col("temp_col").isNotNull() & col("temp_col.description").isNotNull(),col("temp_col.description").cast(StringType())).otherwise(None))
            .withColumn("color", when(col("temp_col").isNotNull() & col("temp_col.color").isNotNull(),col("temp_col.color").cast(StringType())).otherwise(None))
            .withColumn("state", when(col("temp_col").isNotNull() & col("temp_col.amount.state").isNotNull(),col("temp_col.amount.state").cast(StringType())).otherwise(None))
            .withColumn("unit", when(col("temp_col").isNotNull() & col("temp_col.amount.unit").isNotNull(),col("temp_col.amount.unit").cast(StringType())).otherwise(None))
            .withColumn("capacity", when(col("temp_col").isNotNull() & col("temp_col.amount.capacity").isNotNull(),col("temp_col.amount.capacity").cast(StringType())).otherwise(None))
            .withColumn("typical", when(col("temp_col").isNotNull() & col("temp_col.amount.typical").isNotNull(),col("temp_col.amount.typical").cast(StringType())).otherwise(None))
            .select(["deviceId", "type", "timestamp","scheduleName","relatedGroupId", "modelName", "serialNumber", "lastTonerUpdate", "markertype", "description","color","state","unit","capacity","typical"])
        )

# Custom update logic to handle null values in DeviceTonerInfo table
update_set_toner = {
        "deviceId": col("source.deviceId"),
        "type": col("source.type"),
        "relatedGroupId": col("source.relatedGroupId"),
        "modelName": col("source.modelName"),
        "serialNumber": col("source.serialNumber"),
        "lastTonerUpdate": coalesce(col("source.lastTonerUpdate"), col("target.lastTonerUpdate")),
        "markertype": coalesce(col("source.markertype"), col("target.markertype")),
        "description": coalesce(col("source.description"), col("target.description")),
        "color": coalesce(col("source.color"), col("target.color")),
        "state": coalesce(col("source.state"), col("target.state")),
        "unit": coalesce(col("source.unit"), col("target.unit")),
        "capacity": coalesce(col("source.capacity"), col("target.capacity")),
        "typical": coalesce(col("source.typical"), col("target.typical"))
}



# COMMAND ----------


# Temp dataframe for the TraySuppliesInfo table
df_intray = (df
            .withColumn("timestamp", col("Body.timestamp"))
            .withColumn("scheduleName",col("Body.scheduleName"))
            .withColumn("deviceId", when(col("Body.mnsn").isNotNull(), col("Body.mnsn")).otherwise(None).cast(StringType()))
            .withColumn("type", lit("mfp"))
            .withColumn("relatedGroupId", lit("relatedGroupId"))
            .withColumn("lastSuppliesUpdate", when(col("Body.timestamp").isNotNull(), col("Body.timestamp")).otherwise(None).cast(LongType()))
            .withColumn("inTrayList", when(col("Body.smsProperties.device.inTrayList").isNotNull(), col("Body.smsProperties.device.inTrayList")).otherwise(array()))
            .withColumn("temp_intray", explode_outer(col("inTrayList")))
            .withColumn("name", when(col("temp_intray").isNotNull() & col("temp_intray.name").isNotNull(), col("temp_intray.name")).otherwise(None).cast(StringType()))
            .withColumn("id", when(col("temp_intray").isNotNull() & col("temp_intray.id").isNotNull(), col("temp_intray.id")).otherwise(None).cast(StringType()))
            .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
            .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
            .withColumn("description", when(col("temp_intray").isNotNull() & col("temp_intray.description").isNotNull(), col("temp_intray.description")).otherwise(None).cast(StringType()))
            .withColumn("mediaType", when(col("temp_intray").isNotNull() & col("temp_intray.mediaType").isNotNull(), col("temp_intray.mediaType")).otherwise(None).cast(StringType()))
            .withColumn("traymodelName", when(col("temp_intray").isNotNull() & col("temp_intray.modelName").isNotNull(), col("temp_intray.modelName")).otherwise(None).cast(StringType()))
            .withColumn("state", when(col("temp_intray").isNotNull() & col("temp_intray.amount.state").isNotNull(), col("temp_intray.amount.state")).otherwise(None).cast(StringType()))
            .withColumn("unit", when(col("temp_intray").isNotNull() & col("temp_intray.amount.unit").isNotNull(), col("temp_intray.amount.unit")).otherwise(None).cast(StringType()))
            .withColumn("capacity", when(col("temp_intray").isNotNull() & col("temp_intray.amount.capacity").isNotNull(), col("temp_intray.amount.capacity")).otherwise(None).cast(StringType()))
            .withColumn("typical", when(col("temp_intray").isNotNull() & col("temp_intray.amount.typical").isNotNull(), col("temp_intray.amount.typical")).otherwise(None).cast(StringType()))
            .withColumn("mediaSizeHeight", when(col("temp_intray").isNotNull() & col("temp_intray.mediaSizeHeight").isNotNull(), col("temp_intray.mediaSizeHeight")).otherwise(None).cast(StringType()))
            .withColumn("mediaSize", when(col("temp_intray").isNotNull() & col("temp_intray.mediaSize").isNotNull(), col("temp_intray.mediaSize")).otherwise(None).cast(StringType()))
            .withColumn("mediaSizeWidth", when(col("temp_intray").isNotNull() & col("temp_intray.mediaSizeWidth").isNotNull(), col("temp_intray.mediaSizeWidth")).otherwise(None).cast(StringType()))
            .withColumn("mediaSizeUnit", when(col("temp_intray").isNotNull() & col("temp_intray.mediaSizeUnit").isNotNull(), col("temp_intray.mediaSizeUnit")).otherwise(None).cast(StringType()))
            .withColumn("virtual", when(col("temp_intray").isNotNull() & col("temp_intray.virtual").isNotNull(), col("temp_intray.virtual")).otherwise(None).cast(StringType()))
            .withColumn("inserter", when(col("temp_intray").isNotNull() & col("temp_intray.inserter").isNotNull(), col("temp_intray.inserter")).otherwise(None).cast(StringType()))
            .withColumn("mediaSizeName", when(col("temp_intray").isNotNull() & col("temp_intray.mediaSizeName").isNotNull(), col("temp_intray.mediaSizeName")).otherwise(None).cast(StringType()))
            .withColumn("manual", when(col("temp_intray").isNotNull() & col("temp_intray.manual").isNotNull(), col("temp_intray.manual")).otherwise(None).cast(StringType()))
            .withColumn("mediaName", when(col("temp_intray").isNotNull() & col("temp_intray.mediaName").isNotNull(), col("temp_intray.mediaName")).otherwise(None).cast(StringType()))
            .withColumn("mediaDimFeed", when(col("temp_intray").isNotNull() & col("temp_intray.mediaDimFeed").isNotNull(), col("temp_intray.mediaDimFeed")).otherwise(None).cast(StringType()))
            .withColumn("mediaDimXFeed", when(col("temp_intray").isNotNull() & col("temp_intray.mediaDimXFeed").isNotNull(), col("temp_intray.mediaDimXFeed")).otherwise(None).cast(StringType()))
            .withColumn("deliveryOrientation", lit(None).cast(StringType()))
            .select(["deviceId","type","timestamp","scheduleName","relatedGroupId","modelName","serialNumber","lastSuppliesUpdate","name","id","description","mediaType","traymodelName","state","unit","capacity","typical","mediaSizeHeight","mediaSize","mediaSizeWidth","mediaSizeUnit","virtual","inserter","mediaSizeName","manual","mediaName","mediaDimFeed","mediaDimXFeed","deliveryOrientation"])
        )

# Temp dataframe for the TraySuppliesInfo table
df_outtray = (df
            .withColumn("timestamp", col("Body.timestamp"))
            .withColumn("scheduleName",col("Body.scheduleName"))
            .withColumn("deviceId", when(col("Body.mnsn").isNotNull(), col("Body.mnsn")).otherwise(None).cast(StringType()))
            .withColumn("type", lit("mfp"))
            .withColumn("relatedGroupId",lit("relatedGroupId"))
            .withColumn("lastSuppliesUpdate", when(col("Body.timestamp").isNotNull(), col("Body.timestamp")).otherwise(None).cast(LongType()))
            .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))            
            .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
            .withColumn("outTrayList", when(col("Body.smsProperties.device.outTrayList").isNotNull(), col("Body.smsProperties.device.outTrayList")).otherwise(array()))
            .withColumn("temp_intray", explode_outer(col("outTrayList")))
            .withColumn("name", when(col("temp_intray").isNotNull() & col("temp_intray.name").isNotNull(), col("temp_intray.name")).otherwise(None).cast(StringType()))
            .withColumn("id", when(col("temp_intray").isNotNull() & col("temp_intray.id").isNotNull(), col("temp_intray.id")).otherwise(None).cast(StringType()))
            .withColumn("description", when(col("temp_intray").isNotNull() & col("temp_intray.description").isNotNull(), col("temp_intray.description")).otherwise(None). cast(StringType()))
            .withColumn("traymodelName", when(col("temp_intray").isNotNull() & col("temp_intray.modelName").isNotNull(), col("temp_intray.modelName")).otherwise(None).cast(StringType()))
            .withColumn("state", when(col("temp_intray").isNotNull() & col("temp_intray.amount.state").isNotNull(), col("temp_intray.amount.state")).otherwise(None).cast(StringType()))
            .withColumn("unit", when(col("temp_intray").isNotNull() & col("temp_intray.amount.unit").isNotNull(), col("temp_intray.amount.unit")).otherwise(None).cast(StringType()))
            .withColumn("capacity", when(col("temp_intray").isNotNull() & col("temp_intray.amount.capacity").isNotNull(), col("temp_intray.amount.capacity")).otherwise(None).cast(StringType()))
            .withColumn("typical", when(col("temp_intray").isNotNull() & col("temp_intray.amount.typical").isNotNull(), col("temp_intray.amount.typical")).otherwise(None).cast(StringType()))
            .withColumn("deliveryOrientation", when(col("temp_intray").isNotNull() & col("temp_intray.deliveryOrientation").isNotNull(), col("temp_intray.deliveryOrientation")).otherwise(None).cast(StringType()))
            .withColumn("mediaType", lit(None).cast(StringType()))
            .withColumn("mediaSizeHeight", lit(None).cast(StringType()))
            .withColumn("mediaSize", lit(None).cast(StringType()))
            .withColumn("mediaSizeWidth", lit(None).cast(StringType()))
            .withColumn("mediaSizeUnit", lit(None).cast(StringType()))
            .withColumn("virtual", lit(None).cast(StringType()))
            .withColumn("inserter", lit(None).cast(StringType()))
            .withColumn("mediaSizeName", lit(None).cast(StringType()))
            .withColumn("manual", lit(None).cast(StringType()))
            .withColumn("mediaName", lit(None).cast(StringType()))
            .withColumn("mediaDimFeed", lit(None).cast(StringType()))
            .withColumn("mediaDimXFeed", lit(None).cast(StringType()))
            .select(["deviceId","type","timestamp","scheduleName","relatedGroupId","modelName","serialNumber","lastSuppliesUpdate","name","id","description","mediaType","traymodelName","state","unit","capacity","typical","mediaSizeHeight","mediaSize","mediaSizeWidth","mediaSizeUnit","virtual","inserter","mediaSizeName","manual","mediaName","mediaDimFeed","mediaDimXFeed","deliveryOrientation"])
            )
# Dataframe for the TraySuppliesInfo table
# combine both temp dataframes
df_combined_supplies = df_intray.union(df_outtray)

# Custom update logic to handle null values in TraySuppliesInfo table
update_set_traysupply = {
"deviceId": col("source.deviceId"),
"type": col("source.type"),
"relatedGroupId": col("source.relatedGroupId"),
"modelName": col("source.modelName"),
"serialNumber": col("source.serialNumber"),
"lastSuppliesUpdate": coalesce(col("source.lastSuppliesUpdate"), col("target.lastSuppliesUpdate")),
"name": coalesce(col("source.name"), col("target.name")),
"id": coalesce(col("source.id"), col("target.id")),
"description": coalesce(col("source.description"), col("target.description")),
"mediaType": coalesce(col("source.mediaType"), col("target.mediaType")),
"traymodelName": coalesce(col("source.traymodelName"), col("target.traymodelName")),
"state": coalesce(col("source.state"), col("target.state")),
"unit": coalesce(col("source.unit"), col("target.unit")),
"capacity": coalesce(col("source.capacity"), col("target.capacity")),
"typical": coalesce(col("source.typical"), col("target.typical")),
"mediaSizeHeight": coalesce(col("source.mediaSizeHeight"), col("target.mediaSizeHeight")),
"mediaSize": coalesce(col("source.mediaSize"), col("target.mediaSize")),
"mediaSizeWidth": coalesce(col("source.mediaSizeWidth"), col("target.mediaSizeWidth")),
"mediaSizeUnit": coalesce(col("source.mediaSizeUnit"), col("target.mediaSizeUnit")),
"virtual": coalesce(col("source.virtual"), col("target.virtual")),
"inserter": coalesce(col("source.inserter"), col("target.inserter")),
"mediaSizeName": coalesce(col("source.mediaSizeName"), col("target.mediaSizeName")),
"manual": coalesce(col("source.manual"), col("target.manual")),
"mediaName": coalesce(col("source.mediaName"), col("target.mediaName")),
"mediaDimFeed": coalesce(col("source.mediaDimFeed"), col("target.mediaDimFeed")),
"mediaDimXFeed": coalesce(col("source.mediaDimXFeed"), col("target.mediaDimXFeed")),
"deliveryOrientation": coalesce(col("source.deliveryOrientation"), col("target.deliveryOrientation"))
}


# COMMAND ----------


DEVICE_OFFLINE = "offline";

COMMUNICATIONRESULT_OK = "0000";
COMMUNICATIONRESULT_MFP_NO_RESPONSE = "0301";
COMMUNICATIONRESULT_MFP_INVALID = "0303"; 
COMMUNICATIONRESULT_MFP_UNKNOWN_DEVICETYPE = "0304";

# Insert the Record based on deviceId schedule Updates
def insert_to_deviceLog_delta_table(batch_df, batch_id):
    # Define a function to process each schedule name
    try:
        # Define the table name based on the schedule name
        table_name = f"default.deviceLog"  # Adjust this according to your naming convention

        # Load the Delta table
        deltaTable = DeltaTable.forName(spark, table_name)
        print(f"Table {table_name} exists and is a Delta table.")

       # Append all records from the source DataFrame to the target Delta table
        batch_df.write.format("delta").mode("append").save(table_name)

    except Exception as e:
        print(f"Table {table_name} not available. Creating a new Delta table.")
        batch_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name)


# Define UDF to generate UUID
def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

df_deviceLog = (df
                .withColumn("type", lit("mib"))
                .withColumn("timestamp", col("Body.timestamp"))
                .withColumn("targetGroupId", lit("relatedGroupId"))
                .withColumn("operation", col("Body.scheduleName"))
                .withColumn("result", lit("success"))
                .withColumn("modelName", when(col("Body.smsProperties.device.modelName").isNotNull(), col("Body.smsProperties.device.modelName")).otherwise(None).cast(StringType()))
                .withColumn("serialNumber", when(col("Body.smsProperties.device.serialId").isNotNull(), col("Body.smsProperties.device.serialId")).otherwise(None).cast(StringType()))
                .withColumn("ipAddress", when(col("Body.smsProperties.device.address").isNotNull(), col("Body.smsProperties.device.address")).otherwise(None).cast(StringType()))
                .withColumn("location", when(col("Body.smsProperties.device.location.address").isNotNull(), col("Body.smsProperties.device.location.address")).otherwise(None).cast(StringType()))
                .withColumn("firmwareVersion", when(col("Body.smsProperties.device.firmwareVersion").isNotNull(), col("Body.smsProperties.device.firmwareVersion")).otherwise(None).cast(StringType()))
                .withColumn("customName", when(col("Body.smsProperties.device.friendlyName").isNotNull(), col("Body.smsProperties.device.friendlyName")).otherwise(None).cast(StringType()))
                .withColumn("logId", uuid_udf())
                .withColumn(
                    "itemList", 
                    F.array(
                        F.struct(
                            F.lit("code").alias("item"),
                            F.when(F.col("Body.type") == "DEVICE_OFFLINE", F.lit(COMMUNICATIONRESULT_MFP_NO_RESPONSE))
                            .otherwise(F.lit(COMMUNICATIONRESULT_OK)).alias("value")
                        )
                    )
                )    
                .select(["type","timestamp","targetGroupId","modelName","serialNumber","operation","result","ipAddress","firmwareVersion","customName","logId","itemList"])
                )

df_deviceLog = (df_deviceLog
      .writeStream
      .foreachBatch(insert_to_deviceLog_delta_table)
      .option("checkpointLocation", deviceLog_checkpoint_path)  # Ensure the checkpoint path is properly set
      .trigger(availableNow=True)
      .start()
      .awaitTermination()
     )


df_combined_supplies = (df_combined_supplies
                        .writeStream
                        .foreachBatch(lambda batch_df, batch_id: upsert_to_delta_table(batch_df, batch_id,"devicetrayinfo", update_set_traysupply))
                        .option("checkpointLocation", tray_checkpoint_path)  # Ensure the checkpoint path is properly set
                        .trigger(availableNow=True)
                        .start()
                        .awaitTermination()
                    )
df_toner_supplies = (df_toner_supplies
                        .writeStream
                        .foreachBatch(lambda batch_df, batch_id: upsert_to_delta_table(batch_df, batch_id, "devicetonerinfo", update_set_toner))
                        .option("checkpointLocation", toner_checkpoint_path)  # Ensure the checkpoint path is properly set
                        .trigger(availableNow=True)
                        .start()
                        .awaitTermination()
                    )

df_basicUpdate = (df_basicUpdate
                    .writeStream
                    .foreachBatch(lambda batch_df, batch_id: upsert_to_delta_table(batch_df, batch_id, "devicecounterinfo", update_set_basic))
                    .option("checkpointLocation", basicStatus_checkpoint_path)  # Ensure the checkpoint path is properly set
                    .trigger(availableNow=True)
                    .start()
                    .awaitTermination()
                )

df_counterUpdate = (
    df_counterUpdate
    .writeStream
    .foreachBatch(lambda batch_df, batch_id: upsert_to_delta_table(batch_df, batch_id, "counterUpdate", counter_update_set))
    .option("checkpointLocation", counter_checkpoint_path)  # Ensure the checkpoint path is properly set
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

df_deviceStatus = (df_deviceStatus
      .writeStream
      .foreachBatch(lambda batch_df, batch_id: upsert_to_delta_table(batch_df, batch_id, "devicecounterinfo", deviceStatus_update_set))
      .option("checkpointLocation", deviceStatus_checkpoint_path)  # Ensure the checkpoint path is properly set
      .trigger(availableNow=True)
      .start()
      .awaitTermination()
)

