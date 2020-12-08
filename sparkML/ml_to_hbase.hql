create table reid_ML_results_hbase(
    model_key string,
    model string,
    model_num string,
    date_features string,
    rider_features string,
    weather_features string,
    rmse string,
    r2 string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:model,stat:model_num,
stat:date_features,stat:rider_features,stat:weather_features,stat:rmse,stat:r2')
TBLPROPERTIES ('hbase.table.name' = 'reid7_ml_results');

insert overwrite table reid_ML_results_hbase
select model || "-" || model_num, model, model_num, date_features, rider_features, weather_features,
    rmse, r2 from reid_ML_results order by model, model_num;

create table reid_ML_models_hbase (
    model_key string,
    model string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:model')
TBLPROPERTIES ('hbase.table.name' = 'reid7_ml_models');

insert overwrite table reid_ML_models_hbase
select distinct model, model from reid_ML_results;
