package com.qubole.hive.kinesis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class HiveKinesisStorageHandler extends DefaultStorageHandler {
    
    private static final Log LOG = LogFactory.getLog(HiveKinesisRecordReader.class);

    private Configuration config;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HiveKinesisInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveKinesisOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return LazySimpleSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return null;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() {
        return null;
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {

        Properties tableProps = tableDesc.getProperties();
        for (String key : tableProps.stringPropertyNames()) {
            if (config == null || config.get(key) == null) {
                jobProperties.put(key, tableProps.getProperty(key));
            }
        }
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void setConf(Configuration conf) {
        this.config = conf;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }
}
