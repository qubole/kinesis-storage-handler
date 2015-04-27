package com.qubole.hive.kinesis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.qubole.hive.kinesis.util.HiveKinesisUtilsTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HiveKinesisUtilsTest.class,
        KinesisShardCheckpointerTest.class,
        HiveKinesisRecordReaderTest.class})
public class TestSuite {
    //nothing
}
