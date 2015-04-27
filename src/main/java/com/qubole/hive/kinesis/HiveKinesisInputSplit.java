package com.qubole.hive.kinesis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

public class HiveKinesisInputSplit extends FileSplit {

    private String streamName;
    private String shardId;
    private Path path;

    public HiveKinesisInputSplit() {
        super((Path) null, 0, 0, new String[0]);
    }

    public HiveKinesisInputSplit(String streamName, String shardId, JobConf conf) {
        super((Path) null, 0, 0, new String[0]);
        path = FileInputFormat.getInputPaths(conf)[0];
        this.streamName = streamName;
        this.shardId = shardId;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getShardId() {
        return shardId;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public long getLength() {
        return 0L;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeUTF(streamName);
        out.writeUTF(shardId);
        out.writeUTF(path.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        streamName = in.readUTF();
        shardId = in.readUTF();
        path = new Path(in.readUTF());
    }
}
