import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class BiArcRecordReader extends RecordReader<LongWritable, BiArc> {
    protected LineRecordReader reader;
    protected LongWritable key;
    protected BiArc value;

    public BiArcRecordReader() {
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            String[] split = reader.getCurrentValue().toString().split("\\s+");
            if (split.length == 5) {
                key = reader.getCurrentKey();
                // 1 gram: word, year, occur
//                value = new BiArc(split[0]);
            } else {
                // 2-gram: word1, word2, year, occur
                String rawKey = split[0] + "\t" + split[1];
                key = reader.getCurrentKey();
                // 1 gram: word, year, occur
//                value = new BiArc(rawKey, split[2], Long.parseLong(split[3]), true);
            }
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public BiArc getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
