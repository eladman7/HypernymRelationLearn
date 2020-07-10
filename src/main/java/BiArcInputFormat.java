import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BiArcInputFormat extends FileInputFormat<LongWritable, BiArc> {
    @Override
    public RecordReader<LongWritable, BiArc> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new BiArcRecordReader();
    }
}
