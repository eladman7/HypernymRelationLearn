import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BiArc implements WritableComparable<BiArc> {

    private String gram; // - The n gram

    public BiArc(String gram) {
        this.gram = gram;
    }

    public BiArc() {
        gram = null;
    }

    public String getGram() {
        return gram;
    }

    @Override
    public int compareTo(BiArc o) {
        return gram.compareTo(o.gram);
    }

    /**
     * DataOutput - hadoop special object which include array of bytes
     * with the output inside.
     * Convert to byte, and sent online or how the fuck we want.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(gram);
    }

    /**
     * Read the n gram that sent. help us restore the object we sent online.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        gram = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BiArc that = (BiArc) o;
        return Objects.equals(gram, that.gram);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gram);
    }
}
