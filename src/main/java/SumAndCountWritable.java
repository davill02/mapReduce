import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumAndCountWritable implements Writable {
    private double count;
    private double sum;
    private double squadSum;


    public SumAndCountWritable() {
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public double getSquadSum() {
        return squadSum;
    }

    public void setSquadSum(double squadSum) {
        this.squadSum = squadSum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sum);
        dataOutput.writeDouble(count);
        dataOutput.writeDouble(squadSum);
    }

    public SumAndCountWritable(double count, double sum, double squadSum) {
        this.count = count;
        this.sum = sum;
        this.squadSum = squadSum;
    }

    public void readFields(DataInput dataInput) throws IOException {
        sum = dataInput.readDouble();
        count = dataInput.readInt();
        squadSum = dataInput.readDouble();
    }
}
