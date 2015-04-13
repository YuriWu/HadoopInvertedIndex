import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class FloatType implements WritableComparable<FloatType> {

	private float value;
	
	public FloatType()
	{
		value = 0.0f;
	}
	
	public FloatType(String content)
	{
		value = Float.parseFloat(content);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(value);
	}

	@Override
	public int compareTo(FloatType o) {
		if(value < o.value)
		{
			return 1;
		}
		else if(value > o.value)
		{
			return -1;
		}
		else return 0;
	}

	@Override
	public String toString() {
		return (value + "");
	}
}

