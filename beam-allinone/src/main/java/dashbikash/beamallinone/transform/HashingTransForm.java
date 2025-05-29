package dashbikash.beamallinone.transform;

import java.security.MessageDigest;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.common.io.BaseEncoding;

public class HashingTransForm extends DoFn<String, String>{
	@ProcessElement
	public void process(ProcessContext c) throws Exception {
		String[] parts=c.element().split(",");
		StringBuilder sb=new StringBuilder();
		if(!parts[3].equalsIgnoreCase("UK")) {
			sb.append(parts[0]).append(",").append(this.hashing(parts[1])).append(",").append(this.hashing(parts[2])).append(",").append(parts[3]).append(",").append(parts[4]);
		}
		c.output(sb.toString());
	}
	private String hashing(String in) throws Exception{
		MessageDigest digest=MessageDigest.getInstance("SHA256");
		return BaseEncoding.base16().encode(digest.digest(in.getBytes()));
	}
}
