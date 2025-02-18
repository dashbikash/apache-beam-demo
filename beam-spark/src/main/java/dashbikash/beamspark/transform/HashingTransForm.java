package dashbikash.beamspark.transform;

import java.security.MessageDigest;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.common.io.BaseEncoding;

public class HashingTransForm extends DoFn<String, String>{
	private static final long serialVersionUID = 1L;
	@ProcessElement
	public void process(ProcessContext c) throws Exception {
		String[] parts=c.element().split(",");
		
		c.output(String.join(",", parts[0],this.hashing(parts[1]),this.hashing(parts[2]),parts[3],parts[4]));
		
	}
	private String hashing(String in) throws Exception{
		MessageDigest digest=MessageDigest.getInstance("SHA256");
		return BaseEncoding.base16().encode(digest.digest(in.getBytes()));
	}
}
