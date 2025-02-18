package dashbikash.beamspark.transform;

import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.common.io.BaseEncoding;

public class FormatTransform extends DoFn<String, String>{
	private static final long serialVersionUID = 1L;
	@ProcessElement
	public void process(ProcessContext c) throws Exception {
		String[] parts=c.element().split(",");
		c.output(String.join(",", parts[0],parts[1],this.format(parts[2]),parts[3],parts[4]));
	}
	private String format(String str) {
        if (str == null) {
            return null; // Handle null input
        }
        if (str.isEmpty()) {
            return ""; // Handle empty input
        }
        String regex = "[^a-zA-Z0-9\\s+]+(?!^\\+)";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);

        return matcher.replaceAll(""); // Replace all matches with an empty string
    }
}
