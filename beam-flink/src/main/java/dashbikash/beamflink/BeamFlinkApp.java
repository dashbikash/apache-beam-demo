package dashbikash.beamflink;

import java.util.Scanner;

import dashbikash.beamflink.io.AwsS3IO;
import dashbikash.beamflink.io.LocalIO;


public class BeamFlinkApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello Beam Flink !" );
        
        AwsS3IO.RunPipeline();
        
    }
}
