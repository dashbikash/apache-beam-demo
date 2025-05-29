package dashbikash.beamallinone;

import dashbikash.beamallinone.io.LocalIO;


public class BeamAllInOneApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Embedded Processing Started !" );
        
        LocalIO.RunPipeline();
        
    }
}
