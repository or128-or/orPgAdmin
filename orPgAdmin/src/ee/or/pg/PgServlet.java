package ee.or.pg;
/**
 * @author or 26.02.2019
 */
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import ee.or.is.*;

public class PgServlet extends ISServlet 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
    public void init( ServletConfig config) throws ServletException 
    {
        log( "Start of init PgServlet");
        super.init( config);
        log( "End of init PgServlet");
    } 
    public Sight createSight( HttpServletRequest aRequest)
    {
    	return new PgSight( this, aRequest);
    }
    public String getRequest( HttpServletRequest aRequest) 
	{
		String sReq = super.getRequest( aRequest);
		return ( sReq != null)? sReq: "Main";
	}
}
