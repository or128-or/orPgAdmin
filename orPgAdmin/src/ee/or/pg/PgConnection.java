package ee.or.pg;

import java.sql.ResultSet;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import ee.or.db.Database;
import ee.or.db.DbDescr;
import ee.or.is.Config;
import ee.or.is.DOMData;
import ee.or.is.ExceptionIS;
import ee.or.is.GlobalData;
import ee.or.is.ISServlet;
import ee.or.is.MException;
import ee.or.is.Sight;

public class PgConnection extends Database {
	private static final long serialVersionUID = 1L;

	private boolean bOpened = false;
	public boolean isOpened(){	return bOpened;}
	public void setOpened( boolean bOpened){ this.bOpened = bOpened;}
	public void setOpened( String sName, boolean bOpened){ 
		if( sName != null ){
			if( sName.length() == 0 ){
				this.bChildOpened = bOpened;
				return;
			}
			int i = sName.indexOf( PgSight.cSeparator);
			String sNameDb, sNameAdd = null;
			if( i >= 0 ){
				sNameDb = sName.substring( 0, i);
				sNameAdd = sName.substring( ++i);
			}else {
				sNameDb = sName;
			}
			if( aDbs != null ) for( PgDatabase aDb: aDbs){
				if( aDb.getName().equalsIgnoreCase( sNameDb) ){
					aDb.setOpened( sNameAdd, bOpened);
					break;
				}
			}
		}else
			this.bOpened = bOpened;
	}
	
	private boolean bChildOpened = false;
	public boolean isChildOpened(){	return bChildOpened;}
	public void setChildOpened( boolean bOpened){ this.bChildOpened = bOpened;}
	
	ArrayList<PgDatabase> aDbs = null; 
	public ArrayList<PgDatabase> getDbs(){ return aDbs;}
	
	private ArrayList<String> aSystemDatabases = null; 
	public ArrayList<String> getSystemDatabases(){	return aSystemDatabases;}
	
	private ArrayList<String> aSystemSchemas = null; 
	public ArrayList<String> getSystemSchemas(){	return aSystemSchemas;}
	
	private ArrayList<String> aSystemViews = null; 
	public ArrayList<String> getSystemViews(){	return aSystemViews;}

	private ArrayList<String> aSystemTables = null; 
	public ArrayList<String> getSystemTables(){	return aSystemTables;}

	public boolean isShowSystemObjects(){	return (( PgSight)getSight()).isShowSystemObjects();}
	
	private int iId = 0;
	public int getId(){ return iId;}
	
	public PgConnection( Sight aSight) throws MException {
		super( aSight);
	}
	public PgConnection( Sight aSight, DbDescr dbdescr) throws MException {
		super( dbdescr, aSight);
	}
	public Node init( Config aServers)
	{
		iId = 0;
		Node aNodeServer = aServers.findNodeByAttr( aServers.getRootNode(), "database", "name", getName());
		if( aNodeServer == null ) return null;
		iId = aServers.getNodeID();
		String sSystemDatabases = aServers.getChildValue( aNodeServer, "system_databases");
		if( sSystemDatabases != null ){
			aSystemDatabases = new ArrayList<String>( 10);
			_parseSystemObjects(  sSystemDatabases, aSystemDatabases);
		}else aSystemDatabases = null;

		String sSystemSchemas = aServers.getChildValue( aNodeServer, "system_schemas"); 
		if( sSystemSchemas != null ){
			aSystemSchemas = new ArrayList<String>( 10);
			_parseSystemObjects(  sSystemSchemas, aSystemSchemas);
		}else aSystemSchemas = null;

		String sSystemViews = aServers.getChildValue( aNodeServer, "system_views"); 
		if( sSystemViews != null ){
			aSystemViews = new ArrayList<String>( 10);
			_parseSystemObjects(  sSystemViews, aSystemViews);
		}else aSystemViews = null;

		String sSystemTables = aServers.getChildValue( aNodeServer, "system_tables"); 
		if( sSystemTables != null ){
			aSystemTables = new ArrayList<String>( 10);
			_parseSystemObjects(  sSystemTables, aSystemTables);
		}else aSystemTables = null;
		return aNodeServer;
	}
	public boolean loadDatabases( boolean bTreeview)
	{
		if( aDbs == null ) aDbs = new ArrayList<PgDatabase>();
		try {
			ResultSet aRs = execSelect( "SELECT datname FROM pg_database ORDER BY datname");
 			if( aRs != null) while( aRs.next() ){
 				PgDatabase aDb = new PgDatabase( this, aRs.getString( 1));
 				if( bTreeview) {
 					aDb.setOpened( true);
 					aDb.setChildOpened( true);
 				}
				aDbs.add( aDb);
	 	    }
 			aDbs.trimToSize();
		} catch ( Exception aE) {
			log( aE);
		}
		return aDbs != null;
	}
	public boolean loadXXX( Database aDb, String sName)
	{
		if( sName == null ) {
			if( aDbs == null ) aDbs = new ArrayList<PgDatabase>();
			try {
				ResultSet aRs = aDb.getDatabases();
	 			if( aRs != null) while( aRs.next() ){
					aDbs.add( new PgDatabase( this, aRs.getString("TABLE_CAT")));
		 	    }
	 			aDbs.trimToSize();
			} catch ( Exception aE) {
				aDb.log( aE);
			}
		}
		return aDbs != null;
	}
    public void setTreeXML( DOMData aDoc, Node aRoot) 
    {
		(( Element)aRoot).setAttribute( "connected", "true");
		if( isOpened() ) (( Element)aRoot).setAttribute( "opened", "true");
		if( isChildOpened() ) aDoc.addChildNode( aRoot, "opened", true);
		int n = 0;
		if( aDbs != null ) for( PgDatabase aDb: aDbs){
			if( isShowSystemObjects() || !isSystemDatabase( aDb)){
				aDb.setXML( aDoc, aRoot);
				++n;
			}
		}
		aDoc.addChildNode( aRoot, "count", n);
	}
	public void loadDatabase( String sName) 
	{
		int i = sName.indexOf( PgSight.cSeparator);
		String sNameDb, sNameAdd = null;
		if( i > 0 ){
			sNameDb = sName.substring( 0, i);
			sNameAdd = sName.substring( ++i);
		}else {
			sNameDb = sName;
		}
		if( aDbs != null ) for( PgDatabase aDb: aDbs){
			if( aDb.getName().equalsIgnoreCase( sNameDb) ){
				setDatabaseName( sNameDb);
				aDb.load( sNameAdd);
				aDb.setOpened( true);
				aDb.setChildOpened( true);
				break;
			}
		}
	}
    public PgDatabase getDatabase( String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			sName = sName.substring( ++i);
			i = sName.indexOf( PgSight.cSeparator);
			if( i > 0 ) sName = sName.substring( 0, i);
		}
		if( aDbs != null ) for( PgDatabase aDb: aDbs) {
			if( aDb.getName().equalsIgnoreCase( sName) ) return aDb;
		}
		return null;
    }
    public PgSchema getSchema( String sName) // siin on täisnimi
    {
    	int i = sName.indexOf( PgSight.cSeparator);
    	if( i > 0 ){
    		return _getSchema( sName.substring( ++i));
		}
		return null;
    }
    private PgSchema _getSchema( String sName) // siin on nimi alates db-st
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgDatabase aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getSchema( sNameDb);
				}
			}
		}
		return null;
    }
    public PgView getView( String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgDatabase aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getView( sNameDb);
				}
			}
		}
		return null;
    }
    public static PgConnection getConnection( ArrayList<PgConnection> aDbs, String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		String sNameCon = ( i > 0 )? sName.substring( 0, i): sName;
		if( aDbs != null ) for( PgConnection aDb: aDbs) {
			if( aDb.getName().equalsIgnoreCase( sNameCon) ){
				if( i > 0 ){
					String sNameDb = sName.substring( ++i);
					i = sNameDb.indexOf( PgSight.cSeparator);
					String sNameSc = null;
					if( i > 0 ){
						sNameSc = sNameDb.substring( i+1);
						sNameDb = sNameDb.substring( 0, i);
					}
					aDb.setDatabaseName( sNameDb);
					if( sNameSc != null ){
						i = sNameSc.indexOf( PgSight.cSeparator);
						aDb.setSchema( (i>0)? sNameSc.substring( 0, i): sNameSc);
					}
				}
				return aDb;
			}
		}
		return null;
    }
    public static int getConnectionId( ArrayList<PgConnection> aDbs, String sName)
    {
		int iId = 0;
		int n = aDbs.size();
    	int i = sName.indexOf( PgSight.cSeparator);
		String sNameCon = ( i > 0 )? sName.substring( 0, i): sName;
		if( aDbs != null ) for( PgConnection aDb: aDbs) { 
			if( aDb.getName().equalsIgnoreCase( sNameCon) ) break;
			++iId;
		}
		return (iId<n)? iId: -1;
    }
    public static PgDatabase getDatabase( ArrayList<PgConnection> aDbs, String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getDatabase( sNameDb);
				}
			}
		}
		return null;
   }
   public static PgSchema getSchema( ArrayList<PgConnection> aDbs, String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb._getSchema( sNameDb);
				}
			}
		}
		return null;
    }
    public static PgView getView( ArrayList<PgConnection> aDbs, String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getView( sNameDb);
				}
			}
		}
		return null;
    }
    public  ArrayList<PgView> getViews( String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgDatabase aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getViews( sNameDb);
				}
			}
		}
		return null;
    }
    public static  ArrayList<PgView> getViews( ArrayList<PgConnection> aDbs, String sName)
    {
		int i = sName.indexOf( PgSight.cSeparator);
		if( i > 0 ){
			String sNameCon = sName.substring( 0, i);
			String sNameDb = sName.substring( ++i);
			if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
					return aDb.getViews( sNameDb);
				}
			}
		}
		return null;
    }
    public static  ArrayList<PgTable> getTables( ArrayList<PgConnection> aDbs, String sName)
    {
    	PgSchema aSchema = PgConnection.getSchema( aDbs, sName);
		return ( aSchema != null )? aSchema.getTables(): null;
    }
    public static  PgTable getTable( ArrayList<PgConnection> aDbs, String sName)
    {
    	PgSchema aSchema = PgConnection.getSchema( aDbs, sName);
		return ( aSchema != null )? aSchema.getTable( getTableName( sName)): null;
    }
    public static boolean add( ArrayList<PgConnection> aDbs, String sName, PgView aView)
    {
    	PgSchema aSchema = PgConnection.getSchema( aDbs, sName);
    	return ( aSchema != null )? aSchema.add( aView): false;
    }
    public Node setXML( DOMData aDoc, Node aRoot) 
    {
    	aRoot = aDoc.addChildNode( aRoot, "server");
    	DbDescr aDbDescr = getDbDescr();
		aDoc.addChildNode( aRoot, "id", iId);
		aDoc.addChildNode( aRoot, "NAME", aDbDescr.getDbclass());
		aDoc.addChildNode( aRoot, "db_name", aDbDescr.getDbname());
		aDoc.addChildNode( aRoot, "url", aDbDescr.getDbhost());
		aDoc.addChildNode( aRoot, "user", aDbDescr.getDbuser());
		aDoc.addChildNode( aRoot, "psw", aDbDescr.getPSW());
		aDoc.addChildNode( aRoot, "type", aDbDescr.getDbtypestr());
		aDoc.addChildNode( aRoot, "driver", aDbDescr.getDrivername());
		aDoc.addChildNode( aRoot, "srid", aDbDescr.getProjection());
		aDoc.addChildNode( aRoot, "encoding", aDbDescr.getEncoding());
		aDoc.addChildNode( aRoot, "ShowSystemObjects", (( PgSight)getSight()).isShowSystemObjects());
		if( aSystemDatabases != null ) aDoc.addChildNode( aRoot, "SystemDatabases", GlobalData.toString( aSystemDatabases));
		if( aSystemSchemas != null ) aDoc.addChildNode( aRoot, "SystemSchemas", GlobalData.toString( aSystemSchemas));
		if( aSystemViews != null ) aDoc.addChildNode( aRoot, "SystemViews", GlobalData.toString( aSystemViews));
		if( aSystemTables != null ) aDoc.addChildNode( aRoot, "SystemTables", GlobalData.toString( aSystemTables));
		return aRoot;
	}
	public void save( HttpServletRequest aRequest)
	{
		DbDescr aDescr = getDbDescr();
		String sName = ISServlet.getParameterString( aRequest, "NAME");
		aDescr.setDbclass( sName);
		
		String sDbName = ISServlet.getParameterString( aRequest, "db_name");
		aDescr.setDbname( sDbName);

		String sUrl = ISServlet.getParameterString( aRequest, "url");
		aDescr.setDbhost( sUrl);
		
		String sUser = ISServlet.getParameterString( aRequest, "user");
		aDescr.setDbuser( sUser);
		
		String sUserPsw = ISServlet.getParameterString( aRequest, "psw");
		aDescr.setDbpasswd( sUserPsw);
		
		String sType = ISServlet.getParameterString( aRequest, "type");
		aDescr.setDbtypestr( sType);
		
		String sDriver = ISServlet.getParameterString( aRequest, "driver");
		aDescr.setDrivername( sDriver);
		
		String sProjection = ISServlet.getParameterString( aRequest, "srid");
		aDescr.setDbGeometry( sProjection);
		
		String sEncoding = ISServlet.getParameterString( aRequest, "encoding");
		aDescr.setEncoding( sEncoding);
	}
	public void save( Config aDoc, Node aNodeServer)
	{
		DbDescr aDbDescr = getDbDescr();
		(( Element)aNodeServer).setAttribute( "name", aDbDescr.getDbclass());
		(( Element)aNodeServer).setAttribute( "dbname", aDbDescr.getDbname());
		(( Element)aNodeServer).setAttribute( "host", aDbDescr.getDbhost());
		(( Element)aNodeServer).setAttribute( "user", aDbDescr.getDbuser());
		(( Element)aNodeServer).setAttribute( "passwd", aDbDescr.getPSW());
		(( Element)aNodeServer).setAttribute( "typestr", aDbDescr.getDbtypestr());
		(( Element)aNodeServer).setAttribute( "drivername", aDbDescr.getDrivername());
		(( Element)aNodeServer).setAttribute( "srid", aDbDescr.getDbGeometry());
		(( Element)aNodeServer).setAttribute( "encoding", aDbDescr.getEncoding());
		try {
			aDoc.writeFile();
			aDoc.reloadDatabaseConf();
		} catch (Throwable aE) {
			log( aE);
		}
		if( iId == 0 ){
			aDoc.findNodeByAttr( aDoc.getRootNode(), "database", "name", getName());
			iId = aDoc.getNodeID();
		}
	}
    public static String trimToSchema( String sName) 
    {
    	int i = sName.indexOf( PgSight.cSeparator);
    	if( i>0 ) {
        	int j = sName.indexOf( PgSight.cSeparator, ++i);
        	if( j > 0 ) {
        		i = j;
        		j = sName.indexOf( PgSight.cSeparator, ++i);
            	return ( j > 0 )? sName.substring( 0, j): sName; 
        	}
    	}
    	return null;
	}
    public static String getSchemaName( String sName) 
    {
    	int i = sName.indexOf( PgSight.cSeparator);
    	if( i>0 ) {
        	int j = sName.indexOf( PgSight.cSeparator, ++i);
        	if( j > 0 ) {
        		i = j;
        		j = sName.indexOf( PgSight.cSeparator, ++i);
            	return ( j > 0 )? sName.substring( i, j): sName.substring( i); 
        	}
    	}
    	return null;
	}
    public static String getViewName( String sName) 
    {
    	int i = sName.indexOf( PgSight.cSeparator);
    	if( i>0 ) {
        	int j = sName.indexOf( PgSight.cSeparator, ++i);
        	if( j > 0 ) {
        		i = j;
        		j = sName.indexOf( PgSight.cSeparator, ++i);
        		if( j > 0 ) {
        			return sName.substring( ++j);
        		}
        	}
    	}
    	return null;
	}
    public static String getTableName( String sName) 
    {
    	return getViewName( sName);
    }
	private void _parseSystemObjects( String sPar, ArrayList<String>aObjects) 
	{
		if( sPar != null ){
			String [] as = sPar.split( ",");
			for( String s: as){
				aObjects.add( s.trim());
			}
			aObjects.trimToSize(); 
		}
	}
	private void _saveSystemObjects( Config aDoc, Node aNode, String sPar, ArrayList<String>aObjects, String sName) 
	{
		if( sPar != null ){
			_parseSystemObjects( sPar, aObjects);
		}else{ sPar = "";}
		try {
			aDoc.setChildNodeValue( aNode, sName, sPar);
			aDoc.writeFile();
		} catch (Throwable aE) {
			log( aE);
		}
	}
	public void saveSystemViews( Config aDoc, Node aNode, String sPar) 
	{
		if( sPar != null && sPar.trim().length() > 0 ){
			aSystemViews = new ArrayList<String>( 10);
		}else{ sPar = null; aSystemViews = null;}
		_saveSystemObjects( aDoc, aNode, sPar, aSystemViews, "system_views");
	}
	public void saveSystemSchemas( Config aDoc, Node aNode, String sPar) 
	{
		if( sPar != null && sPar.trim().length() > 0 ){
			aSystemSchemas = new ArrayList<String>( 10);
		}else{ sPar = null; aSystemSchemas = null;}
		_saveSystemObjects( aDoc, aNode, sPar, aSystemSchemas, "system_schemas");
	}
	public void saveSystemDatabases( Config aDoc, Node aNode, String sPar) 
	{
		if( sPar != null && sPar.trim().length() > 0 ){
			aSystemDatabases = new ArrayList<String>( 10);
		}else{ sPar = null; aSystemDatabases = null;}
		_saveSystemObjects( aDoc, aNode, sPar, aSystemDatabases, "system_databases");
	}
	public void saveSystemTables( Config aDoc, Node aNode, String sPar) 
	{
		if( sPar != null && sPar.trim().length() > 0 ){
			aSystemTables = new ArrayList<String>( 10);
		}else{ sPar = null; aSystemTables = null;}
		_saveSystemObjects( aDoc, aNode, sPar, aSystemTables, "system_tables");
	}
	public boolean isSystemDatabase( PgDatabase aDatabase) 
	{
		if( aSystemDatabases != null) for( String sSystemDatabase: aSystemDatabases){
			if( aDatabase.getName().equalsIgnoreCase( sSystemDatabase)) return true;
		}
		return false;
	}
	public boolean isSystemSchema( PgSchema aSchema) 
	{
		if( aSystemSchemas != null) for( String sSystemSchema: aSystemSchemas){
			if( aSchema.getName().equalsIgnoreCase( sSystemSchema)) return true;
		}
		return false;
	}
	public boolean isSystemView( PgView aView) 
	{
		if( aSystemViews != null) for( String sSystemView: aSystemViews){
			if( aView.getName().equalsIgnoreCase( sSystemView)) return true;
		}
		return false;
	}
	public boolean isSystemTable( PgTable aTable) 
	{
		if( aSystemTables != null) for( String sSystemTable: aSystemTables){
			if( aTable.getName().equalsIgnoreCase( sSystemTable)) return true;
		}
		return false;
	}
    public void setXML( DOMData aDoc, Node aRoot, ArrayList<PgView> aViews) 
    {
    	for( PgView aView: aViews){
   			aDoc.addChildNode( aRoot, isSystemView( aView)? "sys_view": "view", aView.getName());
    	}
    }
	public static DOMData getFormXML( HttpServletRequest aRequest, Sight aSight) throws ExceptionIS
    {
    	DOMData aDoc = aSight.getTemplate();
		Config aServers = aSight.getConfig();
		if( aServers != null ){
			ArrayList<Node> aServersList = aServers.findChildNodes( aServers.getRootNode(), "database");
			aDoc.addNodes( aDoc.getRootNode(), aServersList);
		}
		ArrayList<PgConnection> aDbs =  (( PgSight)aSight).getConnections();
		int iRet = Sight.getReturn( aRequest);
		if( iRet == 11 ) {
			Database aOldDb = null;
			String sName = aRequest.getParameter( "NAME"); 
			if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sName) ) {
					aDb.setOpened( true);
					aDb.setChildOpened( true);
					aOldDb = aDb;
					break;
				}
			}
			if( aOldDb == null ) {
				try {
					DbDescr aDbDescr = aSight.getServlet().getDbDesc( sName);
					PgConnection aDb = new PgConnection( aSight, aDbDescr);
					aDb.init( aServers);
					if( aDbs == null ) aDbs = new ArrayList<PgConnection>();
					aDbs.add( aDb);
					aDb.init( aServers);
					aDb.loadDatabases( false);
					aDb.setOpened( true);
					aDb.setChildOpened( true);
				} catch( Exception aE) {
					aSight.log( aE);
					aSight.setError( "Connection to server not succeeded");
				}
			}
		}else if( iRet == 12 ) { // load databases
			String sName = aRequest.getParameter( "NAME"); 
			int i = sName.indexOf( PgSight.cSeparator);
			if( i > 0 ){
				String sNameCon = sName.substring( 0, i);
				String sNameDb = sName.substring( ++i);
				if( aDbs != null ) for( PgConnection aDb: aDbs) {
					if( aDb.getName().equalsIgnoreCase( sNameCon) ) {
						aDb.loadDatabase( sNameDb);
						aDb.setOpened( true);
						aDb.setChildOpened( true);
						break;
					}
				}
			}
		}
		Node aRoot = aDoc.getRootNode();
		if( aServers != null && aDbs != null ) for( PgConnection aDb: aDbs) {
			String sName = aDb.getName();
			ArrayList<Node> aServersList = aDoc.findChildNodes( aRoot, "database");
			for( Node aNode: aServersList) {
				if( (( Element)aNode).getAttribute( "name").equalsIgnoreCase( sName) ){
					aDb.setTreeXML( aDoc, aNode);
				}
			}
		}
		aDoc.addChildNode( aRoot, "count", aSight.getServlet().getDbsSize());
    	return aDoc;
    }
}
