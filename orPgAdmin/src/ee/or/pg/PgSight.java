package ee.or.pg;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;

/**
 * @author or 26.02.2019
 * */
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.w3c.dom.Node;

import ee.or.is.*;
import ee.or.db.*;

public class PgSight extends Sight 
{
	private static final long serialVersionUID = 1L;
//	private ArrayList<PgView> aViews;
	Config aServers = null;
	Node aNodeServer = null;

	private ArrayList<PgConnection> aDbs;
	public ArrayList<PgConnection> getConnections(){ return aDbs;}
	public static char cSeparator = '/';
	
	public PgSight( ISServlet aServlet, HttpServletRequest aRequest) 
	{
		super();
		setServlet( aServlet);
		if( aRequest.getRemoteUser() != null ){
			User = new User( aRequest);
		}else{
			User = new User();
			User.setName( "test");
		}
		try {
			init();
			setMySight( this, aRequest);
		} catch (MException aE) {
			log( aE);
		}
	}
	public void clearAll() 
	{
//		aViews = null;
		aServers = null;
		aDbs = null;
		aNodeServer = null;
		bShowSystemObjects = false;
		super.clearAll();
	}
	public void setError( String sError)
	{
		int i = sError.indexOf( "ERROR:");
		if( i > 0 ) sError = sError.substring( i+6);
		int j = sError.indexOf( " Call getNextException");
		if( j > 0 ) sError = sError.substring( 0, j);
		super.setError( sError);
	}
	public PgSchema getSchema( String sName){ return PgConnection.getSchema (aDbs, sName);}
	public PgTable getTable( String sName){ return PgConnection.getTable (aDbs, sName);}
	public PgView getView( String sName){ return PgConnection.getView(aDbs, sName);}
	
	private boolean bShowSystemObjects = false;
	public void setShowSystemObjects( boolean bShowSystemObjects) { this.bShowSystemObjects = bShowSystemObjects;}
	public boolean isShowSystemObjects(){	return bShowSystemObjects;}
	
	public int doInputRequest( int iOper, HttpServletRequest request) throws ExceptionIS 
	{
		int iRet = 0;
		return iRet;
	}
	public DOMData doRequest( HttpServletRequest aRequest, HttpServletResponse aResponse) throws ExceptionIS 
	{	
//		loadViews();
	    DOMData aDoc = null;
	    boolean bWaitDownload = false;
	    String sReq = aRequest.getParameter( "REQUEST"); 
	    if( sReq == null );
	    else if(  sReq.equalsIgnoreCase( "setServerMenu") ){
    		int iRet = getReturn( aRequest);
    		String sName = aRequest.getParameter( "NAME");
    		int i = sName.indexOf( PgSight.cSeparator);
    		String sNameDb, sNameAdd = null;
    		if( i > 0 ){
    			sNameDb = sName.substring( 0, i);
    			sNameAdd = sName.substring( ++i);
    		}else {
    			sNameDb = sName;
    		}
    		if( aDbs != null ) for( PgConnection aDb: aDbs) {
				if( aDb.getName().equalsIgnoreCase( sNameDb) ) {
					aDb.setOpened( sNameAdd, iRet > 0 );
					break;
				}
			}
	    }else if(  sReq.equalsIgnoreCase( "ServersMenu") ){
	    	if( aServers == null ) aServers = getServlet().getConfig();
	    	if( aDbs == null ) aDbs = new ArrayList<PgConnection>();
	    	aDoc = PgConnection.getFormXML( aRequest, this);
/*
	    	aDoc = getTemplate();
			if( aServers != null ){
				ArrayList<Node> aServersList = aServers.findChildNodes( aServers.getRootNode(), "database");
				aDoc.addNodes( aDoc.getRootNode(), aServersList);
			}
			int iRet = getReturn( aRequest);
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
						DbDescr aDbDescr = getServlet().getDbDesc( sName);
						PgConnection aDb = new PgConnection( this, aDbDescr);
						aDb.init( aServers);
						if( aDbs == null ) aDbs = new ArrayList<PgConnection>();
						aDbs.add( aDb);
						aDb.init( aServers);
						aDb.loadDatabases( false);
					} catch( Exception aE) {
						log( aE);
						setError( "Connection to server not succeeded");
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
			aDoc.addChildNode( aRoot, "count", getServlet().getDbsSize()); */
	    }else if(  sReq.equalsIgnoreCase( "ServerForm") ){
	    	aDoc = getTemplate();
	    	PgConnection aConnection;
			String sName = aRequest.getParameter( "NAME"); 
			DbDescr aDbDesc = getServlet().getDbDesc( sName);
			if( aDbDesc == null ) {
				aDbDesc = new DbDescr();
			}
			aConnection = new PgConnection( this);
			aConnection.setDbDescr( aDbDesc);
			aNodeServer = aConnection.init( aServers);
			Node aNodeServerNew = aServers.findNodeByAttr( aServers.getRootNode(), "database", "name", sName);
			int iRet = getReturn( aRequest);
			if( iRet == 1 ){
				aConnection.save( aRequest);
				if( aConnection.getId() == 0 ){ // insert new
					if( aNodeServerNew != null ) {
						setError( "Server name must be uniq");
					}else if( sName.isEmpty() ){
						setError( "Server name must not be empty");
					}else{
						aNodeServer = aServers.addChildNode( aServers.getRootNode(), "database");
						aConnection.save( aServers, aNodeServer);
						aConnection.saveSystemSchemas( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemSchemas"));
						aConnection.saveSystemDatabases( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemDatabases"));
						aConnection.saveSystemViews( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemViews"));
						aConnection.saveSystemTables( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemTables"));						
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					}
				}else{ // update
					if( PgConnection.getConnection( aDbs, sName) != null ) {
						setError( "When changing server parameters, the server must not be connected");
					}else if( !(aNodeServer == aNodeServerNew || aNodeServerNew == null) ) {
						setError( "Server name must be uniq");
					}else {
						aConnection.save( aServers, aNodeServer);
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					}
				}
			}else if( iRet == 2 && aNodeServer != null ){ // remove
				aConnection.save( aRequest);
				if( PgConnection.getConnection( aDbs, sName) != null ) {
					setError( "When removing server, the server must not be connected");
				}else {
					aServers.removeNode( aServers.getRootNode(), aNodeServer);
					aDoc.addChildNode( aDoc.getRootNode(), "msg", "Server removed");
					try {
						aServers.writeFile();
					} catch (Throwable aE) {
						log( aE);
					}
					aDbDesc = new DbDescr();
					aConnection = new PgConnection( this);
					aConnection.setDbDescr( aDbDesc);
					aDoc.addChildNode( aDoc.getRootNode(), "ok", 2);
				}
			}else if( iRet == 21 ){ // test connection
				aConnection.save( aRequest);
				try {
					aConnection.testDbConnection();
					aDoc.addChildNode( aDoc.getRootNode(), "msg", "Connection to server succeeded");
				} catch (Exception aE) {
					setError( aE.toString());
					aDoc.addChildNode( aDoc.getRootNode(), "msg", "Connection to server not succeeded");
					log( aE.toString());
				}
			}else if( iRet == 22 ){ // create new
				aNodeServer = null;
			}else if( iRet == 23 ){ // changed show systemObjects	
				setShowSystemObjects( ISServlet.getParameterBoolean( aRequest, "ShowSystemObjects"));
			}else if( iRet == 24 && aNodeServer != null ){ // changed system objects views	
				aConnection.saveSystemViews( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemViews"));
			}else if( iRet == 25 && aNodeServer != null ){ // changed system objects schemas	
				aConnection.saveSystemSchemas( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemSchemas"));
			}else if( iRet == 26 && aNodeServer != null ){ // changed system objects schemas	
				aConnection.saveSystemDatabases( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemDatabases"));
			}else if( iRet == 27 && aNodeServer != null ){ // changed system objects schemas	
				aConnection.saveSystemTables( aServers, aNodeServer, ISServlet.getParameterString( aRequest, "SystemTables"));
			}else{
				aNodeServer = aNodeServerNew;
			}
			Node aNode = aConnection.setXML( aDoc, aDoc.getRootNode());
			if( iRet == 22 ) aDoc.setChildNodeValue( aNode, "id", 0);
	    }else if(  sReq.equalsIgnoreCase( "DatabaseForm") ){
	    	int iRet = getReturn( aRequest);
	    	aDoc = getTemplate();	
	    	String sName = aRequest.getParameter( "full_name");
	    	PgDatabase aDatabase = PgConnection.getDatabase( aDbs, sName);
	    	if( iRet == 22 ){ // upload
	 			if( aRequest instanceof MultipartRequest ){
	 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
					try {
						aDatabase.execSql( aMRequest.getInputStream2(), 3);
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					} catch (Exception aE) {
						log( aE);
						setError( aE.getLocalizedMessage());
					}
	 			}
	 		}
	    	aDatabase.addXML( aDoc, aDoc.getRootNode(), false);
	    	setXmlNames( aDoc, sName, true);
			setCurDataObject( aDatabase);
	    }else if(  sReq.equalsIgnoreCase( "SchemaForm") ){
	    	int iRet = getReturn( aRequest);
	    	aDoc = getTemplate();	
	    	String sName = aRequest.getParameter( "full_name");
	    	PgSchema aSchema = PgConnection.getSchema (aDbs, sName);
	    	if( iRet == 22 ){ // upload
	 			if( aRequest instanceof MultipartRequest ){
	 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
					try {
						aSchema.execSql( aMRequest.getInputStream2(), 3);
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					} catch (Exception aE) {
						log( aE);
						setError( aE.getLocalizedMessage());
					}
	 			}
	 		}
	    	aSchema.addXML( aDoc, aDoc.getRootNode());
	    	setXmlNames( aDoc, sName, true);
			setCurDataObject( aSchema);
	    }else if(  sReq.equalsIgnoreCase( "ViewForm") ){
	    	aDoc = PgView.getFormXML( aRequest, this);
	    }else if(  sReq.equalsIgnoreCase( "ViewsForm") ){	
	    	aDoc = getTemplate();
			String sName = aRequest.getParameter( "full_name");
	    	int iRet = getReturn( aRequest);
	    	if( iRet == 22 ){ // upload
	 			if( aRequest instanceof MultipartRequest ){
	 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
					try {
				    	PgSchema aSchema = PgConnection.getSchema (aDbs, sName);
				    	aSchema.execSql( aMRequest.getInputStream2(), 2);
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					} catch (Exception aE) {
						log( aE);
						setError( aE.getLocalizedMessage());
					}
	 			}
	 		}
	    	Node aNodeViews = aDoc.addChildNode( aDoc.getRootNode(), "views");
	    	PgConnection aConnection = PgConnection.getConnection(aDbs, sName);
			ArrayList<PgView> aViews = PgConnection.getViews( aDbs, sName);
			if( aViews != null )for( PgView aView: aViews){
				if( isShowSystemObjects() || !aConnection.isSystemView( aView)) 
					aView.addXML( aDoc, aNodeViews);
			}
			setXmlNames( aDoc, sName, false);
	    }else if(  sReq.equalsIgnoreCase( "TableForm") ){
	    	aDoc = PgTable.getFormXML( aRequest, this);
	    }else if(  sReq.equalsIgnoreCase( "TablesForm") ){
	    	aDoc = getTemplate();
			String sName = aRequest.getParameter( "full_name");
	    	PgSchema aSchema = PgConnection.getSchema (aDbs, sName);
	    	int iRet = getReturn( aRequest);
	    	if( iRet == 22 ){ // upload
	 			if( aRequest instanceof MultipartRequest ){
	 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
					try {
				    	aSchema.execSql( aMRequest.getInputStream2(), 1);
						aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
					} catch (Exception aE) {
						log( aE);
						setError( aE.getLocalizedMessage());
					}
	 			}
	 		}
	    	Node aNodeTables = aDoc.addChildNode( aDoc.getRootNode(), "tables");
			ArrayList<PgTable> aTables = aSchema.getTables();
			if( aTables != null ) for( PgTable aTable: aTables){
				if( isShowSystemObjects() || !aSchema.getConnection().isSystemTable( aTable)) 
					aTable.addXML( aDoc, aNodeTables);
			}
			setXmlNames( aDoc, sName, false);
 		}else if( sReq.compareToIgnoreCase( "DownloadForm") == 0 ){
 			int iType = ISServlet.getParameterInt( aRequest, "iType");
 			aDoc = getTemplate();
  			String sName = aRequest.getParameter( "full_name");
// 			setXmlNames( aDoc, aDoc.getRootNode(), sName, false);
 			aDoc.addChildNode( aDoc.getRootNode(), "full_name", sName);
 			aDoc.addChildNode( aDoc.getRootNode(), "iType", iType);
 		}else if( sReq.compareToIgnoreCase( "WaitDownload") == 0 ){
 			aDoc = getTemplate();
 			do
 			{
 				try {
					Thread.sleep( 1000);
				} catch (InterruptedException e) {
				} 
 			}while( bWaitDownload );
 		}else if( sReq.compareToIgnoreCase( "FileDownload") == 0 ){
 			bWaitDownload = true;
 			int iType = ISServlet.getParameterInt( aRequest, "iType");
  			String sName = aRequest.getParameter( "full_name");
  			String sSchema = ISServlet.getParameterStringNull( aRequest, "sSchema");
  			String sOwner = ISServlet.getParameterStringNull( aRequest,  "sOwner");
  			PgView aView = null;
  			PgTable aTable = null;
  			if( iType == 0 ){
  				aView = PgConnection.getView( aDbs, sName);
  				if( aView == null ) aTable = PgConnection.getTable( aDbs, sName);
  			}
  			File aDataFile = null;
			try {
				PgSchema aSchema = PgConnection.getSchema( aDbs, sName);
				if( aView != null ){
					aDataFile = aView.save( this, sSchema, sOwner);
				}else if( aTable != null ){
					aDataFile = aTable.save( this, sSchema, sOwner);
				}else if( iType == 1){
//		  			ArrayList<PgTable> aTables = PgConnection.getTables( aDbs, sName);
					aDataFile = saveTables( PgConnection.getConnection( aDbs, sName), aSchema, sSchema, sOwner);
				}else if( iType == 2){
//		  			ArrayList<PgView> aViews = PgConnection.getViews( aDbs, sName);
					aDataFile = saveViews( PgConnection.getConnection(aDbs, sName), aSchema, sSchema, sOwner);
				}else if( iType == 3){
					PgConnection aConnection = aSchema.getConnection();
					if( !aSchema.isLoaded() ) aSchema.load();
					aDataFile = saveSchema( aConnection, aSchema, sSchema, sOwner);
				}
	  			if( aDataFile != null ){
	  				getServlet().downloadFile( aResponse, aDataFile, "text/plain"); 
	  				return null;
				}
			} catch (Exception aE) {
				log( aE);
			}
			bWaitDownload = false;
		}else if( sReq.compareToIgnoreCase( "SyncForm") == 0 ){
	    	aDoc = getTemplate();
			if( aServers != null ){
				ArrayList<Node> aServersList = aServers.findChildNodes( aServers.getRootNode(), "database");
				aDoc.addNodes( aDoc.getRootNode(), aServersList);
			}
			PgConnection aCon = null;
			String sName = aRequest.getParameter( "full_name"); 
			if( sName != null ){
				aCon = PgConnection.getConnection( aDbs, sName);
				if( aCon == null ) {
					try {
						DbDescr aDbDescr = getServlet().getDbDesc( sName);
						aCon = new PgConnection( this, aDbDescr);
						aCon.init( aServers);
						aCon.loadDatabases( false);
					} catch( Exception aE) {
						log( aE);
						setError( "Connection to server not succeeded");
					}
				} 
			}else {

			}
			int iRet = getReturn( aRequest);
			Node aRoot = aDoc.getRootNode();
			if( aCon != null && aCon.getDbs() != null ) for( PgDatabase aDb: aCon.getDbs()){
				if( isShowSystemObjects() || !aCon.isSystemDatabase( aDb))  aDb.addXML( aDoc, aRoot, false);
			}
			if( aCon != null ) {
				PgDatabase aDb = aCon.getDatabase( sName);
				if( aDb != null ) {
					aDb.load( ( String)null);
					if( aDb.getSchemas() != null )for( PgSchema aSchema: aDb.getSchemas())
						if( isShowSystemObjects() || !aCon.isSystemSchema( aSchema))  aSchema.addXML( aDoc, aRoot);
				}
				PgSchema aSchemaBase = null;
				if( getCurDataObject() instanceof PgDatabase && sName != null ){
					PgDatabase aDatabaseBase = ( PgDatabase)getCurDataObject();
					aDatabaseBase.load( ( String)null);
					String sSchemaName = PgConnection.getSchemaName( sName); 
					aSchemaBase = aDatabaseBase.getSchema( sSchemaName);
				}
				if( getCurDataObject() instanceof PgSchema ) {
					aSchemaBase = ( PgSchema)getCurDataObject();
				}
				if( aSchemaBase != null ){
					aDoc.addChildNode( aDoc.getRootNode(), "base_name", aSchemaBase.getFullName());
					PgSchema aSchema = aCon.getSchema( sName);
					if( aSchema != null ) {
						aDoc.addChildNode( aDoc.getRootNode(), "full_name", aSchema.getFullName());
						if( iRet == 1 ){
							if( !aSchema.isLoaded() ) aSchema.load();
							if( !aSchemaBase.isLoaded() ) aSchemaBase.load();
							aSchemaBase.sync( aSchema);
							aSchemaBase.setSyncXML( aDoc);
						}else if( iRet == 2 ){ // download
							try {
								File aDataFile = aSchemaBase.syncSql( this, aSchema);
					  			if( aDataFile != null ){
					  				getServlet().downloadFile( aResponse, aDataFile, "text/plain"); 
					  				return null;
								}
							} catch( Exception aE) {
								log( aE);
							}
						}else{
							aSchema.clearProblems();
						}
					}
				}
			}
			if( sName != null ) setXmlNames( aDoc, sName, false);
		}
	    if( aDoc == null ) aDoc = super.doRequest( aRequest, aResponse); 
		return aDoc;
	}
    public void setXmlNames( DOMData aDoc, Node aNodeViews, String sName, boolean bAddView) 
    {
    	int i = sName.indexOf( cSeparator);
    	if( i>0 ) {
        	aDoc.addChildNode( aNodeViews, "connection", sName.substring( 0, i)); 
        	int j = sName.indexOf( cSeparator, ++i);
        	if( j > 0 ) {
        		aDoc.addChildNode( aNodeViews, "database", sName.substring( i, j)); 
        		i = j;
        		j = sName.indexOf( cSeparator, ++i);
            	if( j > 0 ) {
            		aDoc.addChildNode( aNodeViews, "schema", sName.substring( i, j));
            		if( bAddView ) {
                		i = j;
                		j = sName.indexOf( cSeparator, ++i);
                    	if( j > 0 ) {
                    		aDoc.addChildNode( aNodeViews, "view", sName.substring( i, j)); 
                    	}else
                    		aDoc.addChildNode( aNodeViews, "view", sName.substring( i));
            		}
            	}else
            		aDoc.addChildNode( aNodeViews, "schema", sName.substring( i)); 
        	}else
        		aDoc.addChildNode( aNodeViews, "database", sName.substring( i)); 
    	}else 
        	aDoc.addChildNode( aNodeViews, "connection", sName); 
	}
    public void setXmlNames( DOMData aDoc, String sName, boolean bAddTable) 
    {
    	Node aNode = aDoc.addChildNode( aDoc.getRootNode(), "navi");
    	int i = sName.indexOf( cSeparator);
    	if( i>0 ) {
        	aDoc.addChildNode( aNode, "connection", sName.substring( 0, i)); 
        	int j = sName.indexOf( cSeparator, ++i);
        	if( j > 0 ) {
        		aDoc.addChildNode( aNode, "database", sName.substring( i, j)); 
        		i = j;
        		j = sName.indexOf( cSeparator, ++i);
            	if( j > 0 ) {
            		aDoc.addChildNode( aNode, "schema", sName.substring( i, j));
            		if( bAddTable ) {
                		i = j;
                		j = sName.indexOf( cSeparator, ++i);
                    	if( j > 0 ) {
                    		aDoc.addChildNode( aNode, "table", sName.substring( i, j)); 
                    	}else
                    		aDoc.addChildNode( aNode, "table", sName.substring( i));
            		}
            	}else
            		aDoc.addChildNode( aNode, "schema", sName.substring( i)); 
        	}else
        		aDoc.addChildNode( aNode, "database", sName.substring( i)); 
    	}else 
        	aDoc.addChildNode( aNode, "connection", sName); 
	}
	public File saveViews( PgConnection aDb, PgSchema aSchema, String sSchema, String sOwner) throws Exception
	{
		String sCatName = isDebug( 98)? getLogCatName(): getLogCatName() + "/" + User.getLogName();
		if( aSchema != null && sCatName != null ){
			String sFileName = sCatName + "/" + "createViews_";
			return saveSchema( aDb, aSchema, sSchema, sOwner, sFileName, 2);
		}
		return null;
	}
	public File saveTables( PgConnection aDb, PgSchema aSchema, String sSchema, String sOwner) throws Exception
	{
		String sCatName = isDebug( 98)? getLogCatName(): getLogCatName() + "/" + User.getLogName();
		if( aSchema != null && sCatName != null ){
			String sFileName = sCatName + "/" + "createTables_";
			return saveSchema( aDb, aSchema, sSchema, sOwner, sFileName, 1);
		}
		return null;
	}
	public File saveSchema( PgConnection aDb, PgSchema aSchema, String sSchema, String sOwner) throws Exception
	{
		String sCatName = isDebug( 98)? getLogCatName(): getLogCatName() + "/" + User.getLogName();
		if( aSchema != null && sCatName != null ){
			String sFileName = sCatName + "/" + "createSchema_";
			return saveSchema( aDb, aSchema, sSchema, sOwner, sFileName, 3);
		}
		return null;
	}
	private File saveSchema( PgConnection aDb, PgSchema aSchema, String sSchema, String sOwner, String sFileName, int iType) throws Exception
	{
		String sFile = sFileName + (( sSchema != null)? sSchema: aSchema.getName());
		PrintStream aPS = GlobalFile.getPrintStream( sFile, false);
		if( ( iType & 1) > 0){
			ArrayList<PgTable> aTables = aSchema.getTables();
			for( PgTable aTable: aTables) {
				if( !aDb.isShowSystemObjects() && !aDb.isSystemTable( aTable) ) {
					aPS.println( aTable.toCreateString( sSchema, sOwner));
				}
			}
			for( PgTable aTable: aTables) {
				if( !aDb.isShowSystemObjects() && !aDb.isSystemTable( aTable) ) {
					if( aTable.hasConstraints() ) aPS.println( aTable.toCreateConstraintsString( sSchema));
				}
			}
		}
		if( ( iType & 2) > 0){
			ArrayList<PgView> aViews = aSchema.getViews();
//			for( PgView aView: aViews) aView.analize();
			ArrayList<PgView> aOViews = PgView.orderViews( aDb, aViews);
			if( aViews.size() > 0 ) log( 98, "Järjestati " + aOViews.size() +"/" + aViews.size());
			for( PgView aView: aOViews) {
				aPS.println( aView.toCreateString( sSchema, sOwner));
				aPS.println();
			}
		}
		aPS.close();
		return new File( sFile);
	}
}
