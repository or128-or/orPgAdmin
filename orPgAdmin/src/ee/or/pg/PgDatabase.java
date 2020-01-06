package ee.or.pg;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import ee.or.is.DOMData;
import ee.or.is.DataObject;
import ee.or.is.MException;

public class PgDatabase extends DataObject {

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
			if( i > 0 ){
				sNameDb = sName.substring( 0, i);
				sNameAdd = sName.substring( ++i);
			}else {
				sNameDb = sName;
			}
			if( aDbs != null ) for( PgSchema aDb: aDbs){
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
	
	ArrayList<PgSchema> aDbs = null; 
	public ArrayList<PgSchema> getSchemas(){ return aDbs;}
	
	private PgConnection aConnection;
	public PgConnection getConnection(){ 
		aConnection.setDatabaseName( getName());
		return aConnection;
	}
	public String getFullName(){ return  aConnection.getName() + PgSight.cSeparator + getName();}
	
	public PgDatabase( PgConnection aConnection, String sName) throws MException 
	{
		super();
		this.aConnection = aConnection;
		setName( sName);
	}
	public boolean load( /*Database aDb,*/ String sName)
	{
		if( sName == null ) {
			if( aDbs == null ) {
				aDbs = new ArrayList<PgSchema>();
				try {
					ResultSet aRs = aConnection.getSchemas( getName());
		 			if( aRs != null) while( aRs.next() ){
						aDbs.add( new PgSchema( this, aRs.getString( 1)));
			 	    }
		 			aDbs.trimToSize();
				} catch ( Exception aE) {
					aConnection.log( aE);
				}
			}
		}else {
			if( aDbs != null ) for( PgSchema aSchema: aDbs){
				if( aSchema.getName().equalsIgnoreCase( sName) ){
					if( !aSchema.isLoaded() ) aSchema.load(); // aDb);
					break;
				}
			}
		}
		return aDbs != null;
	}
    public void setXML( DOMData aDoc, Node aRoot) 
    {
    	Node aNodeDb = aDoc.addChildNode( aRoot, "db"); 
    	aDoc.addChildNode( aNodeDb, "name", getName()); 
		if( aDbs != null ) {
			(( Element)aNodeDb).setAttribute( "connected", "true");
			if( isOpened() ) (( Element)aNodeDb).setAttribute( "opened", "true");
			if( isChildOpened() ) aDoc.addChildNode( aNodeDb, "opened", true);
			int n = 0;
			for( PgSchema aDb: aDbs){
				if( aConnection.isShowSystemObjects() || !aConnection.isSystemSchema( aDb)){
					aDb.setXML( aDoc, aNodeDb);
					++n;
				}
			}
			aDoc.addChildNode( aNodeDb, "count", n);
		}
	}
    public Node addXML( DOMData aDoc, Node aRoot, boolean bFull) 
    {
    	Node aNodeDb = aDoc.addChildNode( aRoot, "db"); 
    	aDoc.addChildNode( aNodeDb, "name", getName());
    	if( bFull && aDbs != null )	for( PgSchema aDb: aDbs){
			if( aConnection.isShowSystemObjects() || !aConnection.isSystemSchema( aDb)){
				aDb.setXML( aDoc, aNodeDb);
			}
		}
    	return aNodeDb;
    }
    public PgSchema getSchema( String sName)
    {
    	if( sName != null ){
    		int i = sName.indexOf( PgSight.cSeparator);
    		String sNameCon = ( i > 0 )? sName.substring( 0, i): sName;
    		if( aDbs != null ) for( PgSchema aSchema: aDbs) {
    			if( aSchema.getName().equalsIgnoreCase( sNameCon) ) {
    				return aSchema;
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
			if( aDbs != null ) for( PgSchema aSchema: aDbs) {
				if( aSchema.getName().equalsIgnoreCase( sNameCon) ) {
					return aSchema.getView( sNameDb);
				}
			}
		}
		return null;
    }
    public  ArrayList<PgView> getViews( String sName)
    { 
		int i = sName.indexOf( PgSight.cSeparator);
		String sNameCon = ( i > 0 )? sName.substring( 0, i): sName;
		if( aDbs != null ) for( PgSchema aSchema: aDbs) {
			if( aSchema.getName().equalsIgnoreCase( sNameCon) )  return aSchema.getViews();
		}
		return null;
    }
	public void execSql( InputStream aInputStream, int iType) throws Exception
	{	
		StringBuilder aStringBuilder = new StringBuilder();
		String sLine;
		try (BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( aInputStream, "UTF-8"))) {	
			while ((sLine = bufferedReader.readLine()) != null) {
				aStringBuilder.append( sLine);
			}
		}
		PgConnection aDb = getConnection();
		aDb.exec( "SET search_path TO public");
		aDb.execBatch( aStringBuilder.toString());
	}
}
