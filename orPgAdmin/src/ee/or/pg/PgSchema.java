package ee.or.pg;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import ee.or.db.Database;
import ee.or.is.DOMData;
import ee.or.is.DataObject;
import ee.or.is.GlobalFile;
import ee.or.is.MException;
import ee.or.is.Sight;

public class PgSchema extends DataObject {

	private boolean bOpened = true;
	public boolean isOpened(){	return bOpened;}
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
			if( aViews != null ) for( PgView aView: aViews){
				if( aView.getName().equalsIgnoreCase( sNameDb) ){
					aView.setOpened( sNameAdd, bOpened);
					break;
				}
			} 
		}else
			this.bOpened = bOpened;
	}
	private boolean bChildOpened = true;
	public boolean isChildOpened(){	return bChildOpened;}

	ArrayList<PgView> aViews = null; 
	public ArrayList<PgView> getViews(){ return aViews;}
	public void setViews( ArrayList<PgView> aViews){ this.aViews = aViews;}
	public boolean add( PgView aView)
	{ 
		int i = 0;
		for( PgView aV: aViews){
			if( aView.getName().compareTo( aV.getName()) < 0 ){
				aViews.add( i, aView);
				return true;
			}
			++i;
		}
		aViews.add( aView);
		return true;
	}
	
	ArrayList<PgTable> aTables = null; 
	public ArrayList<PgTable> getTables(){ return aTables;}
	public boolean isLoaded() { return aTables != null;}
	
	ArrayList<PgProblem> aProblems = null; 
	public ArrayList<PgProblem> getProblems(){ return aProblems;}
	public void clearProblems(){ aProblems = null;}
	
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
		aDb.exec( "SET search_path TO " + getName() + ",public");
		aDb.execBatch( aStringBuilder.toString());
		if( (iType & 1) > 0 ){
			aTables = null;
			loadTables( aDb);
		}
		if( (iType & 2) > 0 ){
			aViews = null;
			loadViews( aDb);
		}
	}
	
	private PgDatabase aDatabase;
	public PgConnection getConnection(){ return aDatabase.getConnection();}
	public PgDatabase getDatabase(){ return aDatabase;}
	public String getFullName(){ return aDatabase.getFullName() + PgSight.cSeparator + getName();}

	public boolean isOurObject( PgTable aTable){ 
		PgConnection aConnection = getConnection();
		return aConnection.isShowSystemObjects() || 
			!(aTable.isTable()? aConnection.isSystemTable( aTable): aConnection.isSystemView( ( PgView)aTable));
	}
	private String sOwner;
	public String getOwner(){ return sOwner;}
	public void setOwner( String sOwner){ this.sOwner = sOwner;}

	public PgSchema( PgDatabase aDatabase, String sName) throws MException 
	{
		super();
		this.aDatabase = aDatabase;
		setName( sName);
	}
	public boolean load()
	{
		Database aDb = getConnection();
		aDb.setSchema( getName());
		loadTables( aDb);
		loadViews( aDb);
		return true;
	}
	private boolean loadViews( Database aDb)
	{
		aViews = new ArrayList<PgView>( 10);
		PgView.load( aDb, this); //getName(), aViews);
		PgView.loadViewTables( aDb, this);
		

/*		DbAccess aDbIn = null;
		try {
			aDbIn = new DbAccess( aDb);
			aDbIn.setTable( "pg_views");
			if( aDbIn.select( "SELECT * FROM pg_views WHERE schemaname='" + getName() + "' ORDER BY viewname") ) do{
				String sViewName = aDbIn.getString( "viewname");
				if( sViewName.equalsIgnoreCase( "raster_columns") || sViewName.equalsIgnoreCase( "raster_overviews") || 
					sViewName.equalsIgnoreCase( "geometry_columns") || sViewName.equalsIgnoreCase( "geography_columns")) continue;
//				String sSchemaName = aDbIn.getString( "schemaname");
				String sOwnerName = aDbIn.getString( "viewowner");
				PgView aView = new PgView( getName(), sViewName, sOwnerName);
				String sSql = aDbIn.getString( "definition");
				aView.setSql( sSql);
//				aView.set( aDb, sSql);
				
				aViews.add( aView);	
/*
				log( sSql);
				log( "----------------");
				log( aView.getName());
				log( "");
				log( aView.toString());
				log( "----------------"); /
			}while( aDbIn.next());
/*			ArrayList<PgView> aOViews = PgView.orderViews( aViews);
			if( aViews.size() > 0 ) log( "Järjestati " + aOViews.size() +"/" + aViews.size());
			aViews = aOViews; */
			aViews.trimToSize();
/*		}catch (Exception e) {
			aDb.log( e);
		}finally{
			if( aDbIn != null ) aDbIn.close();
		}*/
		return true; 
	}
	static private String sTables = 
			"SELECT c.oid as id, n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner " +
			"FROM pg_class c " +
			"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace " +
//			"LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace " +
			"WHERE c.relkind = 'r'::\"char\" AND n.nspname='";
//			"select * from pg_tables where schemaname = '";
	private boolean loadTables( Database aDb)
	{
		aTables = new ArrayList<PgTable>( 10);
		try {
			ResultSet aRs = aDb.execSelect( sTables + getName() + "' order by tablename");
			while( aRs.next() ){
				PgTable aTable = new PgTable( this);
				aTable.load( aRs);
				aTable.load( aDb);
				aTables.add( aTable);
			}
			aRs.close();
			aTables.trimToSize();
			return true;
		} catch( Exception aE) {
			aDb.log( aE);
		}
		return false;
	}
   public void setXML( DOMData aDoc, Node aRoot) 
    {
    	PgConnection aConnection = getConnection();
    	Node aNodeSchema= aDoc.addChildNode( aRoot, "schema"); 
    	aDoc.addChildNode( aNodeSchema, "name", getName()); 
		if( aViews != null ) {
			(( Element)aNodeSchema).setAttribute( "connected", "true");
			if( isOpened() ) (( Element)aNodeSchema).setAttribute( "opened", "true");
			if( isChildOpened() ) aDoc.addChildNode( aNodeSchema, "opened", true);
			int n = 0;
			Node aNodeViews = aDoc.addChildNode( aNodeSchema, "views");
			for( PgView aView: aViews){
				if( aConnection.isShowSystemObjects() || !aConnection.isSystemView( aView)) {
					aView.addXML( aDoc, aNodeViews);
					++n;
				}
			}
			aDoc.addChildNode( aNodeViews, "count", n);
			n=0;
			Node aNodeTables = aDoc.addChildNode( aNodeSchema, "tables");
			for( PgTable aTable: aTables){
				if( aConnection.isShowSystemObjects() || !aConnection.isSystemTable( aTable)) {
					aTable.addXML( aDoc, aNodeTables);
					++n;
				}
			}
			aDoc.addChildNode( aNodeTables, "count", n);
		}
	}
    public PgView getView( String sName)
    {
		if( aViews != null ) for( PgView aView: aViews) {
			if( aView.getName().equalsIgnoreCase( sName) ) return aView;
		}
		return null;
    }
    public static PgView getView( String sName, ArrayList<PgView> aViews)
    {
		if( aViews != null ) for( PgView aView: aViews) {
			if( aView.getName().equalsIgnoreCase( sName) ) return aView;
		}
		return null;
    }
    public PgTable getTable( String sName)
    {
		if( aTables != null ) for( PgTable aTable: aTables) {
			if( aTable.getName().equalsIgnoreCase( sName) ) return aTable;
		}
		return null;
    }
    public Node addXML( DOMData aDoc, Node aRoot) 
    {
    	Node aNode = aDoc.addChildNode( aRoot, "schema"); 
    	aDoc.addChildNode( aNode, "name", getName()); 
    	return aNode;
	}
	public boolean sync( PgSchema aSchema) 
	{
		boolean bProblem = false;
		aProblems = new ArrayList<PgProblem>( 10);
		if( aTables != null ) for( PgTable aTable: aTables) {
			if( !isOurObject( aTable)) continue;
			PgProblem aProblem = null;
			String sName = aTable.getName();
			PgTable aTable2 = aSchema.getTable( sName);
			if( aTable2 != null ){
				aProblem = new PgProblem( aTable, '=', aTable2);
				if( aTable.sync( aTable2, aProblem) ){
				}else{
					aProblem.setCase( '!');
					bProblem = true;
				}
			}else{ 
				bProblem = true;
				aProblem = new PgProblem( aTable, '-', null);
			}
			if( aProblem != null ) aProblems.add( aProblem);
		}
		if( aSchema.aTables != null ) for( PgTable aTable: aSchema.aTables) {
			if( !isOurObject( aTable)) continue;
			if( getTable( aTable.getName()) == null ){
				aProblems.add( new PgProblem( null, '-', aTable));
				bProblem = true;
			}
		}
		if( aViews != null ) for( PgView aView: aViews) {
			if( !isOurObject( aView)) continue;
			PgProblem aProblem = null;
			String sName = aView.getName();
			PgView aView2 = aSchema.getView( sName);
			if( aView2 != null ){
				if( aView.sync( aView2, aProblems) ){
					aProblem = new PgProblem( aView, '=', aView2);
				}else{
					bProblem = true;
				}
			}else{ 
				bProblem = true;
				aProblem = new PgProblem( aView, '-', null);
			}
			if( aProblem != null ) aProblems.add( aProblem);
		}
		if( aSchema.aViews != null ) for( PgView aView: aSchema.aViews) {
			if( !isOurObject( aView)) continue;
			if( getView( aView.getName()) == null ){
				aProblems.add( new PgProblem( null, '-', aView));
				bProblem = true;
			}
		}
		aProblems.trimToSize();
		return !bProblem;
	}
	public void setSyncXML(DOMData aDoc) {
		if( aProblems != null ) for( PgProblem aProblem: aProblems) {
			aProblem.setXML( aDoc);
		}
	}
	public File syncSql( Sight aSight, PgSchema aSchema) throws Exception
	{
		String sSchema = aSchema.getName();
		String sOwner = aSchema.getOwner();
		String sCatName = aSight.isDebug( 98)? aSight.getLogCatName(): aSight.getLogCatName() + "/" + aSight.User.getLogName();;
		String sFileName = sCatName + "/" + "addToSchema_" + 
			(( sSchema != null)? sSchema: aSchema.getName()) + ".sql";

		PrintStream aPS = GlobalFile.getPrintStream( sFileName, false);
		if( aProblems != null ) for( PgProblem aProblem: aProblems) {	
			String sLine = aProblem.setSql( sSchema, sOwner);
			if( sLine != null )  aPS.println( sLine);
		}
		if( aProblems != null ) for( PgProblem aProblem: aProblems) {	
			String sLine = aProblem.setSqlConstraint( sSchema);
			if( sLine != null )  aPS.println( sLine);
		}
		setSqlViews( aPS, sSchema, sOwner);
		aPS.close();
		return new File( sFileName);
	}
	public void setSqlViews( PrintStream aPS, String sSchema, String sOwner)
	{
		ArrayList<PgView> aDViews = new ArrayList<PgView>( 10);
		if( aProblems != null ) for( PgProblem aProblem: aProblems) {
			PgTable aTable = aProblem.getTable();
			if( aTable != null && !aProblem.isOK() ){
				if( !aTable.isTable() ){
					PgView aView = ( PgView)aTable; 
					PgView.addDependent( aDViews, aView);
					ArrayList<PgView> aDVs = aView.findDependentViews( getConnection(), getViews());
					for( PgView aDV: aDVs) PgView.addDependent( aDViews, aDV);				
				}
			}
		}
		if( aDViews.size() > 0 ){
			for( PgView aView: aDViews) aView.analize();
			ArrayList<PgView> aOViews = PgView.orderViews( getConnection(), aDViews);
			if( aDViews.size() > 0 )  getConnection().log( 98, "Järjestati " + aOViews.size() +"/" + aDViews.size());
			for( int i = aDViews.size(); --i >= 0; ) {
				PgView aDView = aDViews.get( i);
				aPS.println( "DROP VIEW IF EXISTS " + aDView.getName() + " CASCADE;");
			}
			aPS.println();
			for( PgView aView: aOViews) {
				aPS.println( aView.toCreateString( sSchema, sOwner));
				aPS.println();
			}
		}
	}
}
