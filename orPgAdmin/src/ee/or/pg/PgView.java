package ee.or.pg;

import java.io.BufferedReader;
import java.io.File;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;


import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.w3c.dom.Node;

import ee.or.db.Database;
import ee.or.db.DbAccess;
import ee.or.is.DOMData;
import ee.or.is.ExceptionIS;
import ee.or.is.GlobalFile;
import ee.or.is.MException;
import ee.or.is.MultipartRequest;
import ee.or.is.Sight;

/**
 * @author or 27.02.2019
 */
public class PgView extends PgTable 
{	
/*	private String sSchemaName;
	public String getSchemaName(){	return sSchemaName;}
	public void setSchemaName( String sSchemaName){	this.sSchemaName = sSchemaName;}
	
	private String sName;
	public String getName(){ return sName;}
	public void setName( String sName){	this.sName = sName;}
*/	
/*	private String sOwner;
	public String getOwner(){ return sOwner;}
	public void setOwner( String sOwner){ this.sOwner = sOwner;} */

	private String sFrom;
	public String getFrom(){ return sFrom;}
	public void setFrom( String sFrom){ this.sFrom = sFrom;}

	private String sWhere;
	public String getWhere(){ return sWhere;}
	public void setWhere( String sWhere){ this.sWhere = sWhere;}

	private String sOrder;
	public String getOrder(){ return sOrder;}
	public void setOrder( String sOrder){ this.sOrder = sOrder;}

	private ArrayList<PgAttribute> aAttributes;
	private ArrayList<PgTable> aTables = null;
	public ArrayList<PgTable> getTables(){	return aTables;}
	
	private ArrayList<PgTable> aViewTables;
//	private ArrayList<String> aViewsIn;

	private String sSql;
	public String getSql(){ return sSql;}
	public void setSql(String sSql){ 
		int n = sSql.length();
		this.sSql = ( sSql.charAt( --n) == ';' )? sSql.substring( 0, n): sSql;
		bChanged = true;
	}
	public void setSql( InputStream aInputStream) throws Exception
	{	
		StringBuilder aStringBuilder = new StringBuilder();
		String sLine;
		try (BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( aInputStream, "UTF-8"))) {	
			boolean bFirst = true, bNewLine = false;
			while ((sLine = bufferedReader.readLine()) != null) {
				if( bNewLine ) aStringBuilder.append( System.lineSeparator());
				if( bFirst && sLine.indexOf( "CREATE") >= 0 ) {
				}else {
					aStringBuilder.append( sLine);
					bNewLine = true;
				}
				bFirst = false;
			}
		}
		setSql( aStringBuilder.toString());
	}
	private boolean bOpened = true;
	public boolean isOpened(){	return bOpened;}
	public void setOpened( String sName, boolean bOpened){ 
/*		if( sName != null ){
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
		}else */
			this.bOpened = bOpened;
	}
	private boolean bChildOpened = true;
	public boolean isChildOpened(){	return bChildOpened;}
		
	private boolean bEdit = true;
	private boolean bChanged = false;
	
/*	private PgSchema aSchema;
	public PgSchema getSchema(){ return aSchema;}
	public String getSchemaName(){	return aSchema.getName();}
*/	
	public PgView( PgSchema aSchema) 
	{
		super( aSchema);
		setTable( false);
//		this.aSchema = aSchema;
	}
	public PgView( PgSchema aSchema, String sName, String sOwner)  // , String sSchemaName
	{
		super( aSchema);
		setTable( false);
//		this.aSchema = aSchema;
//		this.sSchemaName = aSchema.getName();
		setName( sName);
		setOwner( sOwner);
	}
	public void analize()
	{
		analize( sSql);
	}
	public void analize( String sQuery)
	{
		PgConnection aDb = getConnection(); 
//		String sQuery = sSql;
		if( aTables != null ) return;
		aTables = new ArrayList<PgTable>( 10);
		int is = sQuery.indexOf( "SELECT");
		int iFrom = sQuery.indexOf( "FROM");
		int iWhere = sQuery.indexOf( "WHERE", iFrom);
		// see Where üldjuhul pole vist nii lihtne
		int iOrder = sQuery.indexOf( "ORDER BY", iFrom);
		if( is >= 0 && iFrom > is ) {
			String sAttrs = sQuery.substring( is + 6, iFrom);
			int iS = 0;
			aAttributes = null;
			for(;; ) {
				int iE = getNextAttrEnd( sAttrs, iS);
				if( iE < 0 ) break;
				String sAttr = sAttrs.substring( iS, iE);
				String sAttrName, sAttrValue;
				int ia = sAttr.indexOf( " AS ");
				if( ia > 0 ){
					sAttrValue = sAttr.substring( 0, ia).trim();
					sAttrName = sAttr.substring( ia+4).trim();
				}else{
					sAttrValue = sAttr.trim();
					sAttrName = null;
				}
				if( aAttributes == null ) aAttributes = new ArrayList<PgAttribute>( 10);
				aAttributes.add( new PgAttribute( sAttrName, sAttrValue));	
				iS = iE + 1;
			}
			if( iWhere > 0 ) {
				sFrom = sQuery.substring( iFrom+4, iWhere).trim();
				if( iOrder > 0 ){
					sWhere = sQuery.substring( iWhere+5, iOrder).trim();
					sOrder = sQuery.substring( iOrder+8).trim();
				}else{
					sWhere = sQuery.substring( iWhere+5).trim();
					sOrder = null;
				}
			}else if( iOrder > 0 ) {
				sFrom = sQuery.substring( iFrom+4, iOrder).trim();
				sWhere = null;
				sOrder = sQuery.substring( iOrder+8).trim();
			}else{
				sFrom = sQuery.substring( iFrom+4).trim();
				sWhere = null;
				sOrder = null;
			}
			if( sFrom != null ) {
				is = sFrom.indexOf( "SELECT");
				if( is >= 0 ){
					bEdit = false;
					analize( trimBracketsFull( sFrom)); 
				}else{
					int i = sFrom.length();
					if( sFrom.charAt( i-1) == ';' ) sFrom = sFrom.substring( 0, i-1);
					i = sFrom.indexOf( ',');
					if( i>0 ){
						String [] aS = sFrom.split( ",");
						for( String s: aS){
							PgTable aTable = new PgTable( getSchema());
							aTable.set( s.trim(), aDb);
							aTables.add( aTable);
						}
					}else
						setTables( aDb, sFrom, aTables);
				}
			}
			if( sWhere != null ) {
				int i = sWhere.length();
				if( sWhere.charAt( i-1) == ';' ) sWhere = sWhere.substring( 0, i-1);
			}
			if( sOrder != null ) {
				int i = sOrder.length();
				if( sOrder.charAt( i-1) == ';' ) sOrder = sOrder.substring( 0, i-1);
			}
		}
	}
	public void setTables( Database aDb, String sFrom,  ArrayList<PgTable> aTables)
	{
//		Database aDb = aSight.getDatabase();
/*		aSight.log( "----------------");
		aSight.log( sName);
		aSight.log( "");
		aSight.log( sFrom);
		aSight.log( ""); */
//		ArrayList<String> aJoins = new  ArrayList<String>( 10);
		int iA = 0, iE = 0;
		do {
			iE = sFrom.indexOf( " JOIN ", iA);
			String sTable = ( iE > 0 )? sFrom.substring( iA, iE): sFrom.substring( iA);
			String sTrimTable = trimBrackets( sTable);
//			aJoins.add( sTrimTable);
//			aSight.log( sTrimTable);
			PgTable aTable = new PgTable( getSchema());
			aTable.set( sTrimTable, aDb);
			aTables.add( aTable);
//			aSight.log( aTable.toStringTest());
			iA = iE+6;
		}while( iE > 0);
//		aSight.log( "----------------"); 
	}
	public String createSql()
	{
		StringBuffer aBuf = new StringBuffer();
		aBuf.append( "\r\n");
		aBuf.append( toString());
		return aBuf.toString();
	}
	public String toString()
	{
		StringBuffer aBuf = new StringBuffer();
		aBuf.append( "SELECT "); 
		boolean bFirst = true;
		for( PgAttribute aAttribute: aAttributes) { 
			if( bFirst ) bFirst = false; else aBuf.append( "," + System.lineSeparator());
			aBuf.append( "\t"); aBuf.append( aAttribute);
		}
		aBuf.append( System.lineSeparator());
		aBuf.append( "FROM "); aBuf.append( sFrom); 
		if( sWhere != null ) { aBuf.append( System.lineSeparator()); aBuf.append( "WHERE "); aBuf.append( sWhere); }		
		if( sOrder != null ) { aBuf.append( System.lineSeparator()); aBuf.append( "ORDER BY "); aBuf.append( sOrder); }
//		aBuf.append( ";\r\n");
		return aBuf.toString();
	}
	public String toCreateString( String sSchema, String sOwner)
	{
		StringBuffer aBuf = new StringBuffer();	
		aBuf.append( "CREATE OR REPLACE VIEW ");
		if( sSchema != null){
			aBuf.append( sSchema);
			aBuf.append( ".");
		}else if( !getSchema().getName().equalsIgnoreCase( "public")){
			aBuf.append( getSchema().getName());
			aBuf.append( ".");
		}
		aBuf.append( getName());
		aBuf.append( " AS ");
		aBuf.append( System.lineSeparator());
		aBuf.append( sSql); //toString());
		aBuf.append( ";");
		if( sOwner != null ){
			aBuf.append( System.lineSeparator());
			aBuf.append( "ALTER TABLE ");
			if( sSchema != null ){
				aBuf.append( sSchema);
				aBuf.append( ".");
			}
			aBuf.append( getName());
			aBuf.append( " OWNER TO ");
			aBuf.append( sOwner);
			aBuf.append( ";");
		}
		return aBuf.toString();
	}
	public String toCreateString() {
		return toCreateString( getSchemaName(), getOwner());
	}
	public int getNextAttrEnd( String s, int iS) 
	{
		int m = 0, n = s.length();
		if( iS >= n ) return -1;
		for( int i = iS; i < n; ++i){
			char c = s.charAt( i);
			if( c == '(' ) ++m;
			else if(  c == ')' ) --m;
			else if(  c == ',' && m == 0 ) return i;
		}
		return (m==0)? n: -1;
	}
	public String trimBrackets( String sIn)
	{
		int iS = 0, iE = 0, n = sIn.length();
		for( ; iS < n; ++iS){
			char c = sIn.charAt( iS);
			if( !(c == '(' ||  c == ' ') ) break;
		}
		int m = 0;
		for( iE = iS; iE < n; ++iE){
			char c = sIn.charAt( iE);
			if( c == '(' ) ++m;
			else if(  c == ')' ) { if( --m <= 0) { if( m == 0 ) ++iE; break;}}
			else if( c == '\r' || c == '\n') break;
		}
		return sIn.substring( iS, iE);
	}
	public String trimBracketsFull( String sIn)
	{
		int iS = 0, iE = 0, n = sIn.length();
		for( ; iS < n; ++iS){
			char c = sIn.charAt( iS);
			if( c == '(' ) break;
		}
		int m = 1;
		for( iE = iS; ++iE < n;){
			char c = sIn.charAt( iE);
			if( c == '(' ) ++m;
			else if(  c == ')' ) if( --m <= 0)  break;
		}
		return sIn.substring( iS+1, iE);
	}
	public static ArrayList<PgView> orderViews( PgConnection aConnection, ArrayList<PgView> aViews)
	{
		ArrayList<PgView> aOViews = new ArrayList<PgView>( 10);
		if( !aConnection.isShowSystemObjects() ){
			for( PgView aView: aViews){
				if( !aConnection.isSystemView( aView) ) {
					if( aView.aViewTables == null ) aView.analize();
					aOViews.add( aView);
				}
			}
			aViews = aOViews;
			aViews.trimToSize();
			aOViews = new ArrayList<PgView>( 10);
		}
		int n = 0;
		do {
			n = aViews.size();
			ArrayList<PgView> aNotOViews = new ArrayList<PgView>( 10);
			for( PgView aView: aViews) {
				if( !aView.addOrder( aOViews, aViews) ) aNotOViews.add( aView);
			}
			aViews = aNotOViews;
		}while( aViews.size() > 0 && aViews.size() < n );
		aOViews.trimToSize();
		return aOViews;
	}
	private boolean addOrder( ArrayList<PgView> aOViews,  ArrayList<PgView> aViews) 
	{
		for( PgTable aTable: (aViewTables!= null)? aViewTables: aTables) {
			if( !aTable.isTable() ) {
				int i = aOViews.size();
				for( ; --i >= 0; ) {
					PgView aView = aOViews.get( i);
					if( aView.getName().equalsIgnoreCase( aTable.getName())) break;
				}
				if( i < 0 ) { // It remains to be seen whether our view is
					for( PgView aView: aViews) 
						if( aView.getName().equalsIgnoreCase( aTable.getName()) ) return false;
				}
			}
		}
		aOViews.add( this);
		return true;
	}
    public Node addXML( DOMData aDoc, Node aRoot) 
    {
    	Node aNodeView = aDoc.addChildNode( aRoot, "view"); 
       	aDoc.addChildNode( aNodeView, "id", getId()); 
       	aDoc.addChildNode( aNodeView, "name", getName()); 
        aDoc.addChildNode( aNodeView, "schema_name", getSchemaName()); 
    	aDoc.addChildNode( aNodeView, "owner", getOwner()); 
    	aDoc.addChildNode( aNodeView, "edit", bEdit); 
    	aDoc.addChildNode( aNodeView, "changed", bChanged); 
    	Node aNodeTables = aDoc.addChildNode( aNodeView, "tables");
    	if( aViewTables != null ) for( PgTable aTable: aViewTables) {
    		aDoc.addChildNode( aNodeTables, "table", aTable.getName());
    	}
    	aDoc.addChildNode( aNodeView, "sql", sSql); 
    	return aNodeView;
	}
	public Node setAnalizedXML(DOMData aDoc, Node aRoot) {
		Node aNode = addXML( aDoc, aRoot);
		for( PgTable aTable: aTables) {
			aTable.addXML( aDoc, aNode);
		}
		for( PgAttribute aAttribute: aAttributes) {
			aAttribute.setXML( aDoc, aNode);
		}
		aDoc.addChildNode( aNode, "where", sWhere); 
		aDoc.addChildNode( aNode, "order", sOrder); 
		return aNode;
	}
	public void clear() {
		aTables = null;
		sFrom = null;
		sWhere = null;
		sOrder = null;
		sSql = null;
	}
	public File save( PgSight aSight, String sSchema, String sOwner) throws Exception
	{
		String sCatName = aSight.getLogCatName();
		String sFileName = sCatName + "/" + "createView_" + (( sSchema != null)? sSchema: getSchemaName()) + 
			"_" + getName() + ".sql";
		if( sCatName != null ){
			PrintStream aPS = GlobalFile.getPrintStream( sFileName, false);
			aPS.println( toCreateString( sSchema, sOwner));
			aPS.close();
		}
		return new File( sFileName);
	}
	public boolean save( String sName, HttpServletRequest aRequest)
	{
		PgConnection aDb = getConnection();
		String sNewName = aRequest.getParameter( "name");
		String sOldName = PgConnection.getViewName( sName);
		boolean bNew = sOldName == null || sOldName.trim().length() == 0;
		try {
			if( bNew || !sOldName.equalsIgnoreCase( sNewName) ){
				PgSchema aSchema = aDb.getSchema( sName);
				if( aSchema.getView( sNewName) != null ){
					aDb.setError( "A view with this name already exists");
					return false;
				}
				if( !bNew ){
					aDb.exec( "ALTER VIEW " + sOldName + " RENAME TO " + sNewName);
					aSchema.getViews().remove( this);
				}
				setName( sNewName);
				aSchema.add( this);
			}
			sSql = aRequest.getParameter( "sql");
			aDb.exec( toCreateString());
			if( bNew) load();
			bChanged = false;
			return true; // maybe this is not right
		} catch( Exception aE){
			aDb.log( aE);
			String sError = aE.getMessage();
			int i = sError.indexOf( "ERROR:");
			if( i >= 0 ) sError = sError.substring( i+6);
			if( !bNew && sError.indexOf( "syntax error") < 0 ){ // We will try again but remove the view beforehand
				ArrayList<PgView> aDViews = this.findDependentViews( aDb, getSchema().getViews());
// for( PgView aDView: aDViews) aDb.log( aDView.getName());				
				ArrayList<String> aBatch = new ArrayList<String>( 2);
/*				for( i = aDViews.size(); --i >= 0; ) {
					PgView aDView = aDViews.get( i);
					aBatch.add( "DROP VIEW " + aDView.getName() + ";");
				}*/
				try {
					aBatch.add( "DROP VIEW " + sNewName + " CASCADE;"); // not working in batch
					aBatch.add( toCreateString());
					for( PgView aDView: aDViews ) aBatch.add( aDView.toCreateString());
					aDb.exec( aBatch);
					return true;
				} catch (MException aE2) {
					aDb.log( aE2);
					aDb.setError( aE2.getLocalizedMessage());
				}	
			}
			aDb.setError( "Failed to update view: " + sError);
		}
		return false;
	}
	public String getFullName( String sName)
	{
    	int i = sName.indexOf( PgSight.cSeparator); // server
    	if( i>0 ) {
        	int j = sName.indexOf( PgSight.cSeparator, ++i); // db
        	if( j > 0 ) {
        		i = j;
        		j = sName.indexOf( PgSight.cSeparator, ++i); // schema
        		if( j > 0 ) {
        			return sName.substring( 0, ++j) + getName();
        		}
        		return sName + PgSight.cSeparator + getName();
        	}
    	}
    	return null;
	}
	public boolean load( DbAccess aDbIn) throws MException
	{
		setId( aDbIn.getInt( "id"));
		setName( aDbIn.getString( "viewname"));
//		sSchemaName = aDbIn.getString( "schemaname");
		setOwner( aDbIn.getString( "viewowner"));
		String sSql = aDbIn.getString( "definition");
		setSql( sSql);
		bChanged = false;
//		loadIn( aDbIn);
		return true;
	}
	public boolean load()
	{
		Database aDb = getConnection();
		DbAccess aDbIn = null;
		try {
			aDbIn = new DbAccess( aDb);
			aDbIn.setTable( "pg_views");
			if( aDbIn.select( sViewSql + " AND n.nspname='" + getSchemaName() + "' AND c.relname='" + getName() + "'") ){
				load( aDbIn);
			}
		}catch (Exception aE) {
			aDb.log( aE);
		}finally{
			if( aDbIn != null ) aDbIn.close();
		}
		return true;
	}
	private static String sViewSql = "SELECT c.oid as id, n.nspname AS schemaname, c.relname AS viewname, pg_get_userbyid(c.relowner) AS viewowner " + 
		", pg_get_viewdef(c.oid, 'true'::boolean) AS definition " + 
		"FROM (pg_class c LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace))) " + 
		"WHERE (c.relkind = 'v'::\"char\")";

	public static boolean load( Database aDb, PgSchema aSchema) // String sSchemaName, ArrayList<PgView>aViews)
	{
		ArrayList<PgView> aViews = aSchema.getViews();
		DbAccess aDbIn = null;
		try {
			aDbIn = new DbAccess( aDb);
			aDbIn.setTable( "pg_views");
			if( aDbIn.select( sViewSql + " AND n.nspname='" + aSchema.getName() + "' ORDER BY c.relname") ) do{
//				String sViewName = aDbIn.getString( "viewname");
//				if( sViewName.equalsIgnoreCase( "raster_columns") || sViewName.equalsIgnoreCase( "raster_overviews") || 
//					sViewName.equalsIgnoreCase( "geometry_columns") || sViewName.equalsIgnoreCase( "geography_columns")) continue;
//				String sSchemaName = aDbIn.getString( "schemaname");
//				String sOwnerName = aDbIn.getString( "viewowner");
				PgView aView = new PgView( aSchema); // sSchemaName, sViewName, sOwnerName);
				aView.load( aDbIn);
//				aView.setId( aDbIn.getInt( "id"));
//				String sSql = aDbIn.getString( "definition");
//				aView.setSql( sSql);
				aViews.add( aView);	
			}while( aDbIn.next());
		}catch (Exception e) {
			aDb.log( e);
		}finally{
			if( aDbIn != null ) aDbIn.close();
		}
		return true;
	}
	private static String sViewTablesSql = "SELECT r.ev_class::REGCLASS AS view_name, source_table.relname as source_table " + 
			"      FROM pg_depend d " + 
			"      JOIN pg_rewrite r ON (r.oid = d.objid) " + 
			"      JOIN pg_class as source_table ON d.refobjid = source_table.oid " + 
			"      JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace " + 
			"     WHERE refobjsubid > 0 AND source_ns.nspname =";
//	'public' " + 
//			"	GROUP BY view_name, source_table ORDER BY view_name"; 
	public static boolean loadViewTables( Database aDb, PgSchema aSchema)
	{
		DbAccess aDbIn = null;
		PgView aView = null;
		try {
			aDbIn = new DbAccess( aDb);
			aDbIn.setTable( "pg_views");
			if( aDbIn.select( sViewTablesSql + "'" + aSchema.getName() + 
				"' GROUP BY view_name, source_table ORDER BY view_name") ) do{
				String sViewName = aDbIn.getString( "view_name");
				int i = sViewName.indexOf( ".");
				if( i >= 0 ) sViewName = sViewName.substring( ++i);
				
				PgTable aTable = new PgTable( aSchema);
				aTable.set( aDbIn.getString( "source_table").trim(), aDb);
				if( aView != null && aView.getName().equalsIgnoreCase( sViewName)){	
					aView.aViewTables.add( aTable);
				}else{ 
					if( aView != null ) aView.aViewTables.trimToSize();
					aView = aSchema.getView( sViewName);
					if( aView != null ){
						aView.aViewTables = new ArrayList<PgTable>( 10);
						aView.aViewTables.add( aTable);
					}
				}
			}while( aDbIn.next());
			if( aView != null ) aView.aViewTables.trimToSize();
			return true;
		}catch (Exception e) {
			aDb.log( e);
		}finally{
			if( aDbIn != null ) aDbIn.close();
		}
		return false;
	}		
/*	private static String sViewInSql = "SELECT v.oid::regclass AS view " + 
			"FROM pg_depend AS d " + 
			"   JOIN pg_rewrite AS r " + 
			"      ON r.oid = d.objid " + 
			"   JOIN pg_class AS v " + 
			"      ON v.oid = r.ev_class " + 
			"WHERE v.relkind = 'v' " + 
			"  AND d.classid = 'pg_rewrite'::regclass " + 
			"  AND d.refclassid = 'pg_class'::regclass " + 
			"  AND d.deptype = 'n' " + 
			"  AND d.refobjid = ";
	public boolean xloadIn( DbAccess aDb)
	{
		aViewsIn = new ArrayList<String>( 10);
		DbAccess aDbIn = null;
		try {
			aDbIn = new DbAccess( aDb);
			aDbIn.setTable( "pg_views");
			if( aDbIn.select( sViewInSql + "'" + getSchemaName() + "." + getName() + "'::regclass GROUP BY view ORDER BY view") ) do{
				String sViewIn = aDbIn.getString( "view");
				if( !sViewIn.equalsIgnoreCase( getName())) aViewsIn.add( sViewIn);
			}while( aDbIn.next());
			aViewsIn.trimToSize();
			return true;
		}catch (Exception aE) {
			aDb.log( aE);
		}finally{
			if( aDbIn != null ) aDbIn.close();
		}
		return false;
	}*/
	public boolean remove()
	{
		PgConnection aDb = getConnection(); 
		try {
			aDb.exec( "DROP VIEW " + getName());
			getSchema().getViews().remove( this);
			return true; 
		} catch( Exception aE){
			aDb.log( aE);
		}
		aDb.setError( "Failed to remove view");
		return false;
	}
	public ArrayList<PgView> findDependentViews( PgConnection aDb, ArrayList<PgView>aViews)
	{
		for( PgView aView: aViews) aView.analize();
		ArrayList<PgView>aDViews = findDependentViews( aViews);
		aDViews.trimToSize();
		// they may still need to be ordered
		return aDViews;
	}
	private ArrayList<PgView> findDependentViews( ArrayList<PgView>aViews)
	{
		ArrayList<PgView>aDViews = new ArrayList<PgView>( 10);
		for( PgView aView: aViews){
			if( aView == this ) continue;
//			Log.log( aView.getName());
			for( PgTable aTable: aView.getTables()) {
//				if( aView.getName().equalsIgnoreCase("users2"))
//					Log.log( aTable.getName() + " " + aTable.isTable());
					
				if( !aTable.isTable() && aTable.getName().equalsIgnoreCase( getName()) ){ 
//					PgView aDView = getView( aViews, aTable.getName());
					if( addDependent( aDViews, aView) ){
						ArrayList<PgView>aDs = aView.findDependentViews( aViews);				
						for( PgView aD: aDs){
							addDependent( aDViews, aD);
						}
					}
				}
			}
		}
		return aDViews;
	}
	public static boolean addDependent( ArrayList<PgView>aDViews, PgView aView)
	{
		for( PgView aDView: aDViews){
			if( aDView.getName().equalsIgnoreCase( aView.getName()) ) return false;
		}
		aDViews.add( aView);
		return true;
	}
    public static PgView getView( ArrayList<PgView>aViews, String sName)
    {
		if( aViews != null ) for( PgView aView: aViews){
			if( aView.getName().equalsIgnoreCase( sName) ) return aView;
		}
		return null;
    }
	public static DOMData getFormXML( HttpServletRequest aRequest, Sight aSight) throws ExceptionIS
	{
    	DOMData aDoc = aSight.getTemplate();
		String sName = aRequest.getParameter( "full_name"); 
		PgSchema aSchema = (( PgSight)aSight).getSchema( sName);
		PgView aView = (( PgSight)aSight).getView( sName);
		if( aView == null && aSight.getCurDataObject() instanceof PgView ) aView = ( PgView)aSight.getCurDataObject();
		if( aView == null ){
			aView =  new PgView( aSchema); //, "", null);
			aSight.setCurDataObject( aView);
		}
		if( aView != null ){
			Node aNodeView = null;
	    	int iRet = Sight.getReturn( aRequest);
	    	if( iRet == 1 ){
	    		if( aView.save( sName, aRequest) ){ 
	    			sName = aView.getFullName( sName);
	    			aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
	    		}
	    		aNodeView = aView.addXML( aDoc, aDoc.getRootNode());
	    	}else if( iRet == 2 ){ // remove
	    		if( aView.remove() ){
	    			aView = null;
	    			aSight.setCurDataObject( null);
	    			aDoc.addChildNode( aDoc.getRootNode(), "ok", 2);
	    		}
	    	}else if( iRet == 21 ){ // save file
	    		aView.analize();
//	    		aDb.close();
	    		aNodeView = aView.setAnalizedXML( aDoc, aDoc.getRootNode());
	    	}else if( iRet == 22 ){ // upload
	    		aView.clear();
   	 			if( aRequest instanceof MultipartRequest ){
   	 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
	 					try {
						aView.setSql( aMRequest.getInputStream2());
					} catch (Exception aE) {
						aSight.log( aE);						
						aSight.setError( aE.getLocalizedMessage());
					}
   	 			}
	    		aNodeView = aView.addXML( aDoc, aDoc.getRootNode());
	    	}else if( iRet == 23 ){ // create new
//	    		PgConnection aDb = PgConnection.getConnection( aDbs, sName);
//	    		sName = PgConnection.trimToSchema( sName);
	    		aView = new PgView( (( PgSight)aSight).getSchema( sName), "new", null);
	    		aSight.setCurDataObject( aView);
	    		aNodeView = aView.addXML( aDoc, aDoc.getRootNode());
	    	}else if( iRet == 24 ){ // reload
//	    		PgConnection aDb = PgConnection.getConnection( aDbs, sName);
	    		aView.load();
	    		aNodeView = aView.addXML( aDoc, aDoc.getRootNode());
	    	}else{
				aNodeView = aView.addXML( aDoc, aDoc.getRootNode());
	    	}
	    	if( aView != null ){
				aDoc.addChildNode( aNodeView, "sql", aView.getSql());
				(( PgSight)aSight).setXmlNames( aDoc, sName, iRet != 23);
	    	}
	    }
		return aDoc;
	}
	public boolean sync( PgTable aTableC, ArrayList<PgProblem>aProblems) 
	{
		PgView aViewC = ( PgView)aTableC;
		String sSqlA = sSql.replaceAll( getSchemaName() + ".", "");
		String sSqlC = aViewC.sSql.replaceAll( aTableC.getSchemaName() + ".", "");
		
		
		if( !sSqlA.equalsIgnoreCase( sSqlC )){
			PgProblem aProblem = new PgProblem( this, '!', aViewC);
			aProblems.add( aProblem);
			return false;
		}
		return true;
	}
}
