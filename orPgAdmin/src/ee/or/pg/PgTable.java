package ee.or.pg;

import java.io.File;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.w3c.dom.Node;

import ee.or.db.Database;
import ee.or.is.DOMData;
import ee.or.is.DataObject;
import ee.or.is.ExceptionIS;
import ee.or.is.GlobalData;
import ee.or.is.GlobalFile;
import ee.or.is.MultipartRequest;
import ee.or.is.Sight;

/**
 * @author or 15.11.2019
 */
public class PgTable extends DataObject
{
//	private String sName;
//	public String getName(){ return sName;}
//	public void setName( String sName){	this.sName = sName;}

	private String sAsName;
	public String getAsName(){ return sAsName;}
	public void setAsName(String sAsName){ this.sAsName = sAsName;}

	private String sWhere;
	public String getWhere(){ return sWhere;}
	public void setWhere(String sWhere){ this.sWhere = sWhere;}

	private boolean bTable = true;
	public boolean isTable(){	return bTable;}
	public void setTable( boolean bTable){	this.bTable = bTable;}

	private PgSchema aSchema;
	public PgSchema getSchema(){ return aSchema;}
	public String getSchemaName(){	return aSchema.getName();}
	public PgConnection getConnection(){ return aSchema.getConnection();}
	
	private ArrayList<PgAttribute> aAttributes;
	private PgAttribute getAttribute( String sName) {
		if( aAttributes != null ) for( PgAttribute aAttribute: aAttributes) { 
			if( aAttribute.getName().equalsIgnoreCase( sName) ) return aAttribute;
		}
		return null;
	}
	
	private String sPkey = null;

	private String sOwner;
	public String getOwner(){ return sOwner;}
	public void setOwner( String sOwner){ this.sOwner = sOwner;}

	public PgTable( PgSchema aSchema) 
	{
		this.aSchema = aSchema;
	}
	private ArrayList<String> aConstraints = null;
	public boolean hasConstraints() { return aConstraints != null;}
	
	public PgTable( String sName, String sAsName) {
		setName( sName);
		this.sAsName = sAsName;
	}
	public void set( String s, Database aDb)
	{
		int i = s.indexOf( " ON ");
		sWhere = ( i > 0 )? s.substring( i+4): null;
		String s1 = (( i > 0 )? s.substring( 0, i): s).trim();
		i = s1.indexOf( " ");
		if( i > 0 ){
			setName( s1.substring( 0, i));
			sAsName = s1.substring( i+1);
		}else{
			i = s1.indexOf( ".");
			setName( (i>=0)? s1.substring( ++i): s1);
			sAsName = null;
		}
		bTable = aDb.hasTable( getName());
	}
	public String toString()
	{
		return ( sAsName == null )? getName(): getName() + " " + sAsName + (( sWhere != null)? " ON " + sWhere: "");
	}
	public String toStringTest()
	{
		return getName() + ":" + sAsName + ":" + sWhere + ":" + bTable;
	}
    public Node addXML( DOMData aDoc, Node aRoot) 
    {
    	Node aNode = aDoc.addChildNode( aRoot, "table"); 
    	aDoc.addChildNode( aNode, "id", getId()); 
    	aDoc.addChildNode( aNode, "name", getName()); 
    	aDoc.addChildNode( aNode, "as_name", sAsName); 
    	aDoc.addChildNode( aNode, "where", sWhere); 
    	aDoc.addChildNode( aNode, "table", bTable);
    	aDoc.addChildNode( aNode, "owner", sOwner);
    	if( aAttributes != null) for( PgAttribute aAttr: aAttributes){
    		aAttr.setXML( aDoc, aRoot);
    	}
    	if( sPkey != null ) aDoc.addChildNode( aNode, "constraint", sPkey);
		if( aConstraints != null) for( String sConstraint: aConstraints) { 
			aDoc.addChildNode( aNode, "constraint", sConstraint);
		}
    	return aNode;
	}
	public void load( ResultSet aRs) throws Exception 
	{
		setId( GlobalData.getInt( aRs.getString( "id")));
		setName( aRs.getString( "tablename"));
		sOwner = aRs.getString( "tableowner");
//		setName( aRs.getString("TABLE_NAME"));
	}
	public boolean load( Database aDb)
	{
    	aAttributes = new ArrayList<PgAttribute>( 10);
    	ResultSet aRs = null;
    	try
    	{
			aRs = aDb.getMetaData( getName());
	       	while( aRs.next()) {
	       		PgAttribute aAttr = new PgAttribute();
	       		aAttr.load( aRs);
	       		aAttributes.add( aAttr);
	       		if( aAttr.isGeometry() ){
	       			ResultSet aRGs = aDb.execSelect( "SELECT * FROM geo_columns WHERE f_table_catalog='" + aSchema.getDatabase().getName() + 
	       				"' AND f_table_schema='" + aSchema.getName() + "' AND f_table_name='" + getName() + 
	       				"' AND f_geometry_column='" + aAttr.getName() + "'");
	       			while( aRGs.next() ){
	       				aAttr.setGeometry( aSchema.getDatabase().getName(), aSchema.getName(), getName(), aRGs);
	       			}
	       		}
	       	}
	       	aRs.close();
//	       	aRs = aDb.getTableKeys( aSchema.getName(), getName());
//	       	sPkey = ( aRs.next() )? aRs.getString( "pkcolumn_name"): null;
	       	loadConstraints( aDb);
	       	return true;
		} catch( Exception aE) {
			aDb.log( aE);
		} finally {
			try {
				
				aRs.close();
			} catch (SQLException e) {
			}
		}
    	return false;
	}
	public String toCreateString( String sSchema, String sOwner)
	{
		StringBuffer aBuf = new StringBuffer();
		aBuf.append( "CREATE TABLE ");
		if( sSchema != null ){
			aBuf.append( sSchema);
			aBuf.append( ".");
		}
		aBuf.append( getName());
		boolean bFirst = true;
		aBuf.append( "(" + System.lineSeparator());
		if( aAttributes != null ) for( PgAttribute aAttribute: aAttributes) { 
			if( bFirst ) bFirst = false; else aBuf.append( "," + System.lineSeparator());
			aAttribute.toTableCreateString( aBuf);
		}
		if( sPkey != null){
			aBuf.append( "," + System.lineSeparator());
			aBuf.append( sPkey);
		}
/*		if( aConstraints != null) for( String sConstraint: aConstraints) { 
			aBuf.append( "," + System.lineSeparator());
			aBuf.append( sConstraint);
		}
		if( sPkey != null){ // CONSTRAINT te_user_pkey PRIMARY KEY (id),
			aBuf.append( "," + System.lineSeparator());
			aBuf.append( "\tCONSTRAINT ");
			aBuf.append( getName());
			aBuf.append( "_pkey PRIMARY KEY (");
			aBuf.append( sPkey);
			aBuf.append( ")");
		}*/
		aBuf.append( System.lineSeparator() + ");");
		
/*		for( PgAttribute aAttribute: aAttributes) {
			if( aAttribute.isGeometry()) {
				aBuf.append( System.lineSeparator());
				aAttribute.addGeometry( aBuf);
			}
		}*/
		
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
		aBuf.append( System.lineSeparator());
		return aBuf.toString();
	}
	public String toCreateConstraintsString( String sSchema)
	{
		StringBuffer aBuf = new StringBuffer();
		if( aConstraints != null) for( String sConstraint: aConstraints) { 
			aBuf.append( "ALTER TABLE ");
			if( sSchema != null ){
				aBuf.append( sSchema);
				aBuf.append( ".");
			}
			aBuf.append( getName());
			aBuf.append( " ADD ");
			aBuf.append( sConstraint);
			aBuf.append( ";");
			aBuf.append( System.lineSeparator());
		}
		return aBuf.toString();
	}
	public File save( PgSight aSight, String sSchema, String sOwner) throws Exception
	{
		String sCatName = aSight.getLogCatName();
		String sFileName = sCatName + "/" + "createTable_" + getName() + ".sql";
//		String sFileName = sCatName + "/" + "createTable" + WordUtils.capitalize( getName()) + ".sql";
		if( sCatName != null ){
			PrintStream aPS = GlobalFile.getPrintStream( sFileName, false);
			aPS.println( toCreateString( sSchema, sOwner));
			aPS.println( toCreateConstraintsString( sSchema));
			aPS.close();
		}
		return new File( sFileName);
	}
/*
	private static String sGeomSql = "SELECT current_database()::character varying(256) AS f_table_catalog, n.nspname::character varying(256) AS f_table_schema, c.relname::character varying(256) AS f_table_name, a.attname::character varying(256) AS f_geometry_column, " + 
			"COALESCE(NULLIF(postgis_typmod_dims(a.atttypmod), 2), " + 
			"postgis_constraint_dims(n.nspname::text, c.relname::text, a.attname::text), 2) AS coord_dimension, " + 
			"COALESCE(NULLIF(postgis_typmod_srid(a.atttypmod), 0), postgis_constraint_srid(n.nspname::text, c.relname::text, a.attname::text), 0) AS srid, " + 
			"replace(replace(COALESCE(NULLIF(upper(postgis_typmod_type(a.atttypmod)), 'GEOMETRY'::text), postgis_constraint_type(n.nspname::text, c.relname::text, a.attname::text)::text, 'GEOMETRY'::text), 'ZM'::text, ''::text), 'Z'::text, ''::text)::character varying(30) AS type " +
			"FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n " +
			"WHERE t.typname = 'geometry'::name AND a.attisdropped = false AND a.atttypid = t.oid AND a.attrelid = c.oid AND c.relnamespace = n.oid AND (c.relkind = 'r'::\"char\" OR c.relkind = 'v'::\"char\") AND NOT pg_is_other_temp_schema(c.relnamespace) AND NOT (n.nspname = 'public'::name AND c.relname = 'raster_columns'::name) AND has_table_privilege(c.oid, 'SELECT'::text) ";
*/
	String sSqlConstraints = "SELECT conname, contype, pg_get_constraintdef( c.oid) as condef " + 
			"FROM   pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace " + 
			"WHERE  conrelid::regclass::text = '%1'";
	public void loadConstraints( Database aDb)
	{
		aConstraints = new ArrayList<String>( 10);
		ResultSet aRs = null;
		try {
			String sSchema = aSchema.getName().equalsIgnoreCase( "public")? getName(): aSchema.getName() + "." + getName(); 
			aRs = aDb.execSelect( sSqlConstraints.replace( "%1", sSchema)); 
			while( aRs.next()){
				String sConstraint = "CONSTRAINT " + aRs.getString( "conname") + " " + aRs.getString( "condef");
				String sType = aRs.getString( "contype");
				if( sType.charAt( 0) == 'p' ) sPkey = sConstraint;
				else aConstraints.add( sConstraint);
			}
			if( aConstraints.size() == 0 ) aConstraints = null;
			else aConstraints.trimToSize();
		} catch( Exception aE) {
			aDb.log( aE);
		}finally{
			try{
				if( aRs != null ) aRs.close();
			}catch( SQLException aE ){
			}
		}
	}
	public static DOMData getFormXML( HttpServletRequest aRequest, Sight aSight) throws ExceptionIS
	{
    	DOMData aDoc = aSight.getTemplate();
		String sName = aRequest.getParameter( "full_name"); 
    	int iRet = Sight.getReturn( aRequest);
    	PgSchema aSchema = (( PgSight)aSight).getSchema( sName);
		PgTable aTable =(( PgSight)aSight).getTable( sName);
		if( aTable == null && aSight.getCurDataObject() instanceof PgTable ) aTable = ( PgTable)aSight.getCurDataObject();
		if( aTable == null ){
			aTable =  new PgTable( aSchema); //, "", null);
			aSight.setCurDataObject( aTable);
		}else if( iRet == 22 ){ // upload
 			if( aRequest instanceof MultipartRequest ){
 				MultipartRequest aMRequest = (MultipartRequest)aRequest;
				try {
			    	aSchema.execSql( aMRequest.getInputStream2(), 1);
					aDoc.addChildNode( aDoc.getRootNode(), "ok", 1);
				} catch (Exception aE) {
					aSight.log( aE);
					aSight.setError( aE.getLocalizedMessage());
				}
 			}
 		}
		aTable.addXML( aDoc, aDoc.getRootNode());
    	(( PgSight)aSight).setXmlNames( aDoc, sName, true);
    	return aDoc;
	}
	public boolean sync( PgTable aTableC, PgProblem aProblem) 
	{
		boolean bProblem = false;
		if( aAttributes != null ) for( PgAttribute aAttr: aAttributes) {
			String sName = aAttr.getName();
			PgAttribute aAttrC = aTableC.getAttribute( sName);
			if( aAttrC != null ){
				if( aAttr.sync( aAttrC, aProblem) ){
				}else{
					bProblem = true;
				}
			}else{ 
				aProblem.addProblemAttr( aAttr, '-', null);
				bProblem = true;
			}
		}
		if( aTableC.aAttributes != null ) for( PgAttribute aAttr: aTableC.aAttributes) {
			if( getAttribute( aAttr.getName()) == null ){
				aProblem.addProblemAttr( null, '-', aAttr);
				bProblem = true;
			}
		}
		if( aConstraints != null ) for( String sCon: aConstraints) {
			String sA = sCon.replaceAll( getSchemaName() + ".", "");
			boolean bFound = false;
			if( aTableC.aConstraints != null ) for( String sConC: aTableC.aConstraints) {
				String sC = sConC.replaceAll( aTableC.getSchemaName() + ".", "");
				if( sA.equalsIgnoreCase( sC)) { bFound = true; break;}
			}
			if( !bFound ){
				aProblem.addProblemConstraint( sCon, '-', null);
				bProblem = true;
			}
		}
		if( aTableC.aConstraints != null ) for( String sCon: aTableC.aConstraints) {
			String sA = sCon.replaceAll( aTableC.getSchemaName() + ".", "");
			boolean bFound = false;
			if( aConstraints != null ) for( String sConC: aConstraints) {
				String sC = sConC.replaceAll( getSchemaName() + ".", "");
				if( sA.equalsIgnoreCase( sC)) { bFound = true; break;}
			}
			if( !bFound ){
				aProblem.addProblemConstraint( null, '-', sCon);
				bProblem = true;
			}
		}
		return !bProblem;
	}
}
