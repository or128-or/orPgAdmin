package ee.or.pg;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.w3c.dom.Node;

import ee.or.is.DOMData;
import ee.or.is.GlobalData;

/**
 * @author or 27.02.2019
 */
public class PgAttribute {

	private String sName;
	public String getName(){ return sName;}
	public void setName( String sName){	this.sName = sName;}

	private String sAttrValue;
	public String getAttrValue(){ return sAttrValue;}
	public void setAttrValue(String sAttrValue){ this.sAttrValue = sAttrValue;}
	
	private String sTypeName;
	public String getTypeName(){ return sTypeName;}
	public boolean isGeometry() { return (sTypeName != null && sTypeName.equalsIgnoreCase( "geometry"))? true: false;}
	
	private String sDef;
	
	private String sSize;
	public int getSize(){ return GlobalData.getInt( sSize);}
	
	private int iType;
	private String sGeometry;
	private int iDig;
	private boolean bNull;
	
	public String getGeometry(){ return sGeometry;}
	public void setGeometry( String sDatabase, String sSchema, String sTable, ResultSet aRs) throws SQLException
	{
		//geometry(Point,4326)

//		int iDim = aRs.getShort( "coord_dimension");
		int iSrid = aRs.getShort( "srid");
		String sType = aRs.getString( "type");
		sGeometry = "geometry(" + sType + "," + iSrid + ")";
	}
	public void addGeometryX( StringBuffer aBuf)
	{
		aBuf.append( "SELECT ");
		aBuf.append( sGeometry);
		aBuf.append( ";");
	}
	public PgAttribute() {
	}
	public PgAttribute( String sName, String sAttrValue) {
		this.sName = sName;
		this.sAttrValue = sAttrValue;
	}
	public String toString()
	{
		if( sName == null ) return sAttrValue;
		return sAttrValue + " AS " + sName;
	}
	public String toCreateString()
	{
		return getSqlType() + ( bNull? "": " NOT NULL") + getSqlDefault();
	}
	public void toTableCreateString( StringBuffer aBuf)
	{
		aBuf.append( "\t"); 
		aBuf.append( sName);	
		aBuf.append( " ");
		aBuf.append( getSqlType());
		if( bNull ) aBuf.append( "");
		else aBuf.append( " NOT NULL");
		aBuf.append( getSqlDefault());
	}
	public String getSqlType()
	{
		if( sTypeName.equalsIgnoreCase( "varchar")){
			int iSize = GlobalData.getInt( sSize);
			if( iSize < 1000000 ){
				return "character varying( " + sSize + ")";
			}else
				return "character varying";
		}else if( sTypeName.equalsIgnoreCase( "int2")){
			return "smallint"; 
		}else if( sTypeName.equalsIgnoreCase( "int4")){
			return "integer"; 
		}else if( sTypeName.equalsIgnoreCase( "int8")){
			return "bigint"; 
		}else if( sTypeName.equalsIgnoreCase( "float8")){
			return "double precision"; 
		}else if( sTypeName.equalsIgnoreCase( "numeric")){
			return "numeric( " + sSize + ", " + iDig + ")"; 
		}else if( sTypeName.equalsIgnoreCase( "geometry")){
			return sGeometry;
		}else if( sTypeName.equalsIgnoreCase( "bpchar")){
			return "character( " + sSize + ")"; 
		}else if( sTypeName.equalsIgnoreCase( "timestamptz")){
			return "timestamp with time zone"; 
		}else{
			if( sTypeName.equalsIgnoreCase( "serial")) 
				return "serial";
			else
				return sTypeName;
		}
	}
	public String getSqlDefault()
	{
		if( sTypeName.equalsIgnoreCase( "serial")) return ""; 
		else if( sDef != null ) return " default " + sDef;
		else return "";
	}
    public Node setXML( DOMData aDoc, Node aRoot) 
    {
    	Node aNode = aDoc.addChildNode( aRoot, "attribute"); 
    	aDoc.addChildNode( aNode, "as_name", sName); 
    	aDoc.addChildNode( aNode, "name", sAttrValue); 
    	if( sTypeName != null ){
        	aDoc.addChildNode( aNode, "text", toCreateString()); 
        	aDoc.addChildNode( aNode, "type_name", getSqlType()); 
        	aDoc.addChildNode( aNode, "def", sDef); 
        	aDoc.addChildNode( aNode, "size", sSize); 
        	aDoc.addChildNode( aNode, "type", iType); 
    	}else{
    		aDoc.addChildNode( aNode, "text", toString()); 
    	}
    	return aNode;
	}
	public void load(ResultSet aRs) throws Exception 
	{
		sName = aRs.getString( "COLUMN_NAME").toLowerCase();
		sTypeName = aRs.getString( "TYPE_NAME");
		sDef = aRs.getString( "COLUMN_DEF");
		iType = aRs.getShort("DATA_TYPE");
		sSize = aRs.getString( "COLUMN_SIZE");
		iDig = aRs.getInt( "DECIMAL_DIGITS");
		bNull = aRs.getString("IS_NULLABLE").equalsIgnoreCase( "YES");
  	}
	public boolean sync( PgAttribute aAttrC, PgProblem aProblem) 
	{
		StringBuffer aBuf = new StringBuffer();
		if( sTypeName != null ) {
			if( !sTypeName.equalsIgnoreCase( aAttrC.sTypeName) ) aBuf.append( "The type is different. ");
			else{
				if( sSize != null && !sSize.equalsIgnoreCase( aAttrC.sSize) ){
					aBuf.append( "The size is different. ");
					if( iDig != aAttrC.iDig ) aBuf.append( "The decimal digits is different. ");
				}
				String sDef = getSqlDefault();
				if( sDef != null && !sDef.equalsIgnoreCase( aAttrC.getSqlDefault()) ) aBuf.append( "Initialization has changed. ");
				else if( sDef == null && aAttrC.getSqlDefault() != null ) aBuf.append( "The default value no longer exists. ");
				if( bNull != aAttrC.bNull ) aBuf.append( "The nullable is different. ");
			}
		}
		if( aBuf.length() > 0) {
			aProblem.addProblemAttr( this, '!', aAttrC, aBuf.toString());
			return false;
		}
		return true;
	}
}
