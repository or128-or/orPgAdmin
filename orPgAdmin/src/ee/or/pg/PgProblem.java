<<<<<<< HEAD
package ee.or.pg;

import java.util.ArrayList;

import org.w3c.dom.Node;

import ee.or.is.DOMData;

public class PgProblem 
{
	private PgTable aTable;
	public PgTable getTable(){ return aTable;}
	
	private char cCase;
	public void setCase( char cCase){ this.cCase = cCase;}
	public boolean isOK(){ return cCase == '=';}

	private PgTable aTableC;
	public String getTableNameC(){ return aTableC.getSchemaName() + "." + aTableC.getName();}
	
	ArrayList<PgProblemAttr>aProblems = null;
	ArrayList<PgProblemConstraint>aProblemsC = null;
	
	public PgProblem( PgTable aTable, char cCase, PgTable aTableC) 
	{ 
		this.aTable = aTable;
		this.cCase = cCase;
		this.aTableC = aTableC;
	}
	public void addProblemAttr( PgAttribute aAttr, char cCase, PgAttribute aAttrC) 
	{
		if( aProblems == null ) aProblems = new ArrayList<PgProblemAttr>( 10);
		PgProblemAttr aP = new PgProblemAttr( this, aAttr, cCase, aAttrC);
		aProblems.add( aP);
	}
	public void addProblemAttr( PgAttribute aAttr, char cCase, PgAttribute aAttrC, String sComment) 
	{
		if( aProblems == null ) aProblems = new ArrayList<PgProblemAttr>( 10);
		PgProblemAttr aP = new PgProblemAttr( this, aAttr, cCase, aAttrC);
		aP.setComment( sComment);
		aProblems.add( aP);
	}
	public void addProblemConstraint( String sConstraint, char cCase, String sConstraintC) 
	{
		if( aProblemsC == null ) aProblemsC = new ArrayList<PgProblemConstraint>( 10);
		PgProblemConstraint aP = new PgProblemConstraint( this, sConstraint, cCase, sConstraintC);
		aProblemsC.add( aP);
	}
	public void setXML( DOMData aDoc)
	{
		boolean bTable = ( aTable != null)? aTable.isTable(): aTableC.isTable();
		Node aProblem = aDoc.addChildNode( aDoc.getRootNode(), bTable? "problem_table": "problem_view");
		if( aTable != null ) aDoc.addChildNode( aProblem, "name", aTable.getName());
		aDoc.addChildNode( aProblem, "case", cCase);
		if( aTableC != null ) aDoc.addChildNode( aProblem, "case_name", aTableC.getName());
		if( aProblems != null ){
			aProblems.trimToSize();
			for( PgProblemAttr aP: aProblems) aP.setXML( aDoc, aProblem);
		}
		if( aProblemsC != null ){
			aProblemsC.trimToSize();
			for( PgProblemConstraint aP: aProblemsC) aP.setXML( aDoc, aProblem);
		}
	}
	public String setSql(  String sSchema, String sOwner)
	{
		if( aTable != null ){
			if( aTable.isTable() ){
				if( aTableC == null ){ 
					return aTable.toCreateString( sSchema, sOwner);
				}else if( cCase == '!'){
					StringBuffer aBuf = new StringBuffer();
					if( aProblems != null ){
						aProblems.trimToSize();
						for( PgProblemAttr aP: aProblems) {
							String sLine = aP.setSql();
							if( sLine != null ) aBuf.append( sLine + System.lineSeparator());
						}
					}
					if( aBuf.length() > 0 ) return aBuf.toString();
				}
			}else{
				
			}
		}
		return null;
	}
	public String setSqlConstraint(  String sSchema)
	{
		if( aTable != null ){
			if( aTable.isTable() ){
				if( aTableC == null ){ 
					aTable.toCreateConstraintsString( sSchema);
					return null;
				}else if( cCase == '!'){
					StringBuffer aBuf = new StringBuffer();
					if( aProblemsC != null ){
						aProblemsC.trimToSize();
						for( PgProblemConstraint aP: aProblemsC) {
							String sLine = aP.setSql();
							if( sLine != null ) aBuf.append( sLine + System.lineSeparator());
						}
						if( aBuf.length() > 0 ) return aBuf.toString();
					}
				}
			}else{
				
			}
		}
		return null;
	}
}
final class PgProblemAttr 
{
	private PgProblem aProblem;
	private PgAttribute aAttr;
	private char cCase;
	private PgAttribute aAttrC;
	
	private String sComment;
	public void setComment( String sComment){ this.sComment = sComment;}
	
	public PgProblemAttr( PgProblem aProblem, PgAttribute aAttr, char cCase, PgAttribute aAttrC) {
		this.aProblem = aProblem;
		this.aAttr = aAttr;
		this.cCase = cCase;
		this.aAttrC = aAttrC;
	}
	public void setXML( DOMData aDoc, Node aRoot)
	{
		Node aProblem = aDoc.addChildNode( aRoot, "problem_attr");
		if( aAttr != null ) aDoc.addChildNode( aProblem, "name", aAttr.getName());
		aDoc.addChildNode( aProblem, "case", cCase);
		if( aAttrC != null ) aDoc.addChildNode( aProblem, "case_name", aAttrC.getName());
		if( sComment != null ) aDoc.addChildNode( aProblem, "comment", sComment);
	}
	public String setSql()
	{
		if( aAttr != null ) {
			if( aAttrC == null ){
				// ALTER TABLE cary ADD COLUMN seats integer;
				return "ALTER TABLE " + aProblem.getTableNameC() + " ADD COLUMN " + 
					aAttr.getName() + " " + aAttr.toCreateString() + ";";
			}else if( cCase == '!' ){ // only size change and new must be greater
				if( aAttr.getTypeName().equalsIgnoreCase( aAttrC.getTypeName()) && aAttr.getSize() > aAttrC.getSize() ){
					return "ALTER TABLE " + aProblem.getTableNameC() + " ALTER COLUMN " + 
						aAttr.getName() + " TYPE " + aAttr.toCreateString() + ";";
				}
			}
		}
		return null;
	}
}
final class PgProblemConstraint 
{
	private PgProblem aProblem;
	private String sConstraint;
	private char cCase;
	private String sConstraintC;
	
	public PgProblemConstraint( PgProblem aProblem, String sConstraint, char cCase, String sConstraintC) 
	{
		this.aProblem = aProblem;
		this.sConstraint = sConstraint;
		this.cCase = cCase;
		this.sConstraintC = sConstraintC;
	}
	public void setXML( DOMData aDoc, Node aRoot)
	{
		Node aProblem = aDoc.addChildNode( aRoot, "problem_attr");
		if( sConstraint != null ) aDoc.addChildNode( aProblem, "name", getName( sConstraint));
		aDoc.addChildNode( aProblem, "case", cCase);
		if( sConstraintC != null ) aDoc.addChildNode( aProblem, "case_name", getName( sConstraintC));
		String sComment = ( sConstraint != null)? sConstraint: sConstraintC;
		aDoc.addChildNode( aProblem, "comment", sComment);
	}
	private String getName(String sConstraint) 
	{
		int i = sConstraint.indexOf( " ");
		int j = sConstraint.indexOf( " ", ++i);
		return sConstraint.substring( i, j);
	}
	public String setSql()
	{
		if( sConstraint != null && sConstraintC == null ){
			// ALTER TABLE cary ADD COLUMN seats integer;
			return "ALTER TABLE " + aProblem.getTableNameC() + " ADD " + sConstraint + ";";
		}
		return null;
	}
}
=======
package ee.or.pg;

import java.util.ArrayList;

import org.w3c.dom.Node;

import ee.or.is.DOMData;

public class PgProblem 
{
	private PgTable aTable;
	public PgTable getTable(){ return aTable;}
	
	private char cCase;
	public void setCase( char cCase){ this.cCase = cCase;}
	public boolean isOK(){ return cCase == '=';}

	private PgTable aTableC;
	public String getTableNameC(){ return aTableC.getSchemaName() + "." + aTableC.getName();}
	
	ArrayList<PgProblemAttr>aProblems = null;
	ArrayList<PgProblemConstraint>aProblemsC = null;
	
	public PgProblem( PgTable aTable, char cCase, PgTable aTableC) 
	{ 
		this.aTable = aTable;
		this.cCase = cCase;
		this.aTableC = aTableC;
	}
	public void addProblemAttr( PgAttribute aAttr, char cCase, PgAttribute aAttrC) 
	{
		if( aProblems == null ) aProblems = new ArrayList<PgProblemAttr>( 10);
		PgProblemAttr aP = new PgProblemAttr( this, aAttr, cCase, aAttrC);
		aProblems.add( aP);
	}
	public void addProblemAttr( PgAttribute aAttr, char cCase, PgAttribute aAttrC, String sComment) 
	{
		if( aProblems == null ) aProblems = new ArrayList<PgProblemAttr>( 10);
		PgProblemAttr aP = new PgProblemAttr( this, aAttr, cCase, aAttrC);
		aP.setComment( sComment);
		aProblems.add( aP);
	}
	public void addProblemConstraint( String sConstraint, char cCase, String sConstraintC) 
	{
		if( aProblemsC == null ) aProblemsC = new ArrayList<PgProblemConstraint>( 10);
		PgProblemConstraint aP = new PgProblemConstraint( this, sConstraint, cCase, sConstraintC);
		aProblemsC.add( aP);
	}
	public void setXML( DOMData aDoc)
	{
		boolean bTable = ( aTable != null)? aTable.isTable(): aTableC.isTable();
		Node aProblem = aDoc.addChildNode( aDoc.getRootNode(), bTable? "problem_table": "problem_view");
		if( aTable != null ) aDoc.addChildNode( aProblem, "name", aTable.getName());
		aDoc.addChildNode( aProblem, "case", cCase);
		if( aTableC != null ) aDoc.addChildNode( aProblem, "case_name", aTableC.getName());
		if( aProblems != null ){
			aProblems.trimToSize();
			for( PgProblemAttr aP: aProblems) aP.setXML( aDoc, aProblem);
		}
		if( aProblemsC != null ){
			aProblemsC.trimToSize();
			for( PgProblemConstraint aP: aProblemsC) aP.setXML( aDoc, aProblem);
		}
	}
	public String setSql(  String sSchema, String sOwner)
	{
		if( aTable != null ){
			if( aTable.isTable() ){
				if( aTableC == null ){ 
					return aTable.toCreateString( sSchema, sOwner);
				}else if( cCase == '!'){
					StringBuffer aBuf = new StringBuffer();
					if( aProblems != null ){
						aProblems.trimToSize();
						for( PgProblemAttr aP: aProblems) {
							String sLine = aP.setSql();
							if( sLine != null ) aBuf.append( sLine + System.lineSeparator());
						}
					}
					if( aBuf.length() > 0 ) return aBuf.toString();
				}
			}else{
				
			}
		}
		return null;
	}
	public String setSqlConstraint(  String sSchema)
	{
		if( aTable != null ){
			if( aTable.isTable() ){
				if( aTableC == null ){ 
					aTable.toCreateConstraintsString( sSchema);
					return null;
				}else if( cCase == '!'){
					StringBuffer aBuf = new StringBuffer();
					if( aProblemsC != null ){
						aProblemsC.trimToSize();
						for( PgProblemConstraint aP: aProblemsC) {
							String sLine = aP.setSql();
							if( sLine != null ) aBuf.append( sLine + System.lineSeparator());
						}
						if( aBuf.length() > 0 ) return aBuf.toString();
					}
				}
			}else{
				
			}
		}
		return null;
	}
}
final class PgProblemAttr 
{
	private PgProblem aProblem;
	private PgAttribute aAttr;
	private char cCase;
	private PgAttribute aAttrC;
	
	private String sComment;
	public void setComment( String sComment){ this.sComment = sComment;}
	
	public PgProblemAttr( PgProblem aProblem, PgAttribute aAttr, char cCase, PgAttribute aAttrC) {
		this.aProblem = aProblem;
		this.aAttr = aAttr;
		this.cCase = cCase;
		this.aAttrC = aAttrC;
	}
	public void setXML( DOMData aDoc, Node aRoot)
	{
		Node aProblem = aDoc.addChildNode( aRoot, "problem_attr");
		if( aAttr != null ) aDoc.addChildNode( aProblem, "name", aAttr.getName());
		aDoc.addChildNode( aProblem, "case", cCase);
		if( aAttrC != null ) aDoc.addChildNode( aProblem, "case_name", aAttrC.getName());
		if( sComment != null ) aDoc.addChildNode( aProblem, "comment", sComment);
	}
	public String setSql()
	{
		if( aAttr != null ) {
			if( aAttrC == null ){
				// ALTER TABLE cary ADD COLUMN seats integer;
				return "ALTER TABLE " + aProblem.getTableNameC() + " ADD COLUMN " + 
					aAttr.getName() + " " + aAttr.toCreateString() + ";";
			}else if( cCase == '!' ){ // only size change and new must be greater
				if( aAttr.getTypeName().equalsIgnoreCase( aAttrC.getTypeName()) && aAttr.getSize() > aAttrC.getSize() ){
					return "ALTER TABLE " + aProblem.getTableNameC() + " ALTER COLUMN " + 
						aAttr.getName() + " TYPE " + aAttr.toCreateString() + ";";
				}
			}
		}
		return null;
	}
}
final class PgProblemConstraint 
{
	private PgProblem aProblem;
	private String sConstraint;
	private char cCase;
	private String sConstraintC;
	
	public PgProblemConstraint( PgProblem aProblem, String sConstraint, char cCase, String sConstraintC) 
	{
		this.aProblem = aProblem;
		this.sConstraint = sConstraint;
		this.cCase = cCase;
		this.sConstraintC = sConstraintC;
	}
	public void setXML( DOMData aDoc, Node aRoot)
	{
		Node aProblem = aDoc.addChildNode( aRoot, "problem_attr");
		if( sConstraint != null ) aDoc.addChildNode( aProblem, "name", getName( sConstraint));
		aDoc.addChildNode( aProblem, "case", cCase);
		if( sConstraintC != null ) aDoc.addChildNode( aProblem, "case_name", getName( sConstraintC));
		String sComment = ( sConstraint != null)? sConstraint: sConstraintC;
		aDoc.addChildNode( aProblem, "comment", sComment);
	}
	private String getName(String sConstraint) 
	{
		int i = sConstraint.indexOf( " ");
		int j = sConstraint.indexOf( " ", ++i);
		return sConstraint.substring( i, j);
	}
	public String setSql()
	{
		if( sConstraint != null && sConstraintC == null ){
			// ALTER TABLE cary ADD COLUMN seats integer;
			return "ALTER TABLE " + aProblem.getTableNameC() + " ADD " + sConstraint + ";";
		}
		return null;
	}
}
>>>>>>> refs/remotes/origin/master
