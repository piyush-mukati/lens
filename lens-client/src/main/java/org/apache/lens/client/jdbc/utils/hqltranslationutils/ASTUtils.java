package org.apache.lens.client.jdbc.utils.hqltranslationutils;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Created by vikassingh on 01/06/15.
 */
public class ASTUtils {
  public static List<String> getAllTableList(ASTNode queryAST) throws SemanticException, LensException {
    final ArrayList<String> allTableList = new ArrayList<String>();
    HQLParser.bft(queryAST,new HQLParser.ASTNodeVisitor() {
      public void visit(HQLParser.TreeNode visited) {
        ASTNode node = visited.getNode();
        if(node.getToken().getType()== HiveParser.TOK_TABREF)
          allTableList.add(getTableNameWithoutAlias(node));
      }
    });
    return allTableList;
  }

  public static List<String> getAllColumnList(ASTNode queryAST) throws SemanticException, LensException {
    final ArrayList<String> allColumnList = new ArrayList<String>();
    HQLParser.bft(queryAST,new HQLParser.ASTNodeVisitor() {
      public void visit(HQLParser.TreeNode visited) {

      }
    });
    return allColumnList;
  }

  public static List<String> getAllColumnNames(ASTNode queryAST){
    return new ArrayList<String>();
  }

  public static String getTableName(ASTNode tableNode,String dbname){

    ASTNode tableReferenceNode = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME);
    String tableName;
    if (tableReferenceNode.getChildCount()==2){//This is the case of schema_name.table_name
      String schemaName = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(0).getText();
      String tabName = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(1).getText();
      tableName = schemaName + "." + tabName;
    } else{
      tableName = dbname+"."+HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(0).getText();
    }

    if (tableNode.getChildCount()==2){/*Case of tablename as alias*/
      String alias = HQLParser.findNodeByPath(tableNode, HiveParser.Identifier).getText();
      return tableName +" as "+alias;
    }
    return tableName;
  }

  public static String getTableNameWithoutAlias(ASTNode tableNode){

    ASTNode tableReferenceNode = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME);
    String tableName;
    if (tableReferenceNode.getChildCount()==2){//This is the case of schema_name.table_name
      String schemaName = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(0).getText();
      String tabName = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(1).getText();
      tableName = schemaName + "." + tabName;
    } else{
      tableName = HQLParser.findNodeByPath(tableNode, HiveParser.TOK_TABNAME).getChild(0).getText();
    }

    return tableName;
  }

  public static JoinType getJoinType(ASTNode node) {
    switch (node.getToken().getType()) {
      case TOK_LEFTOUTERJOIN:
        return JoinType.LEFTOUTER;
      case TOK_LEFTSEMIJOIN:
        return JoinType.LEFTSEMI;
      case TOK_RIGHTOUTERJOIN:
        return JoinType.RIGHTOUTER;
      case TOK_FULLOUTERJOIN:
        return JoinType.FULLOUTER;
      case TOK_JOIN:
        return JoinType.INNER;
      case TOK_UNIQUEJOIN:
        return JoinType.UNIQUE;
      default:
        return null;
    }
  }

}
