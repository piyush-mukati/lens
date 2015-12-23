package org.apache.lens.client.jdbc.utils.hqltranslationutils;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lens.cube.parse.HQLParser;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Created by vikassingh on 22/05/15.
 * Given a parsed ASTNode tree of HQL select query, converts it to HQL String.
 */
public class SelectQueryTranslator {
  private ASTNode query;
  private String select;
  private String from;
  private String where;
  private String groupby;
  private String orderby;
  private String having;
  private Integer limit;
  public static final Log LOG = LogFactory.getLog(SelectQueryTranslator.class.getName());
  public SelectQueryTranslator(ASTNode query) {
    this.query = query;
  }

  public String toHQL(String  dbname) throws SemanticException {
    setMissingExpressions(dbname);
    String qfmt = getQueryFormat();
    Object[] queryTreeStrings = getQueryTreeStrings();
    if (LOG.isDebugEnabled()) {
      LOG.debug("qfmt:" + qfmt + " Query strings: " + Arrays.toString(queryTreeStrings));
    }
    String baseQuery = String.format(qfmt, queryTreeStrings);
    return baseQuery;
  }

  protected void setMissingExpressions(String dbname) throws SemanticException {
    ASTNode fromTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_FROM);
    ASTNode selectTree = getSelectTree();
    ASTNode whereTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_WHERE);
    ASTNode groupByTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_GROUPBY);
    ASTNode orderByTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_ORDERBY);
    ASTNode havingTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_HAVING);
    ASTNode limitTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_LIMIT);
    this.from = populateFromClause(fromTree,dbname);
    this.select =HQLParser.getString(selectTree);
    this.where = HQLParser.getString(whereTree);
    this.groupby = HQLParser.getString(groupByTree);
    this.orderby = HQLParser.getString(orderByTree);
    this.having = HQLParser.getString(havingTree);
    this.limit = populateLimitClause(limitTree);
  }

  private ASTNode getSelectTree(){
    ASTNode selectTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_SELECT);
    if (selectTree == null){
      selectTree = HQLParser.findNodeByPath(this.query, HiveParser.TOK_INSERT, HiveParser.TOK_SELECTDI);
    }
    return selectTree;
  }

  private String populateFromClause(ASTNode fromClause,String dbname){
    StringBuilder from = new StringBuilder();
    if (fromClause.getChildCount()==1){
      ASTNode child =  (ASTNode)fromClause.getChild(0);
      LOG.info("type of child node: "+child.token.getType());
      LOG.info("token value: "+HiveParser.TOK_TABREF);
      if (child.token.getType() == HiveParser.TOK_TABREF){
        //Case of No joins, Select from a single table
        from.append(ASTUtils.getTableName(child,dbname));
      } else if (child.token.getType() == HiveParser.TOK_SUBQUERY){
        //Case of Subquery, Select ab from (Select rm from ts)
        from.append(populateFromClauseForSubexpression(child,dbname));
      } else {
        from.append(populateFromClauseForJoins(child,dbname));
      }
    }

    return from.toString();
  }

  private String populateFromClauseForJoins(ASTNode joinNode,String dbname){
    StringBuilder queryString = new StringBuilder();
    String andCondition="";
    List<String> rightAndLeftPartOfJoin = new ArrayList<String>();
    for(Node child : joinNode.getChildren()){
      if (ASTUtils.getJoinType((ASTNode)child)!=null || ((ASTNode)child).getToken().getType()==HiveParser.TOK_SUBQUERY
              || ((ASTNode)child).getToken().getType()==HiveParser.TOK_TABREF){
        rightAndLeftPartOfJoin.add(populateGenericExpressions((ASTNode)child,dbname));
      } else{
        andCondition = populateGenericExpressions((ASTNode)child,dbname);
      }
    }

    String rightPart = rightAndLeftPartOfJoin.get(0);
    String leftPart = rightAndLeftPartOfJoin.get(1);

    queryString.append(" "+ rightPart + " " +translateJoinType(joinNode)+ " " +leftPart);

    if (!StringUtils.isBlank(andCondition)){
      queryString.append(" on "+ andCondition);
    }
    return queryString.toString();
  }

  private String populateGenericExpressions(ASTNode child,String dbname) {
    StringBuilder queryString = new StringBuilder();
    if (child.getToken().getType()== HiveParser.TOK_SUBQUERY){
      queryString.append(ASTUtils.getTableName(child,dbname));
    } else if (child.getToken().getType()==HiveParser.TOK_TABREF){
      queryString.append(ASTUtils.getTableName(child,dbname));
    } else if (ASTUtils.getJoinType(child)!=null){
      queryString.append(populateFromClauseForJoins(child,dbname));
    } else if (child.getToken().getType()==HiveParser.KW_AND){
      queryString.append(HQLParser.getString(child));
    } else if (child.getToken().getType()== HiveParser.EQUAL){
      queryString.append(HQLParser.getString(child));
    } else{
      queryString.append(HQLParser.getString(child));
    }
    return queryString.toString();
  }



  private String populateFromClauseForSubexpression(ASTNode subExpressionNode,String dbname){
    String alias = HQLParser.getString(HQLParser.findNodeByPath(subExpressionNode, HiveParser.Identifier));
    ASTNode subQuery = HQLParser.findNodeByPath(subExpressionNode, HiveParser.TOK_QUERY);
    SelectQueryTranslator hqlGeneratorBasicQuery = new SelectQueryTranslator(subQuery);
    String subQueryString;
    try {
      subQueryString = hqlGeneratorBasicQuery.toHQL(dbname);
    } catch(Exception e){
      throw new RuntimeException(e);
    }
    if (!StringUtils.isBlank(alias)){
      return " ( "+subQueryString +" as "+ alias+" ) ";
    }
    return " ( "+subQueryString+" ) ";
  }

  private Integer populateLimitClause(ASTNode limitClause){
    if(limitClause==null){
      return null;
    }
    String limitText = limitClause.getChild(0).getText();
    return Integer.parseInt(limitText);
  }



  private String translateJoinType(ASTNode node) {
    switch (node.getToken().getType()) {
      case TOK_LEFTOUTERJOIN:
        return "left outer join";
      case TOK_LEFTSEMIJOIN:
        return "left semi join";
      case TOK_RIGHTOUTERJOIN:
        return "right outer join";
      case TOK_FULLOUTERJOIN:
        return "full outer join";
      case TOK_JOIN:
        return "inner join";
      case TOK_UNIQUEJOIN:
        return "unique join";
      case TOK_CROSSJOIN:
        return "join";
      default:
        return null;
    }
  }

  private String getQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append("SELECT %s FROM %s");
    if(!StringUtils.isBlank(this.where)) {
      queryFormat.append(" WHERE %s");
    }

    if(!StringUtils.isBlank(this.groupby)) {
      queryFormat.append(" GROUP BY %s");
    }

    if(!StringUtils.isBlank(this.having)) {
      queryFormat.append(" HAVING %s");
    }

    if(!StringUtils.isBlank(this.orderby)) {
      queryFormat.append(" ORDER BY %s");
    }

    if(this.limit != null) {
      queryFormat.append(" LIMIT %s");
    }

    return queryFormat.toString();
  }

  private String[] getQueryTreeStrings() throws SemanticException {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(select);
    qstrs.add(from);
    if (!StringUtils.isBlank(where)) {
      qstrs.add(where);
    }
    if (!StringUtils.isBlank(groupby)) {
      qstrs.add(groupby);
    }
    if (!StringUtils.isBlank(having)) {
      qstrs.add(having);
    }
    if (!StringUtils.isBlank(orderby)) {
      qstrs.add(orderby);
    }
    if (limit != null) {
      qstrs.add(String.valueOf(limit));
    }
    return qstrs.toArray(new String[0]);
  }

}
