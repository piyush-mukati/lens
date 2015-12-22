package org.apache.lens.client.jdbc;

import java.io.IOException;

import org.apache.lens.client.jdbc.utils.hqltranslationutils.ASTToHQLTranslator;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.codehaus.groovy.ast.ASTNode;

/**
 * Created by piyush.mukati on 14/12/15.
 */
public class QueryRewriter {
  String query;
  StringBuilder rewrittenQuery = new StringBuilder();
  private static final Log LOG = LogFactory.getLog(QueryRewriter.class);
  org.apache.hadoop.hive.ql.parse.ASTNode ast;

  public String rewrite(String query, String dbname, HiveConf metastoreConf,int limit) throws LensException {
    if (query.startsWith("add jar"))
      return query;

    this.query = query;
    StringBuilder mergedQuery;
    rewrittenQuery.setLength(0);

    try {
      if (query.toLowerCase().matches("(.*)union all(.*)")) {
        String[] queries = query.toLowerCase().split("union all");
        for (int i = 0; i < queries.length; i++) {
          LOG.info("Union Query Part " + i + " : " + queries[i]);
          ast = HQLParser.parseHQL(queries[i], metastoreConf);

          ASTToHQLTranslator queryGenerator = new ASTToHQLTranslator(ast);
          rewrittenQuery.append(queryGenerator.toHQL(dbname));
          mergedQuery = rewrittenQuery.append(" union all ");
          rewrittenQuery = new StringBuilder(mergedQuery.toString().substring(0, mergedQuery.lastIndexOf("union all")));
        }
        LOG.info("Input Query : " + query);
        LOG.info("Rewritten Query :  " + rewrittenQuery.toString());
      } else {
        ast = HQLParser.parseHQL(query, metastoreConf);

        ASTToHQLTranslator queryGenerator = new ASTToHQLTranslator(ast);
        rewrittenQuery.append(queryGenerator.toHQL(dbname));
        LOG.info("Input Query : " + query);
        LOG.info("Rewritten Query :  " + rewrittenQuery.toString());
      }
      } catch (Exception e) {
      LOG.info("Exception  " + e.getMessage());
      throw new LensException(e);
    }
    LOG.info("Query rewrite success full");
    return rewrittenQuery.toString()+" LIMIT "+limit;
  }

}
