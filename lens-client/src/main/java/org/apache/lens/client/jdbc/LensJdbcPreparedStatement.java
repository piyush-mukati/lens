package org.apache.lens.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

/**
 * Created by piyush.mukati on 11/12/15.
 */
public class LensJdbcPreparedStatement extends LensJdbcStatement implements PreparedStatement  {
  final private List<String> sqlTokens;
   private List<String> parameters;
  
  private final static String NULL="\"NULL\"";
  /**
   * Instantiates a new lens jdbc statement.
   *
   * @param connection the connection
   */
  public LensJdbcPreparedStatement(LensJdbcConnection connection,String parametrisedSql) {
    super(connection);
    this.sqlTokens =tokenize(parametrisedSql);
    this.parameters= Arrays.asList(new String[sqlTokens.size() - 1]);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    final String  finalSql=compile();
    return super.executeQuery(finalSql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    parameters.set(parameterIndex-1,NULL);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));

  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));

  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));

  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));

  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    parameters.set(parameterIndex-1,String.valueOf(x));

  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    parameters.set(parameterIndex-1, wrapString(x));

  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new UnsupportedOperationException();
  }


  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd");
    parameters.set(parameterIndex-1,formatter.format(x));
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS");
    parameters.set(parameterIndex-1,formatter.format(x));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS");
    parameters.set(parameterIndex-1,formatter.format(x));
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
throw new UnsupportedOperationException();
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearParameters() throws SQLException {
    parameters= Arrays.asList(new String[sqlTokens.size() - 1]);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute() throws SQLException {
    final String  finalSql=compile();
    return super.execute(finalSql);
  }

  @Override
  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
parameters.set(parameterIndex-1,NULL);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
parameters.set(parameterIndex-1, wrapString(value));
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();

  }
  public static List<String> tokenize(String sql){
    Preconditions.checkNotNull(sql);

    boolean inSingleQuotes=false;
    boolean inDoubleQuotes=false;

    int last=0;

    ArrayList tokens=new ArrayList<String>();
    for (int i=0;i<sql.length() ;i++){
      if('\''==sql.charAt(i))
        inSingleQuotes=!inSingleQuotes;
      else if('"'==sql.charAt(i))
        inDoubleQuotes=!inDoubleQuotes;
      else if(sql.charAt(i)=='?' && !(inDoubleQuotes|| inSingleQuotes)){
        tokens.add(sql.substring(last,i));
        last=i+1;
      }
    }
    tokens.add(sql.substring(last, sql.length()));

    return tokens;
  }

  public  String  compile(){
    StringBuffer buf=new StringBuffer();
    buf.append(sqlTokens.get(0));
    for (int i=0 ;i<parameters.size();i++){
      buf.append(parameters.get(i));
      buf.append(sqlTokens.get(i+1));
    }
    return buf.toString();
  }

  private static String wrapString(String x) {
    return "'" + x + "'";
  }

  @Test
  public void testTokenize(){
    List out= tokenize("select * from tab limit ?");
    Assert.assertArrayEquals(out.toArray(),new String[]{"select * from tab limit ",""});

    out= tokenize("select * from tab where a = ? and b=c");
    Assert.assertArrayEquals(out.toArray(),new String[]{"select * from tab where a = ","and b=c"});

    out= tokenize("select \"?\" from tab");
    Assert.assertArrayEquals(out.toArray(),new String[]{"select \"?\" from tab"});

    out= tokenize("select \'?\' from tab");
    Assert.assertArrayEquals(out.toArray(),new String[]{"select \'?\' from tab"});



  }

}
