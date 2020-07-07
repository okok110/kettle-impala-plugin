/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.impalaoutput;

import java.sql.Statement;
import java.sql.Savepoint;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Storage class for table output step.
 *
 * @author Matt
 * @since 24-jan-2005
 */
public class ImpalaOutputData extends BaseStepData implements StepDataInterface {
  public Database db;
  public int warnings;
  public String tableName;
  public int[] valuenrs; // Stream valuename nrs to prevent searches.
  public Map<String, String> sqls;
  /**
   * Mapping between the SQL and the actual prepared statement. Normally this is only one, but in case we have more then
   * one, it's convenient to have this.
   */

  public Map<String, Statement> statements;

  public int indexOfPartitioningField;

  /** Cache of the data formatter object */
  public SimpleDateFormat dateFormater;

  /** Use batch mode or not? */
  public boolean batchMode;
  public int indexOfTableNameField;

  public List<Object[]> batchBuffer;
  public boolean sendToErrorRow;
  public RowMetaInterface outputRowMeta;
  public RowMetaInterface insertRowMeta;
  public boolean useSafePoints;
  public Savepoint savepoint;
  public boolean releaseSavepoint;

  public DatabaseMeta databaseMeta;

  public Map<String, Integer> commitCounterMap;

  public int commitSize;

  public ImpalaOutputData() {
    super();

    db = null;
    warnings = 0;
    tableName = null;

    statements = new Hashtable<String, Statement>();

    indexOfPartitioningField = -1;
    indexOfTableNameField = -1;

    batchBuffer = new ArrayList<Object[]>();
    commitCounterMap = new HashMap<String, Integer>();
    sqls = new Hashtable<String, String>();

    releaseSavepoint = true;
  }
}
