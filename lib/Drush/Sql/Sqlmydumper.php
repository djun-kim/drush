<?PHP
/**
 * @file
 * This is Sqlmydumper.php, implementing the mydumper SQL driver.
 */

namespace Drush\Sql;

class Sqlmydumper extends Sqlmysql {

  public function dumpCmd($table_selection, $output_dir = '') {
    $parens = FALSE;
    $skip_tables = $table_selection['skip'];
    $structure_tables = $table_selection['structure'];
    $tables = $table_selection['tables'];

    $skip_tables  = array_merge($structure_tables, $skip_tables);
    $data_only = drush_get_option('data-only');

    $exec = 'mydumper ';

    // Start building up arguments for the command
    // Silent operation.
    $extra = " --verbose 0 --build-empty-files ";

    $output_dir = drush_escapeshellarg($output_dir);

    if (!empty($output_dir)) {
      $extra .= " --outputdir $output_dir ";
    }

    // Mydumper can't read credentials from a file, yet.
    $exec .= $this->creds(FALSE);

    if (drush_get_option('gzip')) {
      $extra .= ' --compress';
    }
    if (isset($data_only)) {
      $extra .= ' --no-schemas';
    }

    $exec .= $extra;

    if (!empty($tables)) {
      $exec .= ' --tables-list ' . implode(',', $tables);
    }
    else {
      $parens = TRUE;
      $tables = array_diff(parent::listTables(), $skip_tables);

      $exec .= ' --tables-list ' . implode(',', $tables);

      // Output_dir is not empty in default case where this is called from dump().
      if (empty($output_dir)) {
        $output_dir = '.';
      }

      // Run mysqldump and append output if we need some structure only tables.
      if (!empty($structure_tables)) {
        $only_db_name = str_replace('--database=', ' ', $this->creds());
        $extra = ' --no-autocommit --single-transaction --opt -Q';
        // NB: myloader is fussy about the files in $output_dir.
        // Hence schema_sql is ignored, but schema.sql causes a segfault.
        $exec .= " && mysqldump " . $only_db_name . " --no-data $extra " .
          implode(' ', $structure_tables) . " > $output_dir/schema_sql";
      }
    }
    return $parens ? "($exec)" : $exec;
  }

  /*
   * Generate a path to an output file for a SQL dump when needed.
   *
   * @param string|bool @file
   *   If TRUE, generate a path based on usual backup directory and current date.
   *   Otherwise, just return the path that was provided.
   */
  public function dumpFile($output_dir) {
    $database = $this->db_spec['database'];

    // $file is passed in to us usually via --result-file.  If the user
    // has set $options['result-file'] = TRUE, then we
    // will generate an SQL dump file in the same backup
    // directory that pm-updatecode uses.

    if (($output_dir === TRUE) || $output_dir == '') {
      // User did not pass a specific value for --result-file. Make one.
      $backup = drush_include_engine('version_control', 'backup');
      $backup_dir = $backup->prepare_backup_dir($database);
      if (empty($backup_dir)) {
        $backup_dir = drush_find_tmp();
      }
      $output_dir = $backup_dir . '/@DATABASE_@DATE';
    }
    $output_dir = str_replace(array('@DATABASE', '@DATE'), array($database, gmdate('Ymd_His')), $output_dir);
    return $output_dir;
  }

  /*
   * Dump the database using mydumper and return the path to the resulting dump directory.
   *
   * @param string|bool @file
   *   The path where the dump directory should be created. If TRUE, generate a path
   *   based on usual backup directory and current date.
   */
  public function dump($output_dir = '') {
    $table_selection = $this->get_expanded_table_selection();
    $output_dir = drush_escapeshellarg($this->dumpFile($output_dir));

    $cmd = $this->dumpCmd($table_selection, $output_dir);

    // Avoid the php memory of the $output array in drush_shell_exec().
    if (!$return = drush_op_system($cmd)) {
      if ($output_dir) {
        drush_log(dt('Database dump saved to !path', array('!path' => $output_dir)), 'success');
        drush_backend_set_result($output_dir);
      }
    }
    else {
      return drush_set_error('DRUSH_SQL_DUMP_FAIL', 'Database dump failed');
    }
  }

  public function loadCmd($dump_dir) {

    $dump_dir = drush_escapeshellarg($dump_dir);

    if (!file_exists($dump_dir)) {
      return drush_set_error('DRUSH_SQL_LOAD_FAIL', "Can't find the given dump directory: $dump_dir");
    }

    $exec = "myloader --directory $dump_dir";

    // Start building up arguments for the command.
    // Silent operation.
    $extra = " --verbose 0 ";
    $exec .= $extra;

    // Myloader can't read credentials from a file, yet.
    $exec .= $this->creds(FALSE);

    if (file_exists("$dump_dir/schema_sql")) {
      // First restore schemas by running mysql on dump_dir/schemas_sql.
      $myexec = 'mysql ' . $this->creds() . " < $dump_dir/schema_sql";

      if (!$return = drush_op_system($myexec)) {
        drush_log(dt('Restored database structure table schemas from !path', array('!path' => $output_dir . '/schema_sql')), 'success');
      }
      else {
        return drush_set_error('DRUSH_SQL_LOAD_FAIL', "Database load failed: could not create structure schemas ($return)");
      }
    }

    return $exec;
  }

  public function load($dump_dir) {
    $cmd = $this->loadCmd($dump_dir);

    // Avoid the php memory of the $output array in drush_shell_exec().
    if (!$return = drush_op_system($cmd)) {
      drush_log(dt('Database restored from !path', array('!path' => $dump_dir)), 'success');
    }
    else {
      return drush_set_error('DRUSH_SQL_LOAD_FAIL', "Database load failed: $return");
    }

  }

}
