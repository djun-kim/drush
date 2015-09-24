<?php

/**
 * @file
 * Drush sql-load command.
 */

require_once "./sql.drush.inc";

/**
 * Implements hook_drush_command().
 */
function sqlload_drush_command() {
  $options['database'] = array(
    'description' => 'The DB connection key if using multiple connections in settings.php.',
    'example-value' => 'key',
  );
  $db_url['db-url'] = array(
    'description' => 'A Drupal 6 style database URL.',
    'example-value' => 'mysql://root:pass@127.0.0.1/db',
  );
  $options['target'] = array(
    'description' => 'The name of a target within the specified database connection. Defaults to \'default\'.',
    'example-value' => 'key',
    // Gets unhidden in help_alter(). We only want to show this to D7 users but have to
    // declare it here since some commands do not bootstrap fully.
    'hidden' => TRUE,
  );
  $items['sql-load'] = array(
    'description' => 'Imports the Drupal DB from a mydumper format dump using myloader.',
    'bootstrap' => DRUSH_BOOTSTRAP_NONE,
    'examples' => array(
      'drush sql-load db-20150918-223437' => 'Load from given mydumper directory.',
    ),
    'arguments' => array(
      'directory' => 'The mydumper directory to load from. Required',
    ),
    'required-arguments' => 1,
    'options' => $options + $db_url,
  );
  return $items;
}

/**
 * Command callback. Loads a database from a mydumper format dump directory.
 */
function drush_sql_load($dump_dir) {
  drush_sql_bootstrap_further();
  $sql = drush_sql_get_class();
  return $sql->load($dump_dir);
}
