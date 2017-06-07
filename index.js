'use strict';

const pg = require('pg');
const Promise = require('bluebird');
const wrapper = module.exports;

let pool;

/**
 * Initialise pg connection pool.  Must be called before any other method.
 * Note: this is synchronous.
 * @param {object} cfg - configuration parameters
 */
wrapper.setup = (cfg) => {
  const config = Object.assign({}, cfg, {
    Promise: Promise
  });

  if (pool) {
    throw new Error('pg-wrapper may only be initialised once');
  }

  pool = new pg.Pool(config);
  // TODO: set idle error handler
};

/**
 * Cleanup the pg connection pool. Use this to close all idle connections ahead of process shutdown.
 * Note: setup() must be called first.
 * @returns {Promise}
 */
wrapper.teardown = () => {
  const poolToEnd = pool;
  if (pool) {
    pool = null;
    return poolToEnd.end();
  }
  return Promise.reject(new Error('pg-wrapper must be initialised before teardown'));
};

/**
 * Execute a query using a connection from the pool.
 * Note: setup() must be called first.
 * @param {string} sql - SQL query
 * @param {Object[]} params - query params
 */
wrapper.query = (sql, params) => {
  let conn;
  return pool.connect().then((_conn) => {
    conn = _conn;
    return conn.query(sql, params);
  }).finally(() => {
    if (conn) {
      conn.release();
      conn = null;
    }
  });
};

/**
 * Execute an INSERT using a connection from the pool.
 * Note: setup() must be called first.
 * @param {string} sql - SQL query
 * @param {Object[]} params - query params
 * @returns {Promise.number} - ID of inserted record
 */
wrapper.insert = (sql, params) => wrapper.query(`${sql} RETURNING id`, params)
    .then((result) => result.rows[0].id);

/**
 * Pull a pg connection from the pool for use within a transaction.
 * Note: setup() must be called first.
 * @returns {Promise.Object}
 */
wrapper.getConnection = () => pool.connect().then((conn) => ({
  query: conn.query.bind(conn),
  insert: (sql, params) => conn.query(`${sql} RETURNING id`, params),
  begin: () => Promise.resolve(conn.query('BEGIN')),
  commit: () => Promise.resolve(conn.query('COMMIT')),
  rollback: () => Promise.resolve(conn.query('ROLLBACK')),
  release: conn.release.bind(conn)
}));

wrapper.arrayParams = (arr) => arr.map((elt, idx) => `$${idx + 1}`).join(',');

