@sparkpost/msys-pg
==================

A simple promisified wrapping of [pg](https://www.npmjs.com/package/pg) with pooling.

`setup()` accepts all the same options as `pg.Pool` but note that `msys-pg` uses bluebird so it sets the `Promise` option accordingly.
