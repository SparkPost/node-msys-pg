'use strict';


const chai = require('chai');
const expect = chai.expect;
const _ = require('lodash');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const testSuite = require('@sparkpost/msys-test-suite');

chai.use(require('sinon-chai'));

describe('PostgreSQL Wrapper', () => {
  let wrapper
    , configMock
    , pgMock
    , connectionMock
    , poolMock;

  beforeEach(() => {
    configMock = {
      pg: {
        user: 'you',
        password: 'soylentgreen',
        host: 'host.aroo.non-existant'
      }
    };

    connectionMock = {
      query: sinon.stub().resolves(),
      release: sinon.stub().resolves(),
      arrayParams: sinon.stub().returns()
    };

    poolMock = {
      connect: sinon.stub().resolves(connectionMock),
      end: sinon.stub().resolves()
    };

    pgMock = {
      Pool: sinon.stub().returns(poolMock)
    };

    wrapper = proxyquire('../../index', {
      pg: pgMock
    });
  });

  describe('setup', () => {
    it('should create a connection pool', () => {
      wrapper.setup(configMock.pg);
      expect(pgMock.Pool).to.have.been.calledOnce;
      expect(pgMock.Pool.args[0][0]).to.include.keys(Object.keys(configMock.pg));
    });

    it('should throw if called >1 time', () => {
      wrapper.setup(configMock.pg);
      expect(() => wrapper.setup(configMock.pg)).to.throw('pg-wrapper may only be initialised once');
    });
  });

  describe('query', () => {
    let error
      , sql
      , values;

    beforeEach(() => {
      error = new Error('I\'VE MADE A HUGE MISTAKE');

      sql = 'SELECT * FROM A_TABLE';
      values = [];

      wrapper.setup(configMock);
    });

    it('should get a connection and query', () => wrapper.query(sql, values)
        .then(() => {
          expect(poolMock.connect).to.have.been.called;
          expect(connectionMock.query).to.have.been.calledWith(sql, values);
          expect(connectionMock.release).to.have.been.called;
        }));

    it('should reject if there is an error getting a connection', () => {
      poolMock.connect.rejects(error);

      return testSuite.promiseFail(wrapper.query(sql, values))
        .catch((err) => {
          expect(err).to.equal(error);

          expect(poolMock.connect, 'connect').to.have.been.called;
          expect(connectionMock.query, 'query').not.to.have.been.called;
          expect(connectionMock.release, 'release').not.to.have.been.called;
        });
    });

    it('should reject if it there is an error querying', () => {
      connectionMock.query.rejects(error);

      return testSuite.promiseFail(wrapper.query(sql, values))
        .catch((err) => {
          expect(err).to.equal(error);

          expect(poolMock.connect, 'connect').to.have.been.called;
          expect(connectionMock.query, 'query').to.have.been.called;
          expect(connectionMock.release, 'release').to.have.been.called;
        });
    });
  });

  describe('getConnection', () => {
    beforeEach(() => {
      wrapper.setup(configMock);
    });

    it('should return a connection', () => wrapper.getConnection()
        .then((conn) => {
          expect(poolMock.connect).to.have.been.calledOnce;
          expect(_.keys(conn)).to.be.deep.equal(['query', 'insert', 'begin', 'commit', 'rollback', 'release']);
        }));

    it('should throw error if connection creation fails', () => {
      poolMock.connect.rejects(new Error('uh ah!'));
      return testSuite.promiseFail(wrapper.getConnection())
        .catch((err) => {
          expect(err.message).to.equal('uh ah!');
          expect(poolMock.connect).to.have.been.calledOnce;
        });
    });

    describe('release', () => {
      it('should release connection', () => wrapper.getConnection()
          .then((conn) => conn.release())
          .then(() => {
            expect(poolMock.connect).to.have.been.calledOnce;
            expect(connectionMock.release).to.have.been.calledOnce;
          }));
    });
  });

  describe('insert', () => {
    const sql = 'INSERT INTO bob (x) VALUES ($1)';
    const params = ['Ex'];

    beforeEach(() => {
      sinon.stub(wrapper, 'query').resolves({rows: [{id: 101}]});
      wrapper.setup(configMock);
    });

    it('should call wrapper.query', () => wrapper.insert(sql, params)
        .then(() => {
          expect(wrapper.query).to.have.been.calledOnce;
        }));

    it('should append RETURNING id to each query', () => wrapper.insert(sql, params)
        .then(() => {
          expect(wrapper.query.args[0][0]).to.equal(`${sql} RETURNING id`);
        }));
  });

  describe('arrayParams', () => {
    it('should form a list of pg-style params', () => {
      const params = wrapper.arrayParams([1,2,'a','b']);
      expect(params).to.equal('$1,$2,$3,$4');
    });
  });

  describe('teardown', () => {
    it('should tear down the db connection pool', () => {
      wrapper.setup(configMock);
      return wrapper.teardown().then(() => {
        expect(poolMock.end).to.have.been.calledOnce;
      });
    });

    it('should reject if called before setup', () => testSuite.promiseFail(wrapper.teardown())
        .catch((err) => {
          expect(err.message).to.equal('pg-wrapper must be initialised before teardown');
        }));
  });
});
