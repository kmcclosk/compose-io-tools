'use strict';

var R = require('ramda');
var Promise = require('bluebird');
var moment = require('moment');
var format = require('string-template');
var compose = require('./lib/compose-io-api');
var asyncPoller = require('./lib/async-poller');

////////////////////////////////////////////////////////////////////////////////

module.exports = function(config,logger) {

    //logger.debug(config,'config');

    var API_TOKEN = config.API_TOKEN;
    var ACCOUNT_SLUG = config.ACCOUNT_SLUG;
    var LOCATION = config.LOCATION;

    if (!logger) {
	
	var bunyan = require('bunyan');
	var blackhole = require('stream-blackhole');

	logger = bunyan.createLogger({
            name: 'compose-io-tools',	
	    stream: blackhole()
	});
    }

    ////////////////////////////////////////////////////////////////////////////////

    function createElasticDeployment(deployment,database,location) {

        logger.debug(deployment,'deployment');
        logger.debug(database,'database');
        logger.debug(location,'location');

        location = location || config.LOCATION;

        //POST /accounts/:account/deployments/elastic

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .post()
                .accounts()
                .account(ACCOUNT_SLUG)
                .deployments()
                .elastic()
                .send({ name: deployment })
                .send({ database_name: database })
                .send({ type: 'mongodb' })
                .send({ location: location })
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function getDeployments() {

        //GET /accounts/:account/deployments

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .accounts()
                .account(ACCOUNT_SLUG)
                .deployments()
                .end(function(err,res) {

                    //'new' 'running' 'pending_deprovision'

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function getDeployment(deployment) {

        //GET /deployments/:account/:deployment

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .end(function(err,res) {

                    //'new' 'running' 'pending_deprovision'

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function createDatabaseUser(deployment,database,username,password,readOnly) {

        readOnly = readOnly || false;

        //POST /deployments/:account/:deployment/mongodb/:database/users

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .post()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .type('mongodb')
                .database(database)
                .users()
                .send({ username: username })
                .send({ password: password })
                .send({ readOnly: readOnly })
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function waitForDeployment(deployment,delay) {

        delay = delay || 10000;

        return getDeployment(deployment)
            .then(function(rv) {

                logger.debug(rv.statusCode,'statusCode');
                logger.debug(rv.text,'text');
                logger.debug(rv.body,'body');

                var status = rv.body.status;

                /*
                  if (status === 'error') {
                  throw new Error(rv.error);
                  }
                  else*/
                if (status === 'running') {
                    return rv;
                }

                return Promise
                    .delay(delay)
                    .then(function() {
                        return waitForDeployment(deployment,delay)
                    });
            });
    }

    function waitForBackup(deployment,type,delay) {

        delay = delay || 10000;

        return listBackups(deployment)
            .then(function(rv) {

                logger.debug(rv.statusCode,'statusCode');
                logger.debug(rv.text,'text');
                logger.debug(rv.body,'body');

                var backup = R.find(R.propEq('type',type))(rv.body);

                logger.debug(backup,'backup');

                var status = backup.status;

                if (status === 'complete') {
                    return rv;
                }

                return Promise
                    .delay(delay)
                    .then(function() {
                        return waitForBackup(deployment,delay)
                    });
            });
    }

    function createElasticDeploymentAndDatabaseUser(deployment,database,location,username,password,readOnly) {

        return createElasticDeployment(deployment,database)
            .then(function(rv) {

                logger.debug(rv.statusCode,'statusCode');
                logger.debug(rv.text,'text');
                logger.debug(rv.body,'body');

                return waitForDeployment(deployment);
            })
            .then(function(rv) {

                logger.debug(rv.statusCode,'statusCode');
                logger.debug(rv.text,'text');
                logger.debug(rv.body,'body');

                return createDatabaseUser(deployment,database,username,password,readOnly);
            });
    }

    function triggerBackup(deployment) {

        //POST /deployments/:account/:deployment/backups

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .post()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .type('backups')
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function listBackups(deployment) {

        //GET /deployments/:account/:deployment/backups

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .type('backups')
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res.body);
                });
        });
    }

    function getOnDemandBackup(deployment) {

        return listBackups(deployment)
            .then(function(backups) {

                logger.debug(backups,'backups');

                return R.find(R.propEq('type','on_demand'))(backups);
            });
    }

    function getBackup(backupId) {

        //GET /accounts/:account/backups/:backup

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .account(ACCOUNT_SLUG)
                .type('backups')
                .type(backupId)
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function copyOnDemandBackup(src_deployment,src_database,dst_deployment,dst_database,location) {

        location = location || config.LOCATION;

        return getOnDemandBackup(src_deployment)
            .then(function(onDemandBackup) {

                logger.debug(onDemandBackup,'onDemandBackup');

                return restoreBackup(onDemandBackup.id,src_database,dst_deployment,dst_database,location);
            });
    }

    function restoreBackup(backupId,src_database,dst_deployment,dst_database,location) {

        location = location || config.LOCATION;

        //POST /accounts/:account/backups/:backup/restore

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .post()
                .accounts()
                .account(ACCOUNT_SLUG)
                .type('backups')
                .type(backupId)
                .type('restore')
                .send({ name: dst_deployment })
                .send({ database_name: dst_database })
                .send({ src_database: src_database })
                .send({ location: location })

                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function listDatabases(deployment) {

        //GET /deployments/:account/:deployment/mongodb/databases

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .type('mongodb')
                .type('databases')
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function getHistoricalLogs(deployment,day) {

        day = day || new moment().format();

        logger.debug(day,'day');

        //GET /deployments/:account/:deployment/historical_logs

        return new Promise(function(resolve,reject) {

            compose()
                .accessToken(API_TOKEN)
                .get()
                .deployments()
                .account(ACCOUNT_SLUG)
                .deployment(deployment)
                .type('historical_logs')
                .send({ day: day })
                .end(function(err,res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
        });
    }

    function connectString(deploymentName) {

        return getDeployment(deploymentName)
            .then(function(rv) {

                //logger.debug(rv.body,'rv');

                var deployment = rv.body;

                var un = 'trafficbridge';
                var pw = 'Talent123!';
                var db = 'app';
                var conn0 = deployment.members[0];
                var conn1 = deployment.members[1];
                var replicaSet = deployment.databases[0].deployment_id;

                var str = format('mongodb://{0}:{1}@{2},{3}/{4}?replicaSet=set-{5}',un,pw,conn0,conn1,db,replicaSet);

                return str;
            });
    }

    return {
        createElasticDeployment: createElasticDeployment,
        createDatabaseUser: createDatabaseUser,
        getDeployments: getDeployments,
        getDeployment: getDeployment,
        listBackups: listBackups,
        getBackup: getBackup,
        triggerBackup: triggerBackup,
        restoreBackup: restoreBackup,
        getHistoricalLogs: getHistoricalLogs,
        listDatabases: listDatabases,
        //
        connectString: connectString,
        createElasticDeploymentAndDatabaseUser: createElasticDeploymentAndDatabaseUser,
        copyOnDemandBackup: copyOnDemandBackup,
        waitForDeployment: waitForDeployment,
        waitForBackup: waitForBackup
    };
}
