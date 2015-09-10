'use strict';

var chai = require('chai');
var expect = chai.expect;

var config = {
    API_TOKEN: process.env.COMPOSEIO_API_TOKEN,
    ACCOUNT_SLUG: process.env.COMPOSEIO_ACCOUNT_SLUG,
    LOCATION: process.env.COMPOSEIO_LOCATION
};

var composeIO = require('../index')(config);

describe('Compose IO Tools', function () {

    it('should get deployments',function(done) {

        composeIO
            .getDeployments()
	    .then(function(res) {

		//console.log(res);
		
                expect(res.status).to.equal(200);
		
                done();
            })
	    .catch(function(err) {
                done(err);		
	    });
    });
});
