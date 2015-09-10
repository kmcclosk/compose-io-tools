// https://gist.github.com/wpm/f745ea6478507c6eb72f

var Promise = require('bluebird');

/**
 * Periodically poll a signal function until either it returns true or a timeout is reached.
 *
 * @param signal function that returns true when the polled operation is complete
 * @param interval time interval between polls in milliseconds
 * @param timeout period of time before giving up on polling
 * @returns true if the signal function returned true, false if the operation timed out
 */
function poll(signal, interval, timeout) {
    
    function pollRecursive() {
	return signal() ? Promise.resolve(true) : Promise.delay(interval).then(pollRecursive);
    }

    return pollRecursive()
        .cancellable()
        .timeout(timeout)
        .catch(Promise.TimeoutError, Promise.CancellationError, function () {
	    return false;
	});
}

module.exports = {
    poll: poll
};
