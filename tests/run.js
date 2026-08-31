'use strict';
const harness = require('./harness');

require('./rand.test');
require('./kv.test');
require('./raft.test');
require('./sim.test');
require('./linearizability.test');
require('./scripted.test');
require('./fuzz.test');

const result = harness.run('windlass');
module.exports = result;
if (result.failures.length > 0) process.exitCode = 1;
