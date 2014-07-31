#!/usr/bin/env python

from subprocess import Popen, PIPE, call
import optparse
import sys
import yaml

def getExperimentConfigFile(project, experiment, node=None):
	if node == None:
		cmd = "/usr/testbed/bin/node_list -e %s,%s -c" % (project, experiment)
		(output, err) = Popen(cmd.split(), stdout=PIPE).communicate()
		nodes = output.split(' ')
		for e in nodes:
			if not e.startswith('tbdelay'):
				node = e
				break
		
	remotefile = "/var/log/magi/experiment.conf"
	localfile = "/tmp/%s_%s_experiment.conf" % (project, experiment)
	
	cmd = "scp %s.%s.%s:%s %s" % (node, experiment, project, remotefile, localfile)
	call(cmd.split())
	
	return localfile

def getBridge(experimentConfigFile=None, project=None, experiment=None, node=None):
	if not experimentConfigFile:
		if not project or not experiment:
			raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
		experimentConfigFile = getExperimentConfigFile(project, experiment, node)
		
	mesdl = yaml.load(open(experimentConfigFile, 'r'))['mesdl']
	bridges = mesdl['bridges']
	
	return (bridges[0]['server'], bridges[0]['port'])
	
def getDBConfigHost(experimentConfigFile=None, project=None, experiment=None, node=None):
	if not experimentConfigFile:
		if not project or not experiment:
			raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
		experimentConfigFile = getExperimentConfigFile(project, experiment, node)
		
	experimentConfig = yaml.load(open(experimentConfigFile, 'r'))
	dbdl = experimentConfig['dbdl']
	expdl = experimentConfig['expdl']
	
	return "%s.%s.%s" % (dbdl['configHost'], expdl['eid'], expdl['pid'])

if __name__ == '__main__':
	optparser = optparse.OptionParser(description="Fetches bridge node and database configuration node. \
													Experiment Configuration File OR Project and Experiment Name \
                                                    needs to be provided to be able to connect to the experiment.")
	 
	optparser.add_option("-c", "--config", dest="config", help="Experiment configuration file location")
	optparser.add_option("-p", "--project", dest="project", help="Project name")
	optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
	optparser.add_option("-n", "--node", dest="node", default=None, help="Node name of one of the nodes in the experiment, if known.")
	(options, args) = optparser.parse_args()

	if not options.config and (not options.project or not options.experiment):
		optparser.print_help()
		sys.exit(2)
		
	(bridgeNode, bridgePort) = getBridge(experimentConfigFile=options.config, project=options.project, experiment=options.experiment, node=options.node)
	dbConfigNode = getDBConfigHost(experimentConfigFile=options.config, project=options.project, experiment=options.experiment, node=options.node)
	
	print "Bridge/Control Node: %s" %(bridgeNode)
	print "Bridge Port: %s" %(bridgePort)
	print "DB Config Node: %s" %(dbConfigNode)

