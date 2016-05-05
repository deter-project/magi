#include "Agent.h"
#include "AgentMessenger.h"
#include "AgentTransport.h"
#include "Logger.h"
#include "Util.h"

#include <string.h>
#include <yaml.h>

char *agentName, *dockName, *nodeConfigFile, *experimentConfigFile;

char *expDBLocation, *expDBPort, *logDir, *logFileName = NULL,
		*commGroup = NULL, *commHost = NULL, *hostName = NULL;

int log_level, commPort = 0;

Logger* logger;

int stop_flag = 0;

// Union Data structure to hold the parameter (either int or char*).
union Data {
	int i;
	char* s;
};

// A maximum of 10 parameters are allowed for a function call from the agent file.
union Data data[10];

// Macro to get the parameter value from the Union Data structure defined above.
//#define ARG(x) (sizeof(data[x])==sizeof(int)) ? data[x].i : data[x].s
#define ARG(x) data[x]

// Structure to hold the function map and its arguments.
typedef struct {
	char *name;
	int aCnt;
	char* argList[10];
	char* retType;
	void* (*func)();
} fMap;

fMap* function_map;

// Structure to hold the list of the functions from the agent code.
typedef struct fList {
	char* name;
	int aCnt;
	char* argList[10];
	char* retType;
	void* (*fptr)();
	struct fList* next;
} fList_t;

// list of registered functions
static fList_t* regFuncList = NULL;

void freeFList(fList_t* funcList) {
	fList_t* nextNode;
	while (funcList != NULL) {
		nextNode = funcList->next;
		free(funcList->name);
		free(funcList->retType);
		int k = 0;
		while (k < funcList->aCnt) {
			free(funcList->argList[k]);
		}
		free(funcList->fptr);
		funcList = nextNode;
	}
}

// count of registered functions
int regFuncCount = 0;

/**
 * Sends AgentLoadDone trigger to the control group
 */
void sendAgentLoadDoneTrigger() {
	entrylog(logger, __func__, __FILE__, __LINE__);
	log_info(logger, "Sending AgentLoadDone trigger for host %s and agent %s",
			hostName, agentName);
	char* triggerFmt = "{nodes: %s, event: AgentLoadDone, agent: %s}";
	char* data = (char*) malloc(
			strlen(triggerFmt) + strlen(hostName) + strlen(agentName) + 1);
	sprintf(data, triggerFmt, hostName, agentName);
	trigger("control", NULL, YAML, data);
	free(data);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Sends AgentUnloadDone trigger to the control group
 */
void sendAgentUnloadDoneTrigger() {
	entrylog(logger, __func__, __FILE__, __LINE__);
	log_info(logger, "Sending AgentUnloadDone trigger for host %s and agent %s",
			hostName, agentName);
	char* triggerFmt = "{nodes: %s, event: AgentUnloadDone, agent: %s}";
	char* data = (char*) malloc(
			strlen(triggerFmt) + strlen(hostName) + strlen(agentName) + 1);
	sprintf(data, triggerFmt, hostName, agentName);
	trigger("control", NULL, YAML, data);
	free(data);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Create a list of functions which is added by the agent code.
 *
 * @param name - name of the function to be added.
 * @param retType - return type of the function being added.
 * @param fptr - function pointer to be added.
 * @param argCount - number of arguments of the function.
 */
void registerFunction(char* name, char* retType, void* fptr, int argCount, ...) {
	fList_t* regFuncNode = (fList_t*) malloc(sizeof(fList_t));

	regFuncNode->name = malloc(strlen(name) + 1);
	strcpy(regFuncNode->name, name);
	regFuncNode->retType = malloc(strlen(retType) + 1);
	strcpy(regFuncNode->retType, retType);
	regFuncNode->fptr = fptr;

	va_list kw;
	va_start(kw, argCount);

	char* args;
	int k = 0;

	while ((args = va_arg(kw,char*)) != NULL && k < argCount)
	{
		regFuncNode->argList[k] = (char*) malloc(strlen(args) + 1);
		strcpy(regFuncNode->argList[k], args);
		k++;
	}
	regFuncNode->aCnt = k;

	regFuncNode->next = regFuncList;
	regFuncList = regFuncNode;

	regFuncCount++;
}

// Create a function map from the function list from the agent code..
void create_functionMap() {
	function_map = malloc(regFuncCount * sizeof(fMap));

	fList_t *temp = regFuncList;
	int cnt = regFuncCount;

	int i = 0;
	while (cnt || temp) {
		(function_map[i]).name = temp->name;
		(function_map[i]).func = temp->fptr;
		(function_map[i]).retType = temp->retType;
		int j = 0;
		while (j < temp->aCnt) {
			(function_map[i]).argList[j] = temp->argList[j];
			j++;
		}
		(function_map[i]).aCnt = temp->aCnt;
		temp = temp->next;
		cnt--;
		i++;
	}
	free(regFuncList);
}

// The stop function - set the stop flag to 1.
void stopFunc() {
	stop_flag = 1;
}

void parseConfFile(char* configFile) {
	FILE *fh = fopen(configFile, "r");
	yaml_parser_t parser;
	yaml_token_t token;

	/* Initialize parser */
	if (!yaml_parser_initialize(&parser))
		fputs("Failed to initialize parser!\n", stderr);
	if (fh == NULL)
		fputs("Failed to open file!\n", stderr);

	/* Set input file */
	yaml_parser_set_input_file(&parser, fh);

	do {
		yaml_parser_scan(&parser, &token);
		switch (token.type) {
		case YAML_SCALAR_TOKEN:
			// Parse the file when databse token is found for port and host
			if (!strcmp((const char *) token.data.scalar.value, "database")) {
				if (token.type != YAML_STREAM_END_TOKEN) {
					yaml_token_delete(&token);
					yaml_parser_scan(&parser, &token);
					yaml_token_delete(&token);
					yaml_parser_scan(&parser, &token);
					if (token.type != YAML_BLOCK_MAPPING_START_TOKEN) {
						break;
					}
					while (token.type != YAML_BLOCK_END_TOKEN) {
						yaml_token_delete(&token);
						yaml_parser_scan(&parser, &token);
						if (token.data.scalar.value != NULL) {
							if (!strcmp((const char *) token.data.scalar.value,
									"collectorPort")) {
								if (token.type != YAML_STREAM_END_TOKEN) {
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									expDBPort = (char*) malloc(
											strlen((const char *) token.data.scalar.value)
													+ 1);
									strcpy(expDBPort, (const char *) token.data.scalar.value);
								}
							} else if (!strcmp(
									(const char *) token.data.scalar.value,
									"sensorToCollectorMap")) {
								if (token.type != YAML_STREAM_END_TOKEN) {
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									while (token.type
											!= YAML_FLOW_MAPPING_END_TOKEN) {
										if (token.data.scalar.value != NULL) {
											if (!strcmp(
													(const char *) token.data.scalar.value,
													hostName)) {
												yaml_token_delete(&token);
												yaml_parser_scan(&parser,
														&token);
												yaml_token_delete(&token);
												yaml_parser_scan(&parser,
														&token);
												free(expDBLocation);
												expDBLocation =
														(char*) malloc(
																strlen(
																		(const char *) token.data.scalar.value)
																		+ 1);
												strcpy(expDBLocation,
														(const char *) token.data.scalar.value);
												break;
											} else if (!strcmp(
													(const char *) token.data.scalar.value,
													"__DEFAULT__")) {
												yaml_token_delete(&token);
												yaml_parser_scan(&parser,
														&token);
												yaml_token_delete(&token);
												yaml_parser_scan(&parser,
														&token);
												free(expDBLocation);
												expDBLocation =
														(char*) malloc(
																strlen(
																		(const char *) token.data.scalar.value)
																		+ 1);
												strcpy(expDBLocation,
														(const char *) token.data.scalar.value);
												break;
											}
										}
										yaml_token_delete(&token);
										yaml_parser_scan(&parser, &token);
										yaml_token_delete(&token);
										yaml_parser_scan(&parser, &token);
									}
								}
							}
						}
					}
				}
				break;
			}
			if (!strcmp((const char *) token.data.scalar.value, "localInfo")) {
				if (token.type != YAML_STREAM_END_TOKEN) {
					yaml_token_delete(&token);
					yaml_parser_scan(&parser, &token);
					yaml_token_delete(&token);
					yaml_parser_scan(&parser, &token);
					if (token.type != YAML_BLOCK_MAPPING_START_TOKEN) {
						break;
					}
					int block = 1;
					while (block != 0 || token.type != YAML_BLOCK_END_TOKEN) {
						yaml_token_delete(&token);
						yaml_parser_scan(&parser, &token);
						if (token.type == YAML_BLOCK_MAPPING_START_TOKEN)
							block++;
						else if (token.type == YAML_BLOCK_END_TOKEN)
							block--;
						if (token.data.scalar.value != NULL) {
							if (!strcmp((const char *) token.data.scalar.value,
									"logDir")) {
								if (token.type != YAML_STREAM_END_TOKEN) {
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									free(logDir);
									logDir = (char*) malloc(
											strlen((const char *) token.data.scalar.value)
													+ 1);
									strcpy(logDir, (const char *) token.data.scalar.value);
								}
							} else if (!strcmp(
									(const char *) token.data.scalar.value,
									"processAgentsCommPort")) {
								if (token.type != YAML_STREAM_END_TOKEN) {
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									yaml_token_delete(&token);
									yaml_parser_scan(&parser, &token);
									commPort = atoi((const char *) token.data.scalar.value);
								}
							}
						}
					}
				}
			}
			break;

		default:
			break;
		}
		if (token.type != YAML_STREAM_END_TOKEN)
			yaml_token_delete(&token);
	} while (token.type != YAML_STREAM_END_TOKEN);
	yaml_token_delete(&token);

	/* Cleanup */
	yaml_parser_delete(&parser);
	fclose(fh);
}

/***************************************************************
 Parse helper
 **************************************************************/
void parse_args(int argc, char**argv) {

	//agent_name agent_dock nodeConfigFile experimentConfigFile
	//execute=[pipe|socket] (logfile=path)
	if (argc >= 4) {
		agentName = (char*) malloc(strlen(argv[1] + 1));
		strcpy(agentName, argv[1]);
		dockName = (char*) malloc(strlen(argv[2] + 1));
		strcpy(dockName, argv[2]);
		nodeConfigFile = (char*) malloc(strlen(argv[3] + 1));
		strcpy(nodeConfigFile, argv[3]);
		experimentConfigFile = (char*) malloc(strlen(argv[4] + 1));
		strcpy(experimentConfigFile, argv[4]);
	} else {
		//command line must start with name, dock, node config and exp config
		exit(2);
	}

	int count = 5;
	while (count < argc) {
		char* keyValueStr = (char*) malloc(strlen(argv[count]) + 1);
		if (keyValueStr == NULL) {
			log_error(logger, "Malloc failed: Parsing arguments\n");
			exit(2);
		}
		strcpy(keyValueStr, argv[count]);
		char* key = strtok(keyValueStr, "=");
		key = trimwhitespace(key);
		char* val = strtok(NULL, "=");
		val = trimwhitespace(val);
		if (!strcmp(key, "commGroup")) {
			free(commGroup);
			commGroup = parseYamlString(val);
		} else if (!strcmp(key, "loglevel")) {
			char* strLevel = parseYamlString(val);
			log_level = getIntLevel(covertToUpper(strLevel));
			free(strLevel);
		} else if (!strcmp(key, "logfile")) {
			free(logFileName);
			logFileName = parseYamlString(val);
		} else if (!strcmp(key, "commHost")) {
			free(commHost);
			commHost = parseYamlString(val);
		} else if (!strcmp(key, "commPort")) {
			commPort = atoi(parseYamlString(val));
		} else if (!strcmp(key, "hostname")) {
			free(hostName);
			hostName = parseYamlString(val);
		} else if (!strcmp(key, "execute")) {
		}
		free(keyValueStr);
		keyValueStr = NULL;
		count++;
	}
}

/************************************************************
 * Parses all the incoming args and sets values
 *
 **************************************************************/
void setConfiguration(int argc, char** argv) {
	//setting defaults
	logDir = (char*) malloc(strlen("/var/log/magi/logs") + 1);
	strcpy(logDir, "/var/log/magi/logs");
	log_level = LOG_INFO;

	commHost = (char*) malloc(strlen("localhost") + 1);
	strcpy(commHost, "localhost");
	commPort = 18809;

	// Parse command line input parameters
	parse_args(argc, argv);

	// Parse information from node configuration file
	parseConfFile(nodeConfigFile);

	// if database location and hostName are same then change it to localhost
	if (!strcmp(expDBLocation, hostName)) {
		expDBLocation = realloc(expDBLocation, strlen("127.0.0.1") + 1);
		expDBLocation = "127.0.0.1";
	}

	/*Create logger and start logging*/
	if (logFileName == NULL) {
		logFileName = (char*) malloc(
				strlen(logDir) + 1 + strlen(agentName) + strlen(".log") + 1);
		strcpy(logFileName, logDir);
		strcat(logFileName, "/");
		strcat(logFileName, agentName);
		strcat(logFileName, ".log");
	}

	FILE* logFile = fopen(logFileName, "w");
	logger = Logger_create(logFile, log_level);

	/*Log the parsed info*/
	log_info(logger,
			"agentName : %s\ndockName : %s\nhostName : %s\ncommHost : %s"
					"\ncommPort : %d\nnodeConfigFile: %s\nexperimentConfigFile: %s"
					"\nexpDBLocation : %s\nexpDBPort : %s\nlogFileName : %s\nloglevel : %d\n",
			agentName, dockName, hostName, commHost, commPort, nodeConfigFile,
			experimentConfigFile, expDBLocation, expDBPort, logFileName,
			log_level);

	exitlog(logger, __func__, __FILE__, __LINE__);
}

void initializeAgent(int argc, char** argv) {
	registerFunction("stop", "void", &stopFunc, 0);
	create_functionMap();
	setConfiguration(argc, argv);
	init_connection(commHost, commPort);
	start_connection(dockName, commGroup);
	sendAgentLoadDoneTrigger();
	log_info(logger, "Agent initialized");
}

void runAgent() {
	entrylog(logger, __func__, __FILE__, __LINE__);
	while (!stop_flag) {
		MAGIMessage_t* magiMsg = next(0);
		if (magiMsg != NULL) {
			log_info(logger,
					"New message received. Performing required action");
			doMessageAction(magiMsg);
			freeMagiMessage(magiMsg);
		} /*else {
		 log_debug(logger, "No message");
		 }*/
		nanosleep((const struct timespec[] ) { { 0, 100000000L } }, NULL);
	}

	log_info(logger, "Sending a unlisten dock request for dock %s", dockName);
	unlistenDock(dockName);

	sendAgentUnloadDoneTrigger();

	log_info(logger, "Closing connection to magi daemon");
	closeTransport();

	log_info(logger, "Freeing up memory");
	free(agentName);
	free(dockName);
	free(hostName);
	free(commHost);
	free(nodeConfigFile);
	free(experimentConfigFile);
	//free(expDBLocation);
	free(expDBPort);
	free(logDir);
	free(logFileName);
	free(commGroup);

	log_info(logger, "Agent shutdown complete");
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Function to start the agent.
 *
 * @param argc - argument count
 * @param argv - argument list
 */
void agentStart(int argc, char** argv) {
	initializeAgent(argc, argv);
	runAgent();
}

void doMessageAction(MAGIMessage_t* magiMsg) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	fargs_t funcArgs = decodeMsgDataYaml(magiMsg->data);

	log_debug(logger, "Calling function: %s", funcArgs.func);

	char* retType = malloc(20);
	char* retVal = dispatchCall(funcArgs.func, &(funcArgs.args), retType);
	char* event = funcArgs.trigger;

	if (event) {
		log_info(logger, "Trigger: %s", event);
		log_info(logger, "Sending out Trigger message");
		char* data;
		if (!strcmp(retType, "dictionary")) {
			char* triggerFmt = "{event: %s, %s, nodes: %s}";
			data = (char*) malloc(
					strlen(triggerFmt) + strlen(event) + strlen(retVal)
							+ strlen(hostName) + 1);
			sprintf(data, triggerFmt, event, retVal, hostName);
		} else {
			char* triggerFmt = "{event: %s, retVal: %s, nodes: %s}";
			data = (char*) malloc(
					strlen(triggerFmt) + strlen(event) + strlen(retVal)
							+ strlen(hostName) + 1);
			sprintf(data, triggerFmt, event, retVal, hostName);
		}

		log_info(logger, "Trigger message: %s", data);
		trigger("control", NULL, YAML, data);
		free(data);
		log_info(logger, "Sent the trigger message for event: %s", event);
	}

	free(retVal);
	free(retType);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Method to invoke the function from the orchestrator using the agent code.
 *
 * @return - return value from the function call.
 * @param name - name of the function being called from the orchestrator.
 * @param args - arguments of the function to be called.
 * @param retType - return type of the function.
 */
char* dispatchCall(const char *name, char** args, char* retType) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	int i = 0, argcnt = 0;

	char* retString;

	// loop to count the no of arguments in the argument list.
	while (args[argcnt] != NULL) {
		argcnt++;
	}

	for (i = 0; i < regFuncCount; i++) {
		// Check if the incoming function name matches with any one of the names in function map.
		if (!strcmp(function_map[i].name, name) && function_map[i].func) {
			log_debug(logger, "Found function: %s", name);

			// Verify the argument count
			if (argcnt != function_map[i].aCnt) {
				log_error(logger, "Argument count did not match");
				retString = malloc(strlen("False") + 1);
				strcpy(retString, "False");
				return retString;
			}

			log_debug(logger, "Argument count matched");

			// Copy the arguments in the Union data structure as per the argument type.
			int j = 0;
			while (j < argcnt) {
				if (!strcmp(function_map[i].argList[j], "int")) {
					data[j].i = atoi(args[j]);
				} else if (!strcmp(function_map[i].argList[j], "char*")) {
					data[j].s = malloc(strlen(args[j]) + 1);
					strcpy(data[j].s, args[j]);
				} else {
					// TODO - handling unknown data types
					log_error(logger, "Unknown data type");
					retString = malloc(strlen("False") + 1);
					strcpy(retString, "False");
					return retString;
				}
				j++;
			}

			log_info(logger, "Calling function: %s", name);
			if (!(strcmp(function_map[i].retType, "int*"))) {
				int* ret = NULL;

				switch (argcnt) {
				case 0:
					ret = function_map[i].func();
					break;

				case 1:
					ret = function_map[i].func(data[0]);
					break;

				case 2:
					ret = function_map[i].func(data[0], data[1]);
					break;

				case 3:
					ret = function_map[i].func(data[0], data[1], data[2]);
					break;

				case 4:
					ret = function_map[i].func(data[0], data[1], data[2], data[3]);
					break;

				case 5:
					ret = function_map[i].func(data[0], data[1], data[2], data[3],
							data[4]);
					break;

				case 6:
					ret = function_map[i].func(data[0], data[1], data[2], data[3],
							data[4], data[5]);
					break;

				case 7:
					ret = function_map[i].func(data[0], data[1], data[2], data[3],
							data[4], data[5], data[6]);
					break;

				case 8:
					ret = function_map[i].func(data[0], data[1], data[2], data[3],
							data[4], data[5], data[6], data[7]);
					break;

				case 9:
					ret = function_map[i].func(data[0], data[1], data[2], data[3],
							data[4], data[5], data[6], data[7], data[8]);
					break;

				default:
					log_error(logger,
							"Not able to handle these many number of arguments\n");
					retString = malloc(strlen("False") + 1);
					strcpy(retString, "False");
					return retString;
				}

				int v = *ret;
				retString = malloc(100);
				sprintf(retString, "%d", v);
				free(ret);

			} else if (!strcmp(function_map[i].retType, "char*")) {

				switch (argcnt) {
				case 0:
					retString = function_map[i].func();
					break;

				case 1:
					retString = function_map[i].func(data[0]);
					break;

				case 2:
					retString = function_map[i].func(data[0], data[1]);
					break;

				case 3:
					retString = function_map[i].func(data[0], data[1], data[2]);
					break;

				case 4:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3]);
					break;

				case 5:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3], data[4]);
					break;

				case 6:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5]);
					break;

				case 7:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6]);
					break;

				case 8:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6], data[7]);
					break;

				case 9:
					retString = function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6], data[7], data[8]);
					break;

				default:
					log_error(logger,
							"Not able to handle these many number of arguments\n");
					retString = malloc(strlen("False") + 1);
					strcpy(retString, "False");
					return retString;
				}

			} else if (!strcmp(function_map[i].retType, "dictionary")) {
				dictionary d = NULL;
				char* temp = NULL;

				switch (argcnt) {
				case 0:
					temp = (char*) (function_map[i].func());
					break;

				case 1:
					temp = (char*) (function_map[i].func(data[0]));
					break;

				case 2:
					temp = (char*) (function_map[i].func(data[0], data[1]));
					break;

				case 3:
					temp =
							(char*) (function_map[i].func(data[0], data[1],
									data[2]));
					break;

				case 4:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3]));
					break;

				case 5:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3], data[4]));
					break;

				case 6:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5]));
					break;

				case 7:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6]));
					break;

				case 8:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6], data[7]));
					break;

				case 9:
					temp = (char*) (function_map[i].func(data[0], data[1], data[2],
							data[3], data[4], data[5], data[6], data[7], data[8]));
					break;

				default:
					log_info(logger,
							"Not able to handle these many number of arguments\n");
					retString = malloc(strlen("False") + 1);
					strcpy(retString, "False");
					return retString;
				}

				d = (dictionary) temp;
				retString = dictToYaml(d);
				free(temp);

			} else {
				switch (argcnt) {
				case 0:
					function_map[i].func();
					break;

				case 1:
					function_map[i].func(data[0]);
					break;

				case 2:
					function_map[i].func(data[0], data[1]);
					break;

				case 3:
					function_map[i].func(data[0], data[1], data[2]);
					break;

				case 4:
					function_map[i].func(data[0], data[1], data[2], data[3]);
					break;

				case 5:
					function_map[i].func(data[0], data[1], data[2], data[3],
							data[4]);
					break;

				case 6:
					function_map[i].func(data[0], data[1], data[2], data[3], data[4],
							data[5]);
					break;

				case 7:
					function_map[i].func(data[0], data[1], data[2], data[3], data[4],
							data[5], data[6]);
					break;

				case 8:
					function_map[i].func(data[0], data[1], data[2], data[3], data[4],
							data[5], data[6], data[7]);
					break;

				case 9:
					function_map[i].func(data[0], data[1], data[2], data[3], data[4],
							data[5], data[6], data[7], data[8]);
					break;

				default:
					log_info(logger,
							"Not able to handle these many number of arguments\n");
					break;
				}

				retString = malloc(strlen("True") + 1);
				strcpy(retString, "True");
			}

			strcpy(retType, function_map[i].retType);
			log_info(logger, "Return Type: %s", retType);
			log_info(logger, "Return Value: %s", retString);

			j = 0;
			while (j < argcnt) {
				if (!strcmp(function_map[i].argList[j], "char*")) {
					free(data[j].s);
				}
				j++;
			}
			exitlog(logger, __func__, __FILE__, __LINE__);
			return retString;
		}
	}

	log_error(logger, "No function found with name: %s", name);
	retString = malloc(strlen("False") + 1);
	strcpy(retString, "False");
	return retString;
}

// Utility function to store the arguments passed from the orchestrator.
dList_t* ArgParser(int argc, char** argv) {
	/*argv 1 and 2 are agentName and dockName*/
	if (argc < 4)
		return NULL;

	dList_t* dList = NULL;
	insert(&dList, "agentName", argv[1]);
	insert(&dList, "dockName", argv[2]);
	insert(&dList, "nodeConfigFile", argv[3]);
	insert(&dList, "experimentConfigFile", argv[4]);

	/*From 5th count starts the key value pair*/
	int count = 5;
	while (count < argc) {
		char* temp, *name, *value;
		temp = (char*) malloc(strlen(argv[count]) + 1);
		if (temp == NULL) {
			return NULL;
		}
		memset(temp, 0, strlen(argv[count]) + 1);
		strcpy(temp, argv[count]);
		char* tkn = strtok(temp, "=");
		tkn = trimwhitespace(tkn);
		/*got the key*/
		name = malloc(strlen(tkn) + 1);
		strcpy(name, tkn);
		/*parse the value*/
		tkn = strtok(NULL, "=");
		value = parseYamlString(tkn);
		insert(&dList, name, value);
		//free((char*)temp);
		temp = NULL;
		count++;
	}
	return dList;
}
