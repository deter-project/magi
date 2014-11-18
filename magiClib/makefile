all: AgentRequest.c MAGIMessage.c testModule.c AgentMessenger.c AgentTransport.c logger.c
	gcc -g -w -o testmod testModule.c AgentMessenger.c AgentTransport.c AgentRequest.c MAGIMessage.c logger.c -lpthread
clean:
	rm -rf testmod
